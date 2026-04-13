package s3store

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// MultipartUploader handles streaming uploads via S3 multipart API.
type MultipartUploader struct {
	client   *Client
	bucket   string
	key      string
	uploadID string

	mu        sync.Mutex
	parts     []types.CompletedPart
	partNum   int32
	totalSize int64
}

const partSize = 16 * 1024 * 1024 // 16MB per part

// NewMultipartUploader initiates a new multipart upload.
func (c *Client) NewMultipartUploader(ctx context.Context, key string) (*MultipartUploader, error) {
	input := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	}

	// Apply SSE if configured
	if sse, _ := c.sseConfig(); sse != "" {
		input.ServerSideEncryption = sse
	}
	if kmsKey := c.sseKMSKey(); kmsKey != nil {
		input.SSEKMSEncryptionContext = kmsKey
	}

	out, err := c.s3.CreateMultipartUpload(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("create multipart upload: %w", err)
	}
	return &MultipartUploader{
		client:   c,
		bucket:   c.bucket,
		key:      key,
		uploadID: aws.ToString(out.UploadId),
	}, nil
}

// UploadPart uploads a single part. Thread-safe.
func (u *MultipartUploader) UploadPart(ctx context.Context, partNum int32, r io.ReadSeeker, size int64) error {
	out, err := u.client.s3.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(u.key),
		UploadId:      aws.String(u.uploadID),
		PartNumber:    aws.Int32(partNum),
		Body:          r,
		ContentLength: aws.Int64(size),
	})
	if err != nil {
		return fmt.Errorf("upload part %d: %w", partNum, err)
	}

	u.mu.Lock()
	u.parts = append(u.parts, types.CompletedPart{
		ETag:       out.ETag,
		PartNumber: aws.Int32(partNum),
	})
	u.totalSize += size
	u.mu.Unlock()

	return nil
}

// Complete finalizes the multipart upload.
func (u *MultipartUploader) Complete(ctx context.Context) error {
	u.mu.Lock()
	parts := make([]types.CompletedPart, len(u.parts))
	copy(parts, u.parts)
	u.mu.Unlock()

	_, err := u.client.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(u.bucket),
		Key:             aws.String(u.key),
		UploadId:        aws.String(u.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		return fmt.Errorf("complete multipart upload: %w", err)
	}
	return nil
}

// Abort cancels the multipart upload.
func (u *MultipartUploader) Abort(ctx context.Context) error {
	_, err := u.client.s3.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(u.bucket),
		Key:      aws.String(u.key),
		UploadId: aws.String(u.uploadID),
	})
	return err
}

// StreamingUploader provides a streaming writer interface for S3 multipart uploads.
type StreamingUploader struct {
	uploader *MultipartUploader
	buffer   []byte
	bufPos   int
	ctx      context.Context
	partNum  int32
	done     bool
	errOnce  sync.Once
	err      error
}

// NewStreamingUploader creates a streaming uploader that buffers data and uploads parts automatically.
func (c *Client) NewStreamingUploader(ctx context.Context, key string) (*StreamingUploader, error) {
	uploader, err := c.NewMultipartUploader(ctx, key)
	if err != nil {
		return nil, err
	}
	return &StreamingUploader{
		uploader: uploader,
		buffer:   make([]byte, 0, partSize*2),
		ctx:      ctx,
		partNum:  1,
	}, nil
}

// Write implements io.Writer. Data is buffered until a full part is accumulated.
func (s *StreamingUploader) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}

	s.buffer = append(s.buffer, p...)

	// Upload parts as they fill up
	for len(s.buffer)-s.bufPos >= partSize {
		if err := s.uploadPart(); err != nil {
			s.err = err
			return 0, err
		}
	}

	return len(p), nil
}

func (s *StreamingUploader) uploadPart() error {
	end := s.bufPos + partSize
	if end > len(s.buffer) {
		end = len(s.buffer)
	}
	data := s.buffer[s.bufPos:end]
	s.bufPos = end

	r := &byteSeekReader{data: data}
	if err := s.uploader.UploadPart(s.ctx, s.partNum, r, int64(len(data))); err != nil {
		return err
	}
	s.partNum++
	return nil
}

// Close finalizes the upload. Must be called after all writes are done.
func (s *StreamingUploader) Close() error {
	if s.done {
		return s.err
	}
	s.done = true

	// Upload remaining data
	if s.bufPos < len(s.buffer) {
		remaining := s.buffer[s.bufPos:]
		r := &byteSeekReader{data: remaining}
		if err := s.uploader.UploadPart(s.ctx, s.partNum, r, int64(len(remaining))); err != nil {
			s.uploader.Abort(s.ctx)
			return err
		}
	}

	return s.uploader.Complete(s.ctx)
}

// byteSeekReader wraps a byte slice as an io.ReadSeeker for S3 SDK.
type byteSeekReader struct {
	data []byte
	pos  int64
}

func (r *byteSeekReader) Read(p []byte) (int, error) {
	if r.pos >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += int64(n)
	return n, nil
}

func (r *byteSeekReader) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = r.pos + offset
	case io.SeekEnd:
		newPos = int64(len(r.data)) + offset
	}
	if newPos < 0 || newPos > int64(len(r.data)) {
		return r.pos, fmt.Errorf("invalid seek position")
	}
	r.pos = newPos
	return r.pos, nil
}
