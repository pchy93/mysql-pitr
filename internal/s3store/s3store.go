package s3store

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Config holds S3 connection configuration.
type Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	ForcePathStyle  bool
	// SSEType specifies server-side encryption type: "", "AES256", or "aws:kms"
	SSEType string
	// SSEKMSKeyID specifies KMS key ID for SSE-KMS (optional)
	SSEKMSKeyID string
}

// Client wraps the AWS S3 client.
type Client struct {
	s3         *s3.Client
	bucket     string
	sseType    string
	sseKMSKeyID string
}

// New creates a new S3 client from config.
func New(cfg Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = cfg.ForcePathStyle
		},
	}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}

	return &Client{
		s3:          s3.NewFromConfig(awsCfg, s3Opts...),
		bucket:      cfg.Bucket,
		sseType:     cfg.SSEType,
		sseKMSKeyID: cfg.SSEKMSKeyID,
	}, nil
}

// sseConfig returns the server-side encryption configuration for uploads.
func (c *Client) sseConfig() (types.ServerSideEncryption, *string) {
	if c.sseType == "" {
		return "", nil
	}

	var sse types.ServerSideEncryption
	switch c.sseType {
	case "AES256":
		sse = types.ServerSideEncryptionAes256
	case "aws:kms":
		sse = types.ServerSideEncryptionAwsKms
	case "aws:kms:dsse":
		sse = types.ServerSideEncryptionAwsKmsDsse
	default:
		return "", nil
	}
	return sse, nil
}

// sseKMSKey returns the KMS key ID if SSE-KMS is enabled.
func (c *Client) sseKMSKey() *string {
	if c.sseType == "aws:kms" || c.sseType == "aws:kms:dsse" {
		if c.sseKMSKeyID != "" {
			return aws.String(c.sseKMSKeyID)
		}
	}
	return nil
}

// UploadFile uploads a local file to S3 at the given key.
func (c *Client) UploadFile(ctx context.Context, key, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file %s: %w", localPath, err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat file %s: %w", localPath, err)
	}

	input := &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(key),
		Body:          f,
		ContentLength: aws.Int64(stat.Size()),
	}

	// Apply SSE if configured
	if sse, _ := c.sseConfig(); sse != "" {
		input.ServerSideEncryption = sse
	}
	if kmsKey := c.sseKMSKey(); kmsKey != nil {
		input.SSEKMSEncryptionContext = kmsKey
	}

	_, err = c.s3.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

// UploadReader uploads data from a reader to S3.
func (c *Client) UploadReader(ctx context.Context, key string, r io.Reader, size int64) error {
	input := &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   r,
	}
	if size > 0 {
		input.ContentLength = aws.Int64(size)
	}

	// Apply SSE if configured
	if sse, _ := c.sseConfig(); sse != "" {
		input.ServerSideEncryption = sse
	}
	if kmsKey := c.sseKMSKey(); kmsKey != nil {
		input.SSEKMSEncryptionContext = kmsKey
	}

	_, err := c.s3.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}

// DownloadFile downloads an S3 object to a local file.
func (c *Client) DownloadFile(ctx context.Context, key, localPath string) error {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("get object %s: %w", key, err)
	}
	defer out.Body.Close()

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create file %s: %w", localPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, out.Body); err != nil {
		return fmt.Errorf("write file %s: %w", localPath, err)
	}
	return nil
}

// GetObject returns a reader for an S3 object. Caller must close the body.
func (c *Client) GetObject(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}
	return out.Body, nil
}

// ObjectInfo holds metadata for an S3 object.
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// ListObjects lists all objects with the given prefix.
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	var result []ObjectInfo
	paginator := s3.NewListObjectsV2Paginator(c.s3, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list objects: %w", err)
		}
		for _, obj := range page.Contents {
			result = append(result, ObjectInfo{
				Key:          aws.ToString(obj.Key),
				Size:         aws.ToInt64(obj.Size),
				LastModified: aws.ToTime(obj.LastModified),
			})
		}
	}
	return result, nil
}

// DeleteObject deletes an object from S3.
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	return err
}

// KeyExists checks whether an object key exists in S3.
func (c *Client) KeyExists(ctx context.Context, key string) (bool, error) {
	_, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nf *types.NotFound
		if isNotFound(err, &nf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func isNotFound(err error, _ interface{}) bool {
	return strings.Contains(err.Error(), "NotFound") ||
		strings.Contains(err.Error(), "404") ||
		strings.Contains(err.Error(), "NoSuchKey")
}
