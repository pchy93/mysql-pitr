package backup

import (
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
)

// gzipWriter wraps gzip.Writer to implement io.WriteCloser.
type gzipWriter struct {
	gw *gzip.Writer
	w  io.Writer
}

func newGzipWriter(w io.Writer) *gzipWriter {
	return &gzipWriter{
		gw: gzip.NewWriter(w),
		w:  w,
	}
}

func (g *gzipWriter) Write(p []byte) (int, error) {
	return g.gw.Write(p)
}

func (g *gzipWriter) Close() error {
	return g.gw.Close()
}

// zstdWriter wraps zstd.Encoder.
type zstdWriter struct {
	enc *zstd.Encoder
}

func newZstdWriter(w io.Writer) *zstdWriter {
	enc, _ := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedDefault))
	return &zstdWriter{enc: enc}
}

func (z *zstdWriter) Write(p []byte) (int, error) {
	return z.enc.Write(p)
}

func (z *zstdWriter) Close() error {
	return z.enc.Close()
}
