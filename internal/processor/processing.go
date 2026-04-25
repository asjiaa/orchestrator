package processor

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/davidbyttow/govips/v2/vips"
)

const (
	thumbnailWidth  = 256
	thumbnailLength = 256
	exportQuality   = 85
)

type govipsProcessor struct{}

func NewGovipsProcessor() (*govipsProcessor, error) {
	vips.LoggingSettings(nil, vips.LogLevelError)
	vips.Startup(&vips.Config{
		ConcurrencyLevel: 1, // scale via environment variable
		MaxCacheFiles:    0,
		MaxCacheMem:      0,
		MaxCacheSize:     0,
		ReportLeaks:      false,
		CacheTrace:       false,
		CollectStats:     false,
	})
	return &govipsProcessor{}, nil
}

func (g *govipsProcessor) Shutdown() {
	vips.Shutdown()
}

func (g *govipsProcessor) Process(ctx context.Context, jobID string, input io.Reader) (Result, error) {
	raw, err := io.ReadAll(input)
	if err != nil {
		return Result{}, fmt.Errorf("govips: read input: %w", err)
	}

	img, err := vips.NewImageFromBuffer(raw)
	if err != nil {
		return Result{}, fmt.Errorf("govips: decode image: %w", err)
	}
	defer img.Close()

	if err := img.Thumbnail(thumbnailWidth, thumbnailLength, vips.InterestingCentre); err != nil {
		return Result{}, fmt.Errorf("govips: thumbnail: %w", err)
	}

	// Strip embedded metadata
	if err := img.RemoveMetadata(); err != nil {
		return Result{}, fmt.Errorf("govips: strip metadata: %w", err)
	}

	ep := vips.NewJpegExportParams()
	ep.Quality = exportQuality
	ep.StripMetadata = true

	out, _, err := img.ExportJpeg(ep)
	if err != nil {
		return Result{}, fmt.Errorf("govips: export jpeg: %w", err)
	}

	return Result{
		Data:        bytes.NewReader(out),
		ContentType: "image/jpeg",
		Key:         fmt.Sprintf("results/%s/thumb.jpg", jobID),
	}, nil
}
