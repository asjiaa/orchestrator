package processor

import (
	"context"
	"io"
)

type Result struct {
	Data        io.Reader
	ContentType string // metadata
	Key         string // destination path
}

type Processor interface {
	Process(ctx context.Context, jobID string, input io.Reader) (Result, error)
}
