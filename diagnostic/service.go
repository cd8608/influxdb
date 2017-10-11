package diagnostic

import (
	"io"

	"github.com/uber-go/zap"
)

type Service struct {
	l zap.Logger
}

func New(w io.Writer) *Service {
	logger := zap.New(zap.NewTextEncoder(), zap.Output(zap.AddSync(w)))
	return &Service{l: logger}
}
