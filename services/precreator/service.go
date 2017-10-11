// Package precreator provides the shard precreation service.
package precreator // import "github.com/influxdata/influxdb/services/precreator"

import (
	"sync"
	"time"
)

type Diagnostic interface {
	Starting(checkInterval, advancedPeriod time.Duration)
	Closing()
	PrecreateError(err error)
}

// Service manages the shard precreation service.
type Service struct {
	checkInterval time.Duration
	advancePeriod time.Duration

	Diagnostic Diagnostic

	done chan struct{}
	wg   sync.WaitGroup

	MetaClient interface {
		PrecreateShardGroups(now, cutoff time.Time) error
	}
}

// NewService returns an instance of the precreation service.
func NewService(c Config) (*Service, error) {
	s := Service{
		checkInterval: time.Duration(c.CheckInterval),
		advancePeriod: time.Duration(c.AdvancePeriod),
	}

	return &s, nil
}

// WithLogger sets the logger for the service.
func (s *Service) With(d Diagnostic) {
	s.Diagnostic = d
}

// Open starts the precreation service.
func (s *Service) Open() error {
	if s.done != nil {
		return nil
	}

	if s.Diagnostic != nil {
		s.Diagnostic.Starting(s.checkInterval, s.advancePeriod)
	}

	s.done = make(chan struct{})

	s.wg.Add(1)
	go s.runPrecreation()
	return nil
}

// Close stops the precreation service.
func (s *Service) Close() error {
	if s.done == nil {
		return nil
	}

	close(s.done)
	s.wg.Wait()
	s.done = nil

	return nil
}

// runPrecreation continually checks if resources need precreation.
func (s *Service) runPrecreation() {
	defer s.wg.Done()

	for {
		select {
		case <-time.After(s.checkInterval):
			if err := s.precreate(time.Now().UTC()); err != nil {
				if s.Diagnostic != nil {
					s.Diagnostic.PrecreateError(err)
				}
			}
		case <-s.done:
			if s.Diagnostic != nil {
				s.Diagnostic.Closing()
			}
			return
		}
	}
}

// precreate performs actual resource precreation.
func (s *Service) precreate(now time.Time) error {
	cutoff := now.Add(s.advancePeriod).UTC()
	if err := s.MetaClient.PrecreateShardGroups(now, cutoff); err != nil {
		return err
	}
	return nil
}
