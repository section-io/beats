package actions

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/elastic/beats/libbeat/logp"
)

// PeriodicSamplingMetrics flushes metrics to log for visibility into the
// amount of logs dropped vs published.
type PeriodicSamplingMetrics struct {
	// closeSig is used to close due to os SigInt, but also for tests
	// to call close() method.
	closeSig chan os.Signal
}

// MetricsProvider surfaces the number of events skipped and allowed, while
// additionally providing the most recently seen threshold and rate.
type MetricsProvider func() (allowed, skipped uint64, rate, threshold float64)

// StartPeriodicMetrics starts outputing to log metrics gathered via
// the MetricsProvider given to run().
func StartPeriodicMetrics() *PeriodicSamplingMetrics {
	return &PeriodicSamplingMetrics{
		closeSig: make(chan os.Signal, 1),
	}
}

// Close has the typical closer signature and shuts down the metrics
// logging.
func (p *PeriodicSamplingMetrics) Close() error {
	if p.closeSig == nil {
		return nil
	}
	// this will cause the channel to immediately broadcast with nil
	// effectively helping the run method exit the infinite loop.
	close(p.closeSig)
	return nil
}

func (p *PeriodicSamplingMetrics) run(provider MetricsProvider) {
	running := true
	signal.Notify(p.closeSig, syscall.SIGINT, syscall.SIGTERM)
	tic := time.NewTicker(30 * time.Second)

	defer func() {
		close(p.closeSig)
		logp.Info("closing periodic drop metrics")
	}()

	for running {
		select {
		case <-p.closeSig:
			running = false

		case <-tic.C:
			allowed, skipped, rate, threshold := provider()
			samplePct := p.samplePct(allowed, skipped)
			logp.Info(fmt.Sprintf(
				"allowed: %d, skipped: %d, rate: %f, threshold: %f, sampled pct: %f",
				allowed, skipped, rate, threshold, samplePct))
		}
	}

	logp.Info("terminating period sampling metrics")
}

func (p *PeriodicSamplingMetrics) samplePct(allowed, skipped uint64) float64 {
	total := skipped + allowed
	samplePct := float64(allowed) / float64(total)
	return samplePct
}
