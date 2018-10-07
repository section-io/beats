package actions

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/atomic"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
)

type dropSampling struct {
	rnd         *rand.Rand
	seed        int64
	annotated   *atomic.Int64
	unannotated *atomic.Int64
	skipped     *atomic.Int64
	allowed     *atomic.Int64
	metrics     *PeriodicSamplingMetrics
}

func init() {
	fmt.Println("registering drop_sampling for section.io-filebeat-v6.2.3-k3")
	processors.RegisterPlugin("drop_sampling", newDropSample)
}

// accept returns true iff the random number generated is below
// the configured rate, otherwise false.
func (d *dropSampling) accept(rate float64) (float64, bool) {
	threshold := d.rnd.Float64()
	return threshold, threshold <= rate
}

func newDropSample(c *common.Config) (processors.Processor, error) {
	logp.Info("creating new drop_sampling processor")
	seed := time.Now().UnixNano() // turn into something set by config
	src := rand.NewSource(seed)
	rnd := rand.New(src)

	ds := &dropSampling{
		rnd:         rnd,
		skipped:     &atomic.Int64{},
		allowed:     &atomic.Int64{},
		annotated:   &atomic.Int64{},
		unannotated: &atomic.Int64{},
		metrics:     StartPeriodicMetrics(),
	}

	go ds.metrics.run(ds.provideMetrics)

	return ds, nil
}

func (d *dropSampling) Run(b *beat.Event) (*beat.Event, error) {
	rate, ok := d.findSampling(b)
	if !ok {
		d.annotated.Inc()
		return b, nil
	} else {
		d.unannotated.Inc()
	}

	_, ok = d.accept(rate)

	if ok {
		d.allowed.Inc()
	} else {
		d.skipped.Inc()
	}

	if ok {
		return b, nil
	}

	return nil, nil
}

func (d *dropSampling) provideMetrics() (allowed, skipped, annotated, unannotated int64) {
	return d.allowed.Load(), d.skipped.Load(), d.annotated.Load(), d.unannotated.Load()
}

func (d *dropSampling) findSampling(b *beat.Event) (float64, bool) {
	val, err := b.GetValue("kubernetes")
	if err != nil {
		logp.Debug("drop_sampling", "didn't find kubernetes annotation")
		return 0.0, false
	}

	k8s, ok := val.(common.MapStr)
	if !ok {
		logp.Debug("drop_sampling", "couldn't make k8s into map")
		return 0.0, false
	}

	iface, ok := k8s["annotations"]
	if !ok {
		logp.Debug("drop_sampling", "couldn't find annotations in k8s map")
		return 0.0, false
	}

	anno, ok := iface.(common.MapStr)
	if !ok {
		logp.Debug("drop_sampling", "couldn't make annotations into map")
		return 0.0, false
	}

	iface, ok = anno["section.io/sampling"]
	if !ok {
		logp.Debug("drop_sampling", "couldn't find section.io/sampling")
		return 0.0, false
	}

	s, ok := iface.(string)
	if !ok {
		logp.Debug("drop_sampling", "couldn't make sampling into string")
		return 0.0, false
	}

	pct, err := strconv.ParseFloat(s, 64)
	if err != nil {
		logp.Debug("drop_sampling", "couldn't parse value")
		return 0.0, false
	}

	return pct, true
}

func (*dropSampling) String() string {
	return "drop_sampling"
}
