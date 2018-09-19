package actions

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
)

type dropSampling struct {
	rnd       *rand.Rand
	seed      int64
	lock      *sync.Mutex
	skipped   uint64
	allowed   uint64
	rate      float64
	threshold float64
	metrics   *PeriodicSamplingMetrics
}

func init() {
	processors.RegisterPlugin("drop_sampling", newDropSample)
}

// accept returns true iff the random number generated is below
// the configured rate, otherwise false.
func (d *dropSampling) accept(rate float64) (float64, bool) {
	threshold := d.rnd.Float64()
	return threshold, threshold <= rate
}

func newDropSample(c *common.Config) (processors.Processor, error) {
	seed := time.Now().UnixNano() // turn into something set by config
	src := rand.NewSource(seed)
	rnd := rand.New(src)

	ds := &dropSampling{
		rnd:           rnd,
		statsLogginOn: true,
		lock:          &sync.Mutex{},
		metrics:       StartPeriodicMetrics(),
	}

	go ds.metrics.run(ds.provideMetrics)

	return ds, nil
}

func (d *dropSampling) Run(b *beat.Event) (*beat.Event, error) {
	rate, ok := d.findSampling(b)
	if !ok {
		return b, nil
	}

	threshold, ok := d.accept(rate)

	d.trackMetrics(ok, rate, threshold)

	if ok {
		return b, nil
	}

	return nil, nil
}

func (d *dropSampling) trackMetrics(ok bool, rate, threshold float64) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if ok {
		d.allowed++
	} else {
		d.skipped++
	}
	d.rate = rate
	d.threshold = threshold
}

func (d *dropSampling) provideMetrics() (allowed, skipped uint64, rate, threshold float64) {
	d.lock.Lock()
	allowed = d.allowed
	skipped = d.skipped
	rate = d.rate
	threshold = d.threshold
	d.lock.Unlock()
	return
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
