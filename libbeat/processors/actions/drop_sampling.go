package actions

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
	"github.com/elastic/beats/libbeat/common/atomic"
	"encoding/json"
)

type dropSampling struct {
	rnd  *rand.Rand
	seed int64
	log *Flog
	skipped atomic.Uint64
	allowed atomic.Uint64
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

	log, err := NewFlog("/tests/load/logs/flog.log")
	if err != nil {
		panic(err)
	}

	ds := &dropSampling{
		rnd: rnd,
		log: log,
	}

	return ds, nil
}

func (d *dropSampling) Run(b *beat.Event) (*beat.Event, error) {
	pct, ok := d.findSampling(b)
	if !ok {
		return b, nil
	}

	threshold, ok := d.accept(pct)

	if ok {
		d.allowed.Inc()
	} else {
		d.skipped.Inc()
	}

	skipped := d.skipped.Load()
	allowed := d.allowed.Load()
	total := skipped + allowed

	samplePct := float64(allowed) / float64(total)

	if threshold > .50 {
		d.logEvent(b)
	}

	if ok {
		d.log.logf("allowed: %f, rate: %f, sample pct: %f", threshold, pct, samplePct)
		return b, nil
	} else {
		d.log.logf("skipped: %f, rate: %f, sample pct: %f", threshold, pct, samplePct)
		return nil, nil
	}
}

func (d *dropSampling) logEvent(b *beat.Event) {
	bin, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		d.log.log(err.Error())
	} else {
		d.log.log(string(bin))
	}
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
