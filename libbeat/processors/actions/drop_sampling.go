package actions

import (
	"math/rand"
	"strconv"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
)

type dropSampling struct {
	rnd  *rand.Rand
	seed int64
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
		rnd: rnd,
	}

	return ds, nil
}

func (d *dropSampling) Run(b *beat.Event) (*beat.Event, error) {
	pct, ok := d.findSampling(b)
	if !ok {
		return b, nil
	}

	if threshold, ok := d.accept(pct); ok {
		logp.Debug("drop_sampling", "allowed: %f, rate: %f", threshold, pct)
		return b, nil
	} else {
		logp.Debug("drop_sampling", "skipped: %f, rate: %f", threshold, pct)
		return nil, nil
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
		logp.Debug("drop_sampling", "couldn't make sampling into strin")
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
