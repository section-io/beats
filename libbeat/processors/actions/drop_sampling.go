package actions

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
)

const (
	defaultPct = 0.10 // accept only 10 percent
)

type dropSampling struct {
	rnd  *rand.Rand
	rate float64
	seed int64
}

func init() {
	processors.RegisterPlugin("drop_sampling",
		configChecked(newDropSample, allowedFields("sampling", "when")))
}

// accept returns true iff the random number generated is below
// the configured rate, otherwise false.
func (d *dropSampling) accept(rate float64) bool {
	return d.rnd.Float64() <= rate
}

// infoInspectConfig dumps some information about the incoming initial
// config because delve doesn't provide a deep inspection.
func infoInspectConfig(c *common.Config) {
	logp.Info("new drop_sampling")
	logp.Info(fmt.Sprintf("has sampling: %t", c.HasField("sampling")))
	logp.Info(fmt.Sprintf("is dict: %t", c.IsDict()))
	logp.Info(fmt.Sprintf("is array: %t", c.IsArray()))
	logp.Info(c.Path())

	logp.Info(fmt.Sprintf("enabled: %t", c.Enabled()))
	logp.Info(fmt.Sprintf("fields: %v", c.GetFields()))

	child, e := c.Child("sampling", -1)
	if e != nil {
		logp.Info(e.Error())
	}

	logp.Info(fmt.Sprintf("has pct: %t", child.HasField("pct")))

	pct, e := child.Int("pct", -1)
	if e != nil {
		logp.Info(e.Error())
	}

	logp.Info(fmt.Sprintf("pct: %d", pct))
}

func newDropSample(c *common.Config) (processors.Processor, error) {

	child, err := c.Child("sampling", -1)
	if err != nil {
		logp.Debug("drop_sampling", "sampling should have been part of the config")
	}

	seed := time.Now().UnixNano() // turn into something set by config
	src := rand.NewSource(seed)
	rnd := rand.New(src)

	ds := &dropSampling{
		rnd:  rnd,
		seed: seed,
		rate: findPct(child),
	}

	return ds, nil
}

// loadPct looks for the 'pct' value in the config.
func findPct(c *common.Config) float64 {
	if !c.HasField("pct") {
		return defaultPct
	}

	pct, e := c.Float("pct", -1)
	if e != nil {
		return defaultPct
	}

	return pct
}

func (d *dropSampling) Run(b *beat.Event) (*beat.Event, error) {
	pct, ok := d.findSampling(b)
	if !ok {
		return b, nil
	}

	if d.accept(pct) {
		return b, nil
	} else {
		return nil, nil
	}
}

func (d *dropSampling) findSampling(b *beat.Event) (float64, bool) {
	val, err := b.GetValue("kubernetes")
	if err != nil {
		logp.Info("didn't find kubernetes annotation")
		return 0.0, false
	}

	k8s, ok := val.(common.MapStr)
	if !ok {
		logp.Info("couldn't make k8s into map")
		return 0.0, false
	}

	iface, ok := k8s["annotations"]
	if !ok {
		logp.Info("couldn't find annotations in k8s map")
		return 0.0, false
	}

	anno, ok := iface.(common.MapStr)
	if !ok {
		logp.Info("couldn't make annotations into map")
		return 0.0, false
	}

	iface, ok = anno["section.io/sampling"]
	if !ok {
		logp.Info("couldn't find section.io/sampling")
		return 0.0, false
	}

	s, ok := iface.(string)
	if !ok {
		logp.Info("couldn't make sampling into strin")
		return 0.0, false
	}

	pct, err := strconv.ParseFloat(s, 64)
	if err != nil {
		logp.Info("couldn't parse value")
		return 0.0, false
	}

	return pct, true
}

func (*dropSampling) String() string {
	return "drop_sampling"
}
