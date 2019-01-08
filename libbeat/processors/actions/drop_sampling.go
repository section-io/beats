package actions

import (
	"encoding/json"
	"math/rand"
	"strconv"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/atomic"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/processors"
	"github.com/pkg/errors"
)

// Threshold source
type threshold interface {
	// Float64 returns, as a float64, a pseudo-random threshold in [0.0,1.0).
	Float64() float64
}

// Sampling metrics and control
type dropSampling struct {
	threshold   threshold
	annotated   *atomic.Int64
	unannotated *atomic.Int64
	skipped     *atomic.Int64
	allowed     *atomic.Int64
	metrics     *PeriodicSamplingMetrics
}

// Register this processor plugin
func init() {
	logp.Info("registering drop_sampling")
	processors.RegisterPlugin("drop_sampling", newDropSample)
}

// accept returns true iff the random number generated is below
// the configured rate and the rate is not 0, otherwise false.
// NOTE: a negative rate behaves as 0 i.e. drop everything.
func (d *dropSampling) accept(rate float64) (float64, bool) {
	threshold := d.threshold.Float64()
	return threshold, rate > 0 && threshold <= rate
}

// newDropSample - constructor for dropSampling
func newDropSample(c *common.Config) (processors.Processor, error) {
	logp.Info("creating new drop_sampling processor!!")
	seed := time.Now().UnixNano() // turn into something set by config
	src := rand.NewSource(seed)
	rnd := rand.New(src)

	ds := &dropSampling{
		threshold:   rnd,
		skipped:     &atomic.Int64{},
		allowed:     &atomic.Int64{},
		annotated:   &atomic.Int64{},
		unannotated: &atomic.Int64{},
		metrics:     StartPeriodicMetrics(),
	}

	go ds.metrics.run(ds.provideMetrics)

	return ds, nil
}

// Run - implementation of the actual processor action
func (d *dropSampling) Run(b *beat.Event) (*beat.Event, error) {
	rate, ok := d.findSampling(b)
	if !ok {
		d.unannotated.Inc()
		return b, nil
	}
	d.annotated.Inc()

	_, ok = d.accept(rate)

	if ok {
		d.allowed.Inc()
		return b, nil
	}

	d.skipped.Inc()
	return nil, nil
}

// provideMetrics - return the current metrics for this action
func (d *dropSampling) provideMetrics() (allowed, skipped, annotated, unannotated int64) {
	return d.allowed.Load(), d.skipped.Load(), d.annotated.Load(), d.unannotated.Load()
}

// findSampling - look for the sampling rate for this event
// TODO: make this more generic, pass in 0 - n selectors via the config.
// Selectors need to support a simple nested field matching for what to return e.g.:
//   kubernetes.annotation.sample
// AND conditionals e.g.
//  if kubernetes.k8s.app == 'ingress' then return log.log_sample
// i.e. something like jq/JSON query but this would require an extra dependency which may not please upstream.
// Possibly more recent versions of beats have added something useful in this situation.
func (d *dropSampling) findSampling(b *beat.Event) (float64, bool) {
	logp.Debug("drop_sampling", "findSampling: %v", b)
	k8s, err := GetMapStrValue(b, "kubernetes")
	if err != nil {
		logp.Debug("drop_sampling", "didn't find kubernetes entry. error: %s", err)
		return 0.0, false
	}

	// Log sampling rate set on ingress takes priority, so we check for it first
	if value, ok := d.findIngressSampling(k8s, b); ok {
		logp.Debug("drop_sampling", "found ingress sampling: %f", value)
		return value, true
	}

	// Sampling rate set on annotations is next priority
	if value, ok := d.findAnnotationSampling(k8s, b); ok {
		logp.Debug("drop_sampling", "found annotation sampling: %f", value)
		return value, true
	}
	return 0.0, false
}

// Look for the sampling rate in the k8s annotations
func (d *dropSampling) findAnnotationSampling(k8s common.MapStr, b *beat.Event) (float64, bool) {
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

	iface, ok = anno["sampling"]
	if !ok {
		logp.Debug("drop_sampling", "couldn't find sampling")
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

// Look for log sampling rate in the 'log' entry, but only for k8s ingress object
func (d *dropSampling) findIngressSampling(k8s common.MapStr, b *beat.Event) (float64, bool) {
	iface, ok := k8s["labels"]
	if !ok {
		logp.Debug("drop_sampling", "no labels")
		return 0.0, false
	}

	labels, ok := iface.(common.MapStr)
	if !ok {
		logp.Debug("drop_sampling", "couldn't make labels into map")
		return 0.0, false
	}

	iface, ok = labels["k8s-app"]
	if !ok {
		logp.Debug("drop_sampling", "couldn't find k8s-app label")
		return 0.0, false
	}

	k8sApp := iface.(string)
	if !ok || k8sApp != "nginx-ingress-lb" {
		logp.Debug("drop_sampling", "not ingress %s", k8sApp)
		return 0.0, false
	}

	logp.Debug("drop_sampling", "IS ingress %s", k8sApp)

	// We have an ingress log, now look for the "log_sample" key in the "log"
	log, err := GetStringValue(b, "log")
	if err != nil {
		logp.Debug("drop_sampling", "no log string %v", err)
		return 0.0, false
	}

	logJSON := common.MapStr{}
	err = json.Unmarshal([]byte(log), &logJSON)
	if err != nil {
		logp.Debug("drop_sampling", "could not unmarshal %s - %s", logJSON, err)
		return 0.0, false
	}

	iface, ok = logJSON["log_sample"]
	if !ok {
		logp.Debug("drop_sampling", "no log sampling")
		return 0.0, false
	}

	s, ok := iface.(string)
	if !ok {
		logp.Debug("drop_sampling", "couldn't make log_sample into string")
		return 0.0, false
	}

	if s == "" {
		logp.Debug("drop_sampling: %s", "log_sample is not set")
		return 0.0, false
	}

	sampling, err := strconv.ParseFloat(s, 64)
	if err != nil {
		logp.Debug("drop_sampling", "couldn't parse value")
		return 0.0, false
	}

	return sampling, true
}

func (*dropSampling) String() string {
	return "drop_sampling"
}

var (
	// ErrValueNotString indicates that the specified key was found but was not a string
	ErrValueNotString = errors.New("value not string")
	// ErrValueNotMapStr indicates that the specified key was found but was not a common.MapStr
	ErrValueNotMapStr = errors.New("value not MapStr")
)

// GetStringValue is a helper that returns a string value from the beat.Event for the given key
// It will return an error if the key exists but cannot be cast to a string.
func GetStringValue(b *beat.Event, key string) (string, error) {
	iface, err := b.GetValue(key)
	if err == nil {
		if value, ok := iface.(string); ok {
			return value, nil
		}
		return "", ErrValueNotString
	}
	return "", err
}

// GetMapStrValue is a helper that returns a string value from the beat.Event for the given key
// It will return an error if the key exists but cannot be cast to a string.
func GetMapStrValue(b *beat.Event, key string) (common.MapStr, error) {
	iface, err := b.GetValue(key)
	if err == nil {
		if value, ok := iface.(common.MapStr); ok {
			return value, nil
		}
		return nil, ErrValueNotString
	}
	return nil, err
}
