package actions

import (
	"encoding/json"

	"github.com/elastic/beats/libbeat/logp"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/processors"
)

func init() {
	processors.RegisterPlugin("fake_k8s_annotations", newFakeK8sAnnotations)
}

const fakeAnno = `{
	"container": {
		"name": "proxy"
	},
	"node": {
		"name": "test-endpoint-1"
	},
	"pod": {
		"name": "egress-7cf977b78c-76rzl"
	},
	"namespace": "section-env805-production-8d4b7edebbc0b",
	"annotations": {
		"section.io/egress-version": "beta-1.0.4",
		"section.io/log-stack": "acc380-app430-env805",
		"section.io/log-handler": "egress",
		"section.io/proxy-template": "egress",
		"section.io/sampling": "0.22"
	},
	"labels": {
		"app": "egress",
		"section.io/proxy-name": "egress",
		"pod-template-hash": "3795336347"
	}
}`

type fakeK8sAnnotations struct {
	anno common.MapStr
}

func newFakeK8sAnnotations(c *common.Config) (processors.Processor, error) {
	fake := &fakeK8sAnnotations{
		anno: common.MapStr{},
	}
	err := json.Unmarshal([]byte(fakeAnno), &fake.anno)
	if err != nil {
		logp.Info("couldn't Unmarshall fake annotation")
	}
	return fake, nil
}

func (f *fakeK8sAnnotations) Run(b *beat.Event) (*beat.Event, error) {
	b.Fields.DeepUpdate(common.MapStr{
		"kubernetes": f.anno.Clone(),
	})
	return b, nil
}

func (f *fakeK8sAnnotations) String() string {
	return "fake_k8s_annotations"
}
