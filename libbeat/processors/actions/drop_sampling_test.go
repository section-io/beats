package actions

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/common/atomic"
	"github.com/stretchr/testify/assert"
)

type FixedThreshold struct {
	value float64
}

func (F FixedThreshold) Float64() float64 {
	return F.value
}

const journalCtlK8sFields = `{
    "kubernetes": {
        "annotations": {
            "example.corp/log-handler": "journal"
        },
        "container": {
            "name": "journalctl"
        },
        "labels": {
            "controller-revision-hash": "56db9f4cf7",
            "k8s-app": "journalctl",
            "pod-template-generation": "1"
        },
        "namespace": "example-shared",
        "node": {
            "name": "minikube"
        },
        "pod": {
            "name": "journalctl-zw4zm"
        }
    },
    "log": "{ \"__CURSOR\" : \"s=8d9dd247085c469892a95a5f471454f5;i=d11;b=83cbeed78895419d86e62a1c127f4573;m=9d131152;t=57f243e04d423;x=8f37c2cf71589625\", \"__REALTIME_TIMESTAMP\" : \"1547168519607331\", \"__MONOTONIC_TIMESTAMP\" : \"2635272530\", \"_BOOT_ID\" : \"83cbeed78895419d86e62a1c127f4573\", \"_MACHINE_ID\" : \"f3f859b5c848428186f374050bca9dac\", \"_HOSTNAME\" : \"minikube\", \"_TRANSPORT\" : \"kernel\", \"SYSLOG_FACILITY\" : \"0\", \"SYSLOG_IDENTIFIER\" : \"kernel\", \"PRIORITY\" : \"5\", \"_SOURCE_MONOTONIC_TIMESTAMP\" : \"2635233200\", \"MESSAGE\" : \"audit: type=1300 audit(1547168519.604:279): arch=c000003e syscall=54 success=yes exit=0 a0=3 a1=0 a2=40 a3=55f1d6edf390 items=0 ppid=7966 pid=1349 auid=4294967295 uid=0 gid=0 euid=0suid=0 fsuid=0 egid=0 sgid=0 fsgid=0 tty=(none) ses=4294967295 comm=\\\"iptables-restor\\\" exe=\\\"/sbin/xtables-multi\\\" subj=kernel key=(null)\" }\n",
    "offset": 588615,
    "prospector": {
        "type": "log"
    },
    "exampleEndpoint": "Development Endpoint",
    "exampleRegion": "development-region",
    "source": "/var/lib/docker/containers/056687ab30371babe754137a7511ca333650e1d8ae83d5fbeade156cec7db184/056687ab30371babe754137a7511ca333650e1d8ae83d5fbeade156cec7db184-json.log",
    "stream": "stdout",
    "time": "2019-01-11T01:02:30.022302709Z"
}`

const ingressK8sNoLogSamplingFields = `{
	"kubernetes": {
		"annotations": {
			"example.corp/log-handler": "ingress"
		},
		"container": {
			"name": "nginx-ingress-lb"
		},
		"labels": {
			"controller-revision-hash": "5998f57f75",
			"k8s-app": "nginx-ingress-lb",
			"name": "nginx-ingress-lb",
			"pod-template-generation": "2"
		},
		"namespace": "example-delivery",
		"node": {
			"name": "minikube"
		},
		"pod": {
			"name": "nginx-ingress-controller-qmfrj"
		}
	},
	"log": " { \"time\":\"2019-01-11T06:47:27+00:00\", \"request\":{ \"remote_addr\":\"192.168.99.1\", \"request\":\"GET /custom_errors/ HTTP/1.1\", \"http_referer\":\"http://www.example.com/\", \"http_user_agent\":\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36\", \"http_host\":\"www.example.com\", \"http_accept_encoding\":\"gzip, deflate\", \"http_transfer_encoding\":\"-\", \"cookies\":\"0\", \"scheme\":\"http\", \"ssl_server_name\":\"-\", \"ssl_cipher\":\"-\", \"ssl_protocol\":\"-\", \"spdy\":\"\", \"http_upgrade\":\"-\", \"is_proxy_protocol\":\"-TODO\" }, \"geo\":{ \"latlon\":\",\", \"city\":\"\", \"country\":\"\" }, \"asn\":\"-\", \"downstream\":{ \"status\":\"200\", \"bytes_sent\":\"640\", \"body_bytes_sent\":\"199\", \"request_time\":\"0.003\", \"content_encoding\":\"gzip\", \"request_length\":\"459\", \"content_type\":\"text/html; charset=UTF-8\" }, \"upstream\":{ \"status\":\"200\", \"response_time\":\"0.003\", \"response_length\":\"256\", \"origin_failure\":\"-\", \"origin_status\":\"200\", \"proxy_host\":\"example-env5-master-a7579c37f93e6-ingress-upstream-80\", \"upstream_addr\":\"172.17.0.23:80\" }, \"connection\":\"60\", \"example-id\":\"f9b4b38f25f0250195aee0fbe72bbe4c\", \"stack\":\"-\", \"tcpinfo_rtt\":\"752\", \"example_io_tag\":\"-\"}\n",
	"offset": 40939,
	"prospector": {
		"type": "log"
	},
	"exampleEndpoint": "Development Endpoint",
	"exampleRegion": "development-region",
	"source": "/var/lib/docker/containers/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072-json.log",
	"stream": "stdout",
	"time": "2019-01-11T06:47:27.502162832Z"
}`

const ingressK8sFields = `{
	"kubernetes": {
		"annotations": {
			"example.corp/log-handler": "ingress"
		},
		"container": {
			"name": "nginx-ingress-lb"
		},
		"labels": {
			"controller-revision-hash": "5998f57f75",
			"k8s-app": "nginx-ingress-lb",
			"name": "nginx-ingress-lb",
			"pod-template-generation": "2"
		},
		"namespace": "example-delivery",
		"node": {
			"name": "minikube"
		},
		"pod": {
			"name": "nginx-ingress-controller-qmfrj"
		}
	},
	"log": " { \"time\":\"2019-01-11T06:47:27+00:00\", \"request\":{ \"remote_addr\":\"192.168.99.1\", \"request\":\"GET /custom_errors/ HTTP/1.1\", \"http_referer\":\"http://www.example.com/\", \"http_user_agent\":\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36\", \"http_host\":\"www.example.com\", \"http_accept_encoding\":\"gzip, deflate\", \"http_transfer_encoding\":\"-\", \"cookies\":\"0\", \"scheme\":\"http\", \"ssl_server_name\":\"-\", \"ssl_cipher\":\"-\", \"ssl_protocol\":\"-\", \"spdy\":\"\", \"http_upgrade\":\"-\", \"is_proxy_protocol\":\"-TODO\" }, \"geo\":{ \"latlon\":\",\", \"city\":\"\", \"country\":\"\" }, \"asn\":\"-\", \"downstream\":{ \"status\":\"200\", \"bytes_sent\":\"640\", \"body_bytes_sent\":\"199\", \"request_time\":\"0.003\", \"content_encoding\":\"gzip\", \"request_length\":\"459\", \"content_type\":\"text/html; charset=UTF-8\" }, \"upstream\":{ \"status\":\"200\", \"response_time\":\"0.003\", \"response_length\":\"256\", \"origin_failure\":\"-\", \"origin_status\":\"200\", \"proxy_host\":\"example-env5-master-a7579c37f93e6-ingress-upstream-80\", \"upstream_addr\":\"172.17.0.23:80\" }, \"connection\":\"60\", \"example-id\":\"f9b4b38f25f0250195aee0fbe72bbe4c\", \"stack\":\"-\", \"tcpinfo_rtt\":\"752\", \"example_io_tag\":\"-\", \"log_sample\": \"0.5678\" }\n",
	"offset": 40939,
	"prospector": {
		"type": "log"
	},
	"exampleEndpoint": "Development Endpoint",
	"exampleRegion": "development-region",
	"source": "/var/lib/docker/containers/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072-json.log",
	"stream": "stdout",
	"time": "2019-01-11T06:47:27.502162832Z"
}`

const annotatedIngressK8sFields = `{
	"kubernetes": {
		"annotations": {
			"example.corp/log-handler": "ingress",
			"sampling": "0.34"
		},
		"container": {
			"name": "nginx-ingress-lb"
		},
		"labels": {
			"controller-revision-hash": "5998f57f75",
			"k8s-app": "nginx-ingress-lb",
			"name": "nginx-ingress-lb",
			"pod-template-generation": "2"
		},
		"namespace": "example-delivery",
		"node": {
			"name": "minikube"
		},
		"pod": {
			"name": "nginx-ingress-controller-qmfrj"
		}
	},
	"log": " { \"time\":\"2019-01-11T06:47:27+00:00\", \"request\":{ \"remote_addr\":\"192.168.99.1\", \"request\":\"GET /custom_errors/ HTTP/1.1\", \"http_referer\":\"http://www.example.com/\", \"http_user_agent\":\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36\", \"http_host\":\"www.example.com\", \"http_accept_encoding\":\"gzip, deflate\", \"http_transfer_encoding\":\"-\", \"cookies\":\"0\", \"scheme\":\"http\", \"ssl_server_name\":\"-\", \"ssl_cipher\":\"-\", \"ssl_protocol\":\"-\", \"spdy\":\"\", \"http_upgrade\":\"-\", \"is_proxy_protocol\":\"-TODO\" }, \"geo\":{ \"latlon\":\",\", \"city\":\"\", \"country\":\"\" }, \"asn\":\"-\", \"downstream\":{ \"status\":\"200\", \"bytes_sent\":\"640\", \"body_bytes_sent\":\"199\", \"request_time\":\"0.003\", \"content_encoding\":\"gzip\", \"request_length\":\"459\", \"content_type\":\"text/html; charset=UTF-8\" }, \"upstream\":{ \"status\":\"200\", \"response_time\":\"0.003\", \"response_length\":\"256\", \"origin_failure\":\"-\", \"origin_status\":\"200\", \"proxy_host\":\"example-env5-master-a7579c37f93e6-ingress-upstream-80\", \"upstream_addr\":\"172.17.0.23:80\" }, \"connection\":\"60\", \"example-id\":\"f9b4b38f25f0250195aee0fbe72bbe4c\", \"stack\":\"-\", \"tcpinfo_rtt\":\"752\", \"example_io_tag\":\"-\", \"log_sample\": \"0.9988\" }\n",
	"offset": 40939,
	"prospector": {
		"type": "log"
	},
	"exampleEndpoint": "Development Endpoint",
	"exampleRegion": "development-region",
	"source": "/var/lib/docker/containers/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072-json.log",
	"stream": "stdout",
	"time": "2019-01-11T06:47:27.502162832Z"
}`

const annotatedDefaultIngressK8sFields = `{
	"kubernetes": {
		"annotations": {
			"example.corp/log-handler": "ingress",
			"sampling": "0.34"
		},
		"container": {
			"name": "nginx-ingress-lb"
		},
		"labels": {
			"controller-revision-hash": "5998f57f75",
			"k8s-app": "nginx-ingress-lb",
			"name": "nginx-ingress-lb",
			"pod-template-generation": "2"
		},
		"namespace": "example-delivery",
		"node": {
			"name": "minikube"
		},
		"pod": {
			"name": "nginx-ingress-controller-qmfrj"
		}
	},
	"log": " { \"time\":\"2019-01-11T06:47:27+00:00\", \"request\":{ \"remote_addr\":\"192.168.99.1\", \"request\":\"GET /custom_errors/ HTTP/1.1\", \"http_referer\":\"http://www.example.com/\", \"http_user_agent\":\"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36\", \"http_host\":\"www.example.com\", \"http_accept_encoding\":\"gzip, deflate\", \"http_transfer_encoding\":\"-\", \"cookies\":\"0\", \"scheme\":\"http\", \"ssl_server_name\":\"-\", \"ssl_cipher\":\"-\", \"ssl_protocol\":\"-\", \"spdy\":\"\", \"http_upgrade\":\"-\", \"is_proxy_protocol\":\"-TODO\" }, \"geo\":{ \"latlon\":\",\", \"city\":\"\", \"country\":\"\" }, \"asn\":\"-\", \"downstream\":{ \"status\":\"200\", \"bytes_sent\":\"640\", \"body_bytes_sent\":\"199\", \"request_time\":\"0.003\", \"content_encoding\":\"gzip\", \"request_length\":\"459\", \"content_type\":\"text/html; charset=UTF-8\" }, \"upstream\":{ \"status\":\"200\", \"response_time\":\"0.003\", \"response_length\":\"256\", \"origin_failure\":\"-\", \"origin_status\":\"200\", \"proxy_host\":\"example-env5-master-a7579c37f93e6-ingress-upstream-80\", \"upstream_addr\":\"172.17.0.23:80\" }, \"connection\":\"60\", \"example-id\":\"f9b4b38f25f0250195aee0fbe72bbe4c\", \"stack\":\"-\", \"tcpinfo_rtt\":\"752\", \"example_io_tag\":\"-\", \"log_sample\": \"\" }\n",
	"offset": 40939,
	"prospector": {
		"type": "log"
	},
	"exampleEndpoint": "Development Endpoint",
	"exampleRegion": "development-region",
	"source": "/var/lib/docker/containers/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072/57f9fe935ae4f41f5d3c8b42eb65db94f2f86c8c58485b885adbbc050678f072-json.log",
	"stream": "stdout",
	"time": "2019-01-11T06:47:27.502162832Z"
}`

func TestAccept(t *testing.T) {
	tests := []struct {
		threshold float64
		rate      float64
		expected  bool
		title     string
	}{
		{
			0.9999999999999999999999999,
			1.0,
			true,
			"Marginally less than max rate",
		},
		{
			0.5,
			0.5,
			true,
			"Equal to rate",
		},
		{
			0.9999999999999999999999999,
			0.0,
			false,
			"Drop all the things",
		},
		{
			0.0,
			0.0,
			false,
			"No match if dropping all the things",
		},
		{
			0.0,
			-0.5,
			false,
			"No match if a negative rate",
		},
	}

	for i, test := range tests {
		test := test
		name := fmt.Sprintf("run test(%d) title: %s", i, test.title)

		t.Run(name, func(t *testing.T) {
			ds := &dropSampling{
				threshold:   &FixedThreshold{test.threshold},
				skipped:     &atomic.Int64{},
				allowed:     &atomic.Int64{},
				annotated:   &atomic.Int64{},
				unannotated: &atomic.Int64{},
				metrics:     StartPeriodicMetrics(),
			}

			threshold, actual := ds.accept(test.rate)
			assert.Equal(t, test.threshold, threshold)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestFindSamplingInAnnotations(t *testing.T) {
	allDs := &dropSampling{
		threshold:   &FixedThreshold{0.0},
		skipped:     &atomic.Int64{},
		allowed:     &atomic.Int64{},
		annotated:   &atomic.Int64{},
		unannotated: &atomic.Int64{},
		metrics:     StartPeriodicMetrics(),
	}

	config, err := common.NewConfigFrom(map[string]interface{}{
		"module":        "nginx",
		"error.enabled": false,
	})
	if err != nil {
		t.Fatalf("Config err: %v", err)
	}

	k8sProcessor, err := newFakeK8sAnnotations(config)
	if err != nil {
		t.Fatalf("Could not create fake k8s")
	}
	k8sEvent, _ := k8sProcessor.Run(
		&beat.Event{
			Timestamp: time.Now(),
			Fields:    common.MapStr{"type": "test", "name": "TestFindSampling", "line": 10},
		})

	tests := []struct {
		event        beat.Event
		expectedRate float64
		expectedOk   bool
		title        string
	}{
		{
			beat.Event{
				Timestamp: time.Now(),
				Fields:    common.MapStr{"type": "test", "name": "TestFindSampling", "line": 10, "kubernetes": common.MapStr{"annotations": ""}},
			},
			0.0,
			false,
			"No map-string annotations for k8s",
		},
		{
			beat.Event{
				Timestamp: time.Now(),
				Fields:    common.MapStr{"type": "test", "name": "TestFindSampling", "line": 10, "kubernetes": common.MapStr{"annotations": common.MapStr{"foo": "bar"}}},
			},
			0.0,
			false,
			"No sampling in annotations for k8s",
		},
		{
			beat.Event{
				Timestamp: time.Now(),
				Fields:    common.MapStr{"type": "test", "name": "TestFindSampling", "line": 10, "kubernetes": common.MapStr{"annotations": common.MapStr{"sampling": 123}}},
			},
			0.0,
			false,
			"sampling not a string for k8s",
		},
		{
			*k8sEvent,
			0.22,
			true,
			"Has sampling in annotations for k8s",
		},
	}

	for i, test := range tests {
		name := fmt.Sprintf("run test(%d) title: %s", i, test.title)

		t.Run(name, func(t *testing.T) {

			k8s, err := GetMapStrValue(&test.event, "kubernetes")
			if err != nil {
				t.Fatalf("No kubernetes in test: %s - %v", name, err)
			}
			rate, ok := allDs.findAnnotationSampling(k8s, &test.event)
			assert.Equal(t, test.expectedRate, rate)
			assert.Equal(t, test.expectedOk, ok)
		})
	}
}
func TestFindSamplingInIngressLog(t *testing.T) {
	allDs := &dropSampling{
		threshold:   &FixedThreshold{0.0},
		skipped:     &atomic.Int64{},
		allowed:     &atomic.Int64{},
		annotated:   &atomic.Int64{},
		unannotated: &atomic.Int64{},
		metrics:     StartPeriodicMetrics(),
	}

	k8sJournalctlFields := common.MapStr{}
	err := json.Unmarshal([]byte(journalCtlK8sFields), &k8sJournalctlFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s journalctl - %v", err)
	}

	k8sJournalctlEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sJournalctlFields.Clone(),
	}

	k8sIngressNoLogSamplingFields := common.MapStr{}
	err = json.Unmarshal([]byte(ingressK8sNoLogSamplingFields), &k8sIngressNoLogSamplingFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s no log sampling ingress - %v", err)
	}

	k8sIngressNoLogSamplingEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sIngressNoLogSamplingFields.Clone(),
	}

	k8sAnnotatedDefaultIngressFields := common.MapStr{}
	err = json.Unmarshal([]byte(annotatedDefaultIngressK8sFields), &k8sAnnotatedDefaultIngressFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s ingress %v", err)
	}

	k8sAnnotatedDefaultIngressEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sAnnotatedDefaultIngressFields.Clone(),
	}

	k8sIngressFields := common.MapStr{}
	err = json.Unmarshal([]byte(ingressK8sFields), &k8sIngressFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s ingress - %v", err)
	}

	k8sIngressEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sIngressFields.Clone(),
	}

	tests := []struct {
		event        beat.Event
		expectedRate float64
		expectedOk   bool
		title        string
	}{
		{
			*k8sJournalctlEvent,
			0.0,
			false,
			"Not an ingress log",
		},
		{
			*k8sIngressNoLogSamplingEvent,
			0.0,
			false,
			"An ingress log with no log sampling",
		},
		{
			*k8sAnnotatedDefaultIngressEvent,
			0.0,
			false,
			"Ingress log with default sampling",
		},
		{
			*k8sIngressEvent,
			0.5678,
			true,
			"Has sampling in log for k8s",
		},
	}

	for i, test := range tests {
		name := fmt.Sprintf("run test(%d) title: %s", i, test.title)

		t.Run(name, func(t *testing.T) {

			k8s, err := GetMapStrValue(&test.event, "kubernetes")
			if err != nil {
				t.Fatalf("No kubernetes in test: %s - %v", name, err)
			}
			rate, ok := allDs.findIngressSampling(k8s, &test.event)
			assert.Equal(t, test.expectedRate, rate)
			assert.Equal(t, test.expectedOk, ok)
		})
	}
}

func TestFindSampling(t *testing.T) {
	allDs := &dropSampling{
		threshold:   &FixedThreshold{0.0},
		skipped:     &atomic.Int64{},
		allowed:     &atomic.Int64{},
		annotated:   &atomic.Int64{},
		unannotated: &atomic.Int64{},
		metrics:     StartPeriodicMetrics(),
	}

	k8sAnnotatedDefaultIngressFields := common.MapStr{}
	err := json.Unmarshal([]byte(annotatedDefaultIngressK8sFields), &k8sAnnotatedDefaultIngressFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s ingress %v", err)
	}

	k8sAnnotatedDefaultIngressEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sAnnotatedDefaultIngressFields.Clone(),
	}

	k8sAnnotatedIngressFields := common.MapStr{}
	err = json.Unmarshal([]byte(annotatedIngressK8sFields), &k8sAnnotatedIngressFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s ingress %v", err)
	}

	k8sAnnotatedIngressEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sAnnotatedIngressFields.Clone(),
	}

	tests := []struct {
		event        beat.Event
		expectedRate float64
		expectedOk   bool
		title        string
	}{
		{
			beat.Event{
				Timestamp: time.Now(),
				Fields:    common.MapStr{"type": "test", "name": "TestFindSampling", "line": 10},
			},
			0.0,
			false,
			"No kubernetes",
		},
		{
			beat.Event{
				Timestamp: time.Now(),
				Fields:    common.MapStr{"type": "test", "name": "TestFindSampling", "line": 10, "kubernetes": "foo"},
			},
			0.0,
			false,
			"k8s is not a map string",
		},
		{
			*k8sAnnotatedDefaultIngressEvent,
			0.34,
			true,
			"Default ingress sampling should NOT have priority",
		},
		{
			*k8sAnnotatedIngressEvent,
			0.9988,
			true,
			"Ingress sampling should have priority",
		},
	}

	for i, test := range tests {
		name := fmt.Sprintf("run test(%d) title: %s", i, test.title)

		t.Run(name, func(t *testing.T) {

			rate, ok := allDs.findSampling(&test.event)
			assert.Equal(t, test.expectedRate, rate)
			assert.Equal(t, test.expectedOk, ok)
		})
	}
}

func TestRun(t *testing.T) {
	k8sJournalctlFields := common.MapStr{}
	err := json.Unmarshal([]byte(journalCtlK8sFields), &k8sJournalctlFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s journalctl - %v", err)
	}

	k8sJournalctlEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sJournalctlFields.Clone(),
	}

	k8sAnnotatedIngressFields := common.MapStr{}
	err = json.Unmarshal([]byte(annotatedIngressK8sFields), &k8sAnnotatedIngressFields)
	if err != nil {
		t.Fatalf("couldn't unmarshall k8s ingress %v", err)
	}

	k8sAnnotatedIngressEvent := &beat.Event{
		Timestamp: time.Now(),
		Fields:    k8sAnnotatedIngressFields.Clone(),
	}

	tests := []struct {
		event                  beat.Event
		threshold              float64
		expectedError          error
		expectedEvent          bool
		expectedUnannotedCount int64
		expectedAnnotedCount   int64
		expectedAllowedCount   int64
		expectedSkippedCount   int64
		title                  string
	}{
		{
			*k8sJournalctlEvent,
			1.0,
			nil,
			true,
			1,
			0,
			0,
			0,
			"No sampling",
		},
		{
			*k8sAnnotatedIngressEvent,
			1.0,
			nil,
			false,
			0,
			1,
			0,
			1,
			"Event dropped",
		},
		{
			*k8sAnnotatedIngressEvent,
			0.00001,
			nil,
			true,
			0,
			1,
			1,
			0,
			"Event accepted",
		},
	}

	for i, test := range tests {
		name := fmt.Sprintf("run test(%d) title: %s", i, test.title)

		t.Run(name, func(t *testing.T) {

			allDs := &dropSampling{
				threshold:   &FixedThreshold{test.threshold},
				skipped:     &atomic.Int64{},
				allowed:     &atomic.Int64{},
				annotated:   &atomic.Int64{},
				unannotated: &atomic.Int64{},
				metrics:     StartPeriodicMetrics(),
			}

			event, err := allDs.Run(&test.event)
			assert.Equal(t, test.expectedError, err)
			if test.expectedEvent {
				assert.NotNil(t, event)
			} else {
				assert.Nil(t, event)
			}
			actualAllowedCount, actualSkippedCount, actualAnnotedCount, actualUnannotedCount := allDs.provideMetrics()

			assert.Equal(t, test.expectedUnannotedCount, actualUnannotedCount)
			assert.Equal(t, test.expectedAnnotedCount, actualAnnotedCount)
			assert.Equal(t, test.expectedAllowedCount, actualAllowedCount)
			assert.Equal(t, test.expectedSkippedCount, actualSkippedCount)
		})
	}
}
