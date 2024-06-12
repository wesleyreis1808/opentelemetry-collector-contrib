// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awscloudwatchmetricsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchmetricsreceiver"

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

type metricReceiver struct {
	region        string
	profile       string
	imdsEndpoint  string
	pollInterval  time.Duration
	nextStartTime time.Time
	logger        *zap.Logger
	autoDiscover  *AutoDiscoverConfig
	consumer      consumer.Metrics
	buildInfo     component.BuildInfo
	wg            *sync.WaitGroup
	doneChan      chan bool
}

func newMetricReceiver(cfg *Config, set receiver.Settings, consumer consumer.Metrics) *metricReceiver {
	return &metricReceiver{
		region:        cfg.Region,
		profile:       cfg.Profile,
		imdsEndpoint:  cfg.IMDSEndpoint,
		pollInterval:  cfg.PollInterval,
		nextStartTime: time.Now().Add(-cfg.PollInterval),
		logger:        set.Logger,
		autoDiscover:  cfg.Metrics.AutoDiscover,
		wg:            &sync.WaitGroup{},
		consumer:      consumer,
		buildInfo:     set.BuildInfo,
		doneChan:      make(chan bool),
	}
}

func (m *metricReceiver) Start(ctx context.Context, _ component.Host) error {
	m.logger.Debug("starting to poll for CloudWatch metrics")
	m.wg.Add(1)
	go m.startPolling(ctx)
	return nil
}

func (m *metricReceiver) Shutdown(_ context.Context) error {
	m.logger.Debug("shutting down awscloudwatchmetrics receiver")
	close(m.doneChan)
	m.wg.Wait()
	return nil
}

func (m *metricReceiver) startPolling(ctx context.Context) {
	defer m.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.doneChan:
			return
		}
	}
}
