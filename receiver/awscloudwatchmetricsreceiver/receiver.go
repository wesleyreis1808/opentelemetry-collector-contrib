// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awscloudwatchmetricsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchmetricsreceiver"

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	conventions "go.opentelemetry.io/collector/semconv/v1.6.1"
	"go.uber.org/zap"
)

const (
	maxNumberOfElements = 500
	receiverName        = "otelcol/awscloudwatchmetricsreceiver"
)

type metricReceiver struct {
	region        string
	profile       string
	imdsEndpoint  string
	pollInterval  time.Duration
	nextStartTime time.Time
	logger        *zap.Logger
	client        client
	autoDiscover  *AutoDiscoverConfig
	requests      []request
	consumer      consumer.Metrics
	buildInfo     component.BuildInfo
	wg            *sync.WaitGroup
	doneChan      chan bool
}

type request struct {
	ID             string
	Namespace      string
	MetricName     string
	Period         time.Duration
	AwsAggregation string
	Dimensions     []types.Dimension
}

// CloudWatchAPI is an interface to represent subset of AWS CloudWatch metrics functionality.
type client interface {
	GetMetricData(ctx context.Context, params *cloudwatch.GetMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
	ListMetrics(ctx context.Context, params *cloudwatch.ListMetricsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error)
}

func buildGetMetricDataQueries(metric *request) types.MetricDataQuery {
	mdq := types.MetricDataQuery{
		Id:         aws.String(metric.ID),
		ReturnData: aws.Bool(true),
	}
	mdq.MetricStat = &types.MetricStat{
		Metric: &types.Metric{
			Namespace:  aws.String(metric.Namespace),
			MetricName: aws.String(metric.MetricName),
			Dimensions: metric.Dimensions,
		},
		Period: aws.Int32(int32(metric.Period.Seconds())),
		Stat:   aws.String(metric.AwsAggregation),
	}
	return mdq
}

func chunkSlice(requests []request, maxSize int) [][]request {
	var slicedMetrics [][]request
	for i := 0; i < len(requests); i += maxSize {
		end := i + maxSize

		if end > len(requests) {
			end = len(requests)
		}
		slicedMetrics = append(slicedMetrics, requests[i:end])
	}
	return slicedMetrics
}

// divide up into slices of 500, then execute
// Split requests slices into small slices no longer than 500 elements
// GetMetricData only allows 500 elements in a slice, otherwise we'll get validation error
// Avoids making a network call for each metric configured
func (m *metricReceiver) request(st, et time.Time) ([]cloudwatch.GetMetricDataInput, map[string]request) {

	chunks := chunkSlice(m.requests, maxNumberOfElements)
	metricDataInput := make([]cloudwatch.GetMetricDataInput, len(chunks))
	requestMapID := make(map[string]request)

	for idx, chunk := range chunks {
		metricDataInput[idx].StartTime, metricDataInput[idx].EndTime = aws.Time(st), aws.Time(et)

		for _, request := range chunk {
			mdq := buildGetMetricDataQueries(&request)
			requestMapID[request.ID] = request
			metricDataInput[idx].MetricDataQueries = append(metricDataInput[idx].MetricDataQueries, mdq)
		}
	}
	return metricDataInput, requestMapID
}

func generateID() string {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("m_%d", mrand.Int())
	}
	s := hex.EncodeToString(b)
	return "m_" + s
}

func newMetricReceiver(cfg *Config, set receiver.Settings, consumer consumer.Metrics) *metricReceiver {
	var requests []request

	if cfg.Metrics.NamedMetrics != nil {
		for _, namedMetric := range cfg.Metrics.NamedMetrics {
			var dimensions []types.Dimension

			for _, dimConfig := range namedMetric.Dimensions {
				dimensions = append(dimensions, types.Dimension{
					Name:  aws.String(dimConfig.Name),
					Value: aws.String(dimConfig.Value),
				})
			}

			namedRequest := request{
				ID:             generateID(),
				Namespace:      namedMetric.Namespace,
				MetricName:     namedMetric.MetricName,
				Period:         namedMetric.Period,
				AwsAggregation: namedMetric.AwsAggregation,
				Dimensions:     dimensions,
			}

			requests = append(requests, namedRequest)
		}
	}

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
		requests:      requests,
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

	t := time.NewTicker(m.pollInterval)
	defer t.Stop()
	for {
		if m.autoDiscover != nil {
			requests, err := m.autoDiscoverRequests(ctx, m.autoDiscover)
			if err != nil {
				m.logger.Debug("couldn't discover metrics", zap.Error(err))
			}
			m.requests = requests
		}

		if err := m.poll(ctx); err != nil {
			m.logger.Error("there was an error during polling", zap.Error(err))
		}

		select {
		case <-ctx.Done():
			return
		case <-m.doneChan:
			return
		case <-t.C:
			continue
		}
	}
}

func (m *metricReceiver) poll(ctx context.Context) error {
	var err error
	startTime := m.nextStartTime
	endTime := time.Now()

	if len(m.requests) > 0 {
		err = m.pollForMetrics(ctx, startTime, endTime)
	}

	m.nextStartTime = endTime
	return err
}

func (m *metricReceiver) pollForMetrics(ctx context.Context, startTime time.Time, endTime time.Time) error {
	err := m.configureAWSClient(ctx)
	if err != nil {
		return err
	}

	select {
	case _, ok := <-m.doneChan:
		if !ok {
			return nil
		}
	default:
		filters, rmapID := m.request(startTime, endTime)
		for _, filter := range filters {
			paginator := cloudwatch.NewGetMetricDataPaginator(m.client, &filter)
			for paginator.HasMorePages() {
				select {
				// if done, we want to stop processing paginated requests
				case _, ok := <-m.doneChan:
					if !ok {
						return nil
					}
				default:
					output, err := paginator.NextPage(ctx)
					if err != nil {
						return errors.Join(fmt.Errorf("unable to retrieve metric data from cloudwatch"), err)
					}
					observedTime := pcommon.NewTimestampFromTime(time.Now())
					metrics := m.parseMetrics(observedTime, rmapID, output)

					mCount := metrics.MetricCount()
					m.logger.Debug("metrics to be consumed", zap.Int("count", mCount))
					if mCount > 0 {
						if err := m.consumer.ConsumeMetrics(ctx, metrics); err != nil {
							m.logger.Error("unable to consume metrics", zap.Error(err))
						}
					}
				}
			}
		}
	}
	return nil
}

func (m *metricReceiver) parseMetrics(_ pcommon.Timestamp, rmapID map[string]request, resp *cloudwatch.GetMetricDataOutput) pmetric.Metrics {
	pdm := pmetric.NewMetrics()

	resourceMetrics := pdm.ResourceMetrics().AppendEmpty()
	resource := resourceMetrics.Resource()
	resource.Attributes().PutStr(conventions.AttributeCloudProvider, conventions.AttributeCloudProviderAWS)
	resource.Attributes().PutStr(conventions.AttributeCloudRegion, m.region)

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
	scopeMetrics.Scope().SetName(receiverName)
	scopeMetrics.Scope().SetVersion(m.buildInfo.Version)

	metrics := scopeMetrics.Metrics()
	metrics.EnsureCapacity(len(resp.MetricDataResults))

	// the MetricDataResults can include as many as 500 metrics
	for _, result := range resp.MetricDataResults {
		if len(result.Values) == 0 {
			continue
		}
		metric := metrics.AppendEmpty()

		metricNamespace := rmapID[*result.Id].Namespace
		metricName := rmapID[*result.Id].MetricName
		metric.SetName(strings.ToLower(fmt.Sprintf("%s_%s", strings.ReplaceAll(metricNamespace, "/", "_"), metricName)))

		dps := metric.SetEmptyGauge().DataPoints()
		dps.EnsureCapacity(len(result.Values))

		// number of values *always* equals number of timestamps
		for iPoint := range result.Values {
			ts, value := result.Timestamps[iPoint], result.Values[iPoint]
			dp := dps.AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
			dp.SetDoubleValue(value)
			for _, dim := range rmapID[*result.Id].Dimensions {
				dp.Attributes().PutStr(strings.ToLower(*dim.Name), *dim.Value)
			}
		}
	}
	return pdm
}

func (m *metricReceiver) autoDiscoverRequests(ctx context.Context, auto *AutoDiscoverConfig) ([]request, error) {
	m.logger.Debug("discovering metrics", zap.String("namespace", auto.Namespace))

	requests := []request{}
	err := m.configureAWSClient(ctx)
	if err != nil {
		m.logger.Error("unable to establish connection auto discover metrics on cloudwatch", zap.Error(err))
	}

	input := &cloudwatch.ListMetricsInput{
		Namespace:  aws.String(auto.Namespace),
		MetricName: auto.MetricName,
		Dimensions: auto.DimensionsFilter(),
	}

	paginator := cloudwatch.NewListMetricsPaginator(m.client, input)
	for paginator.HasMorePages() {
		select {
		// if done, we want to stop processing paginated requests
		case _, ok := <-m.doneChan:
			if !ok {
				return nil, fmt.Errorf("done channel closed")
			}
		default:
			out, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, err
			}

			for _, metric := range out.Metrics {
				requests = append(requests,
					request{
						ID:             generateID(),
						Namespace:      *metric.Namespace,
						MetricName:     *metric.MetricName,
						Period:         auto.Period,
						AwsAggregation: auto.AwsAggregation,
						Dimensions:     metric.Dimensions,
					},
				)
			}
		}
	}

	m.logger.Debug("number of metrics discovered", zap.Int("metrics", len(requests)))
	return requests, nil
}

func (m *metricReceiver) configureAWSClient(ctx context.Context) error {
	if m.client != nil {
		return nil
	}

	// if "", helper functions (withXXX) ignores parameter
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(m.region),
		config.WithEC2IMDSEndpoint(m.imdsEndpoint),
		config.WithSharedConfigProfile(m.profile))

	m.client = cloudwatch.NewFromConfig(cfg)
	return err
}
