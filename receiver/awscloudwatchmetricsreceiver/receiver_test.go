// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awscloudwatchmetricsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchmetricsreceiver"

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
)

const (
	namespace  = "AWS/EC2"
	metricname = "CPUUtilization"
	agg        = "Average"
	DimName    = "InstanceId"
	DimValue   = "i-1234567890abcdef0"
)

func TestDefaultFactory(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Region = "eu-west-1"

	sink := &consumertest.MetricsSink{}
	set := receivertest.NewNopCreateSettings()
	set.Logger = zap.NewNop()
	mtrcRcvr := newMetricReceiver(cfg, set, sink)
	mtrcRcvr.client = defaultMockCloudWatchClient()

	err := mtrcRcvr.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	err = mtrcRcvr.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestPoolDataUsingNamedConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Region = "eu-west-1"
	cfg.PollInterval = time.Second * 1
	cfg.Metrics = &MetricsConfig{
		NamedMetrics: []NamedMetricConfig{
			{
				Namespace:      namespace,
				Period:         time.Second * 60 * 5,
				MetricName:     metricname,
				AwsAggregation: agg,
				Dimensions: []MetricDimensionConfig{
					{
						Name:  DimName,
						Value: DimValue,
					},
				},
			},
		},
	}
	sink := &consumertest.MetricsSink{}
	set := receivertest.NewNopCreateSettings()
	set.Logger = zap.NewNop()
	mtrcRcvr := newMetricReceiver(cfg, set, sink)
	// get Id generated for the request and set it in the response
	metricID := mtrcRcvr.requests[0].ID
	getMetricDataOutput.MetricDataResults[0].Id = aws.String(metricID)
	mtrcRcvr.client = defaultMockCloudWatchClient()

	err := mtrcRcvr.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return sink.DataPointCount() > 0
	}, 2000*time.Second, 10*time.Millisecond)

	err = mtrcRcvr.Shutdown(context.Background())
	require.NoError(t, err)

	mdd := sink.AllMetrics()
	require.Len(t, mdd, 1)
	require.Equal(t, 1, mdd[0].MetricCount())
	m := mdd[0].ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0)
	assert.Equal(t, "aws_ec2_cpuutilization", m.Name())
	require.Equal(t, 1, m.Gauge().DataPoints().Len())
	assert.Equal(t, 1.0, m.Gauge().DataPoints().At(0).DoubleValue())
}

func TestPoolDataUsingAutoDiscoverConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Region = "eu-west-1"
	cfg.PollInterval = time.Second * 1
	cfg.Metrics = &MetricsConfig{
		AutoDiscover: &AutoDiscoverConfig{
			Namespace:      namespace,
			AwsAggregation: agg,
			Period:         time.Second * 60 * 5,
		},
	}
	sink := &consumertest.MetricsSink{}
	set := receivertest.NewNopCreateSettings()
	set.Logger = zap.NewNop()
	mtrcRcvr := newMetricReceiver(cfg, set, sink)
	mtrcRcvr.client = defaultMockCloudWatchClient()

	err := mtrcRcvr.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return sink.DataPointCount() > 0
	}, 2000*time.Second, 10*time.Millisecond)

	err = mtrcRcvr.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestListMetricsError(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Region = "eu-west-1"
	cfg.PollInterval = time.Second * 1
	cfg.Metrics = &MetricsConfig{
		AutoDiscover: &AutoDiscoverConfig{
			Namespace:      namespace,
			AwsAggregation: agg,
			Period:         time.Second * 60 * 5,
			Dimensions: []MetricDimensionConfig{
				{
					Name:  DimName,
					Value: DimValue,
				},
			},
		},
	}
	sink := &consumertest.MetricsSink{}
	set := receivertest.NewNopCreateSettings()
	set.Logger = zap.NewNop()
	mtrcRcvr := newMetricReceiver(cfg, set, sink)
	mtrcRcvr.client = listMetricsErrorMockClient()

	err := mtrcRcvr.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	require.Never(t, func() bool {
		return sink.DataPointCount() > 0
	}, 2*time.Second, 10*time.Millisecond)

	err = mtrcRcvr.Shutdown(context.Background())
	require.NoError(t, err)

	require.Len(t, mtrcRcvr.requests, 0)
}

func TestGetMetricDataError(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Region = "eu-west-1"
	cfg.PollInterval = time.Second * 1
	cfg.Metrics = &MetricsConfig{
		AutoDiscover: &AutoDiscoverConfig{
			Namespace:      namespace,
			AwsAggregation: agg,
			Period:         time.Second * 60 * 5,
		},
	}
	sink := &consumertest.MetricsSink{}
	set := receivertest.NewNopCreateSettings()
	set.Logger = zap.NewNop()
	mtrcRcvr := newMetricReceiver(cfg, set, sink)
	mtrcRcvr.client = getMetricDataErrorMockClient()

	err := mtrcRcvr.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	require.Never(t, func() bool {
		return sink.DataPointCount() > 0
	}, 2*time.Second, 10*time.Millisecond)

	err = mtrcRcvr.Shutdown(context.Background())
	require.NoError(t, err)
}

func TestShutdownWhileStreaming(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.Region = "eu-west-1"
	cfg.PollInterval = time.Second * 1
	cfg.Metrics = &MetricsConfig{
		NamedMetrics: []NamedMetricConfig{
			{
				Namespace:      namespace,
				Period:         time.Second * 60 * 5,
				MetricName:     metricname,
				AwsAggregation: agg,
				Dimensions: []MetricDimensionConfig{
					{
						Name:  DimName,
						Value: DimValue,
					},
				},
			},
		},
	}

	sink := &consumertest.MetricsSink{}
	set := receivertest.NewNopCreateSettings()
	set.Logger = zap.NewNop()
	mtrcRcvr := newMetricReceiver(cfg, set, sink)
	mc := &MockClient{}
	mc.On("GetMetricData", mock.Anything, mock.Anything, mock.Anything).Return(
		&cloudwatch.GetMetricDataOutput{
			MetricDataResults: []types.MetricDataResult{
				{},
			},
			NextToken: aws.String("next"),
		}, nil)
	mtrcRcvr.client = mc

	err := mtrcRcvr.Start(context.Background(), componenttest.NewNopHost())
	require.NoError(t, err)

	require.Never(t, func() bool {
		return sink.DataPointCount() > 0
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, mtrcRcvr.Shutdown(context.Background()))
}

var (
	testDimensions = []types.Dimension{
		{
			Name:  aws.String(DimName),
			Value: aws.String(DimValue),
		},
	}
	listMetricsOutput = &cloudwatch.ListMetricsOutput{
		Metrics: []types.Metric{
			{
				MetricName: aws.String(metricname),
				Namespace:  aws.String(namespace),
				Dimensions: testDimensions,
			},
		},
		NextToken: nil,
	}
	getMetricDataOutput = &cloudwatch.GetMetricDataOutput{
		MetricDataResults: []types.MetricDataResult{
			{
				Id:         aws.String("t1"),
				Label:      aws.String("testLabel"),
				Values:     []float64{1.0},
				Timestamps: []time.Time{time.Now()},
				StatusCode: types.StatusCodeComplete,
			},
		},
		NextToken: nil,
	}
)

func defaultMockCloudWatchClient() client {
	mc := &MockClient{}
	mc.On("ListMetrics", mock.Anything, mock.Anything, mock.Anything).Return(listMetricsOutput, nil)
	mc.On("GetMetricData", mock.Anything, mock.Anything, mock.Anything).Return(getMetricDataOutput, nil)

	return mc
}

func listMetricsErrorMockClient() client {
	mc := &MockClient{}
	mc.On("ListMetrics", mock.Anything, mock.Anything, mock.Anything).Return(&cloudwatch.ListMetricsOutput{}, fmt.Errorf("error on ListMetrics"))

	return mc
}

func getMetricDataErrorMockClient() client {
	mc := &MockClient{}
	mc.On("ListMetrics", mock.Anything, mock.Anything, mock.Anything).Return(listMetricsOutput, nil)
	mc.On("GetMetricData", mock.Anything, mock.Anything, mock.Anything).Return(&cloudwatch.GetMetricDataOutput{NextToken: aws.String("")}, fmt.Errorf("error on GetMetricData"))

	return mc
}

type MockClient struct {
	mock.Mock
}

func (m *MockClient) GetMetricData(ctx context.Context, params *cloudwatch.GetMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*cloudwatch.GetMetricDataOutput), args.Error(1)
}

func (m *MockClient) ListMetrics(ctx context.Context, params *cloudwatch.ListMetricsInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.ListMetricsOutput, error) {
	args := m.Called(ctx, params, optFns)
	return args.Get(0).(*cloudwatch.ListMetricsOutput), args.Error(1)
}
