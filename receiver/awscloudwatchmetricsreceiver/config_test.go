// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awscloudwatchmetricsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchmetricsreceiver"

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name        string
		config      Config
		expectedErr error
	}{
		{
			name:        "No metric key configured",
			config:      Config{},
			expectedErr: errNoMetricsConfigured,
		},
		{
			name: "No region configured",
			config: Config{
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "InstanceId",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errNoRegion,
		},
		{
			name: "Invalid IMDS endpoint conifgured",
			config: Config{
				Region:       "us-west-2",
				IMDSEndpoint: "a$B",
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "InstanceId",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errInvalidIMDSEndpoint,
		},
		{
			name: "Poll interval less than 1 second",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: time.Millisecond * 500,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "InstanceId",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errInvalidPollInterval,
		},
		{
			name: "Both named and auto discover parameters are defined",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "InstanceId",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
					AutoDiscover: &AutoDiscoverConfig{
						Namespace:      "AWS/EC2",
						AwsAggregation: "Average",
						Period:         time.Second * 60 * 5,
						Dimensions:     []MetricDimensionConfig{},
					},
				},
			},
			expectedErr: errAutodiscoverAndNamedConfigured,
		},
		// Auto discover configs
		{
			name: "valid autodiscover config",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: defaultPollInterval,
				IMDSEndpoint: "http://localhost:8080",
				Metrics: &MetricsConfig{
					AutoDiscover: &AutoDiscoverConfig{
						Namespace:      "AWS/EC2",
						AwsAggregation: "Average",
						Period:         time.Second * 60 * 5,
						Dimensions: []MetricDimensionConfig{
							{
								Name:  "InstanceId",
								Value: "i-1234567890abcdef0",
							},
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "No namespace configured in autodiscover config",
			config: Config{
				Region:       "eu-west-2",
				PollInterval: time.Minute * 5,
				Metrics: &MetricsConfig{
					AutoDiscover: &AutoDiscoverConfig{
						Period:         time.Second * 60 * 5,
						AwsAggregation: "Sum",
					},
				},
			},
			expectedErr: errNoNamespaceConfigured,
		},
		{
			name: "No valid period in autodiscover config",
			config: Config{
				Region:       "eu-west-2",
				PollInterval: time.Minute * 3,
				Metrics: &MetricsConfig{
					AutoDiscover: &AutoDiscoverConfig{
						Namespace:      "AWS/EC2",
						Period:         time.Second * 3,
						AwsAggregation: "Sum",
					},
				},
			},
			expectedErr: errInvalidPeriod,
		},
		{
			name: "No valid aggregation in autodiscover config",
			config: Config{
				Region:       "eu-west-2",
				PollInterval: time.Minute * 5,
				Metrics: &MetricsConfig{
					AutoDiscover: &AutoDiscoverConfig{
						Namespace:      "AWS/EC2",
						Period:         time.Minute,
						AwsAggregation: "InvalidAggregation",
					},
				},
			},
			expectedErr: errInvalidAwsAggregation,
		},
		{
			name: "Empty dimensions in autodiscover config",
			config: Config{
				Region:       "eu-west-2",
				PollInterval: time.Minute * 5,
				Metrics: &MetricsConfig{
					AutoDiscover: &AutoDiscoverConfig{
						Namespace:      "AWS/EC2",
						Period:         time.Minute,
						AwsAggregation: "Average",
						Dimensions: []MetricDimensionConfig{
							{
								Name:  "",
								Value: "",
							},
						},
					},
				},
			},
			expectedErr: errEmptyDimensions,
		},
		{
			name: "Too many dimensions in autodiscover config",
			config: Config{
				Region:       "eu-west-2",
				PollInterval: time.Minute * 5,
				Metrics: &MetricsConfig{
					AutoDiscover: &AutoDiscoverConfig{
						Namespace:      "AWS/EC2",
						Period:         time.Minute,
						AwsAggregation: "Average",
						Dimensions:     getTooManyDimensions(),
					},
				},
			},
			expectedErr: errTooManyDimensions,
		},
		// Named configs
		{
			name: "Valid named config",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				IMDSEndpoint: "http://localhost:8080",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "InstanceId",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: nil,
		},
		{
			name: "No namespace configured in named config",
			config: Config{
				Region:       "eu-west-2",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "InstanceId",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errNoNamespaceConfigured,
		},
		{
			name: "No metric_name configured in named config",
			config: Config{
				Region:       "eu-west-2",
				PollInterval: time.Minute * 5,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "",
							AwsAggregation: "Sum",
						},
					},
				},
			},
			expectedErr: errNoMetricNameConfigured,
		},
		{
			name: "Name parameter in dimension is empty",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errEmptyDimensions,
		},
		{
			name: "Value parameter in dimension is empty",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "InstanceId",
									Value: "",
								},
							},
						},
					},
				},
			},
			expectedErr: errEmptyDimensions,
		},
		{
			name: "Name parameter in dimension starts with a colon",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60 * 5,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  ":InvalidName",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errDimensionColonPrefix,
		},
		{
			name: "No valid period in named config",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 65,
							MetricName:     "CPUUtilization",
							AwsAggregation: "Average",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "Id",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errInvalidPeriod,
		},
		{
			name: "No valid aggregation in named config",
			config: Config{
				Region:       "us-west-2",
				Profile:      "my_profile",
				PollInterval: defaultPollInterval,
				Metrics: &MetricsConfig{
					NamedMetrics: []NamedMetricConfig{
						{
							Namespace:      "AWS/EC2",
							Period:         time.Second * 60,
							MetricName:     "CPUUtilization",
							AwsAggregation: "AverageInvalid",
							Dimensions: []MetricDimensionConfig{
								{
									Name:  "Id",
									Value: "i-1234567890abcdef0",
								},
							},
						},
					},
				},
			},
			expectedErr: errInvalidAwsAggregation,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.expectedErr != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedErr.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateAwsAggregation(t *testing.T) {

	valid := []string{
		"Average",
		"Minimum",
		"Maximum",
		"SampleCount",
		"Sum",
		"IQM",
		"p95",
		"TM(150:1000)",
		"WM(10%:90%)",
		"PR(100:2000)",
		"TC(0.005:0.030)",
		"TS(80%:)",
	}

	invalid := []string{
		"InvalidAggregation",
		"Avg",
		"Min",
		"Max",
		"Sample",
		"Summation",
		"p",
		"TM",
		"WM",
		"PR",
		"TC",
		"TS",
	}

	for _, agg := range valid {
		err := validateAwsAggregation(agg)
		require.NoError(t, err)
	}

	for _, agg := range invalid {
		err := validateAwsAggregation(agg)
		require.Error(t, err)
	}
}

func getTooManyDimensions() []MetricDimensionConfig {
	var dimensions []MetricDimensionConfig
	for i := 0; i < 31; i++ {
		dimensions = append(dimensions, MetricDimensionConfig{
			Name:  "Name",
			Value: "Value",
		})
	}
	return dimensions
}
