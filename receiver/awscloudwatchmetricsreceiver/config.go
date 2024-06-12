// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package awscloudwatchmetricsreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/awscloudwatchmetricsreceiver"

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"go.opentelemetry.io/collector/confmap"
)

var (
	defaultPollInterval = 5 * time.Minute
)

// Config is the overall config structure for the awscloudwatchmetricsreceiver
type Config struct {
	Region       string         `mapstructure:"region"`
	Profile      string         `mapstructure:"profile"`
	IMDSEndpoint string         `mapstructure:"imds_endpoint"`
	PollInterval time.Duration  `mapstructure:"poll_interval"`
	Metrics      *MetricsConfig `mapstructure:"metrics"`
}

// MetricsConfig is the configuration for the metrics part of the receiver
// this is so we could expand to other inputs such as autodiscover
type MetricsConfig struct {
	AutoDiscover *AutoDiscoverConfig `mapstructure:"autodiscover,omitempty"`
	NamedMetrics []NamedMetricConfig `mapstructure:"named"`
}

type AutoDiscoverConfig struct {
	Namespace      string                  `mapstructure:"namespace"`
	MetricName     *string                 `mapstructure:"metric_name"`
	AwsAggregation string                  `mapstructure:"aws_aggregation"`
	Period         time.Duration           `mapstructure:"period"`
	Dimensions     []MetricDimensionConfig `mapstructure:"dimensions"`
}

type NamedMetricConfig struct {
	Namespace      string                  `mapstructure:"namespace"`
	MetricName     string                  `mapstructure:"metric_name"`
	Period         time.Duration           `mapstructure:"period"`
	AwsAggregation string                  `mapstructure:"aws_aggregation"`
	Dimensions     []MetricDimensionConfig `mapstructure:"dimensions"`
}

// MetricDimensionConfig is the configuration for the metric dimensions
type MetricDimensionConfig struct {
	Name  string `mapstructure:"name"`
	Value string `mapstructure:"value"`
}

var (
	errNoMetricsConfigured            = errors.New("no named metrics configured")
	errNoMetricNameConfigured         = errors.New("metric name was empty")
	errNoNamespaceConfigured          = errors.New("no metric namespace configured")
	errNoRegion                       = errors.New("no region was specified")
	errInvalidPollInterval            = errors.New("poll interval is incorrect, it must be a duration greater than one second")
	errAutodiscoverAndNamedConfigured = errors.New("both autodiscover and named configs are configured, only one or the other is permitted")
	errInvalidPeriod                  = errors.New("period is incorrect, it must be 1s, 5s, 10s, 30s, 60s, or any multiple of 60 seconds")
	errInvalidIMDSEndpoint            = errors.New("unable to parse URI for imds endpoint")

	// https://docs.aws.amazon.com/cli/latest/reference/cloudwatch/get-metric-data.html
	errEmptyDimensions       = errors.New("dimensions name and value is empty")
	errTooManyDimensions     = errors.New("you cannot define more than 30 dimensions for a metric")
	errDimensionColonPrefix  = errors.New("dimension name cannot start with a colon")
	errInvalidAwsAggregation = errors.New("invalid AWS aggregation")
)

func (cfg *Config) Validate() error {
	if cfg.Metrics == nil {
		return errNoMetricsConfigured
	}

	if cfg.Region == "" {
		return errNoRegion
	}

	if cfg.IMDSEndpoint != "" {
		_, err := url.ParseRequestURI(cfg.IMDSEndpoint)
		if err != nil {
			return errors.Join(errInvalidIMDSEndpoint, fmt.Errorf("imds_endpoint: %v", cfg.IMDSEndpoint))
		}
	}

	if cfg.PollInterval < time.Second {
		return errInvalidPollInterval
	}
	return cfg.validateMetricsConfig()
}

// Unmarshal is a custom unmarshaller that ensures that autodiscover is nil if
// autodiscover is not specified
func (cfg *Config) Unmarshal(componentParser *confmap.Conf) error {
	if componentParser == nil {
		return errors.New("")
	}
	err := componentParser.Unmarshal(cfg)
	if err != nil {
		return err
	}

	if componentParser.IsSet("metrics::named") && !componentParser.IsSet("metrics::autodiscover") {
		cfg.Metrics.AutoDiscover = nil
		return nil
	}

	if componentParser.IsSet("metrics::autodiscover") && !componentParser.IsSet("metrics::named") {
		cfg.Metrics.NamedMetrics = nil
		return nil
	}

	return nil
}

func (cfg *Config) validateMetricsConfig() error {
	if len(cfg.Metrics.NamedMetrics) > 0 && cfg.Metrics.AutoDiscover != nil {
		return errAutodiscoverAndNamedConfigured
	}
	if cfg.Metrics.AutoDiscover != nil {
		return validateAutoDiscoveryConfig(cfg.Metrics.AutoDiscover)
	}
	return cfg.validateNamedMetrics()
}

func validateAutoDiscoveryConfig(autodiscoveryConfig *AutoDiscoverConfig) error {
	if autodiscoveryConfig.Namespace == "" {
		return errNoNamespaceConfigured
	}

	if err := validatePeriod(autodiscoveryConfig.Period); err != nil {
		return err
	}
	if err := validateAwsAggregation(autodiscoveryConfig.AwsAggregation); err != nil {
		return err
	}
	if err := validateDimensions(autodiscoveryConfig.Dimensions); err != nil {
		return err
	}
	return nil
}

// Period have be 1, 5, 10, 30, 60, or any multiple of 60 seconds
func validatePeriod(period time.Duration) error {
	seconds := int(period.Seconds())

	switch seconds {
	case 1, 5, 10, 30, 60:
		break
	default:
		if seconds <= 0 || seconds%60 != 0 {
			return errors.Join(errInvalidPeriod, fmt.Errorf("period: %v", period))
		}
	}
	return nil
}

func (cfg *Config) validateNamedMetrics() error {

	for _, metric := range cfg.Metrics.NamedMetrics {
		if metric.Namespace == "" {
			return errNoNamespaceConfigured
		}
		if metric.MetricName == "" {
			return errNoMetricNameConfigured
		}
		if err := validatePeriod(metric.Period); err != nil {
			return err
		}
		if err := validateDimensions(metric.Dimensions); err != nil {
			return err
		}
		if err := validateAwsAggregation(metric.AwsAggregation); err != nil {
			return err
		}
	}

	return nil
}

func validateDimensions(mdc []MetricDimensionConfig) error {
	if len(mdc) > 30 {
		return errTooManyDimensions
	}
	for _, dimensionConfig := range mdc {
		if dimensionConfig.Name == "" || dimensionConfig.Value == "" {
			return errors.Join(errEmptyDimensions, fmt.Errorf("name: %v, value: %v", dimensionConfig.Name, dimensionConfig.Value))
		}
		if strings.HasPrefix(dimensionConfig.Name, ":") {
			return errors.Join(errDimensionColonPrefix, fmt.Errorf("dimension name: %v", dimensionConfig.Name))
		}
	}
	return nil
}

// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Statistics-definitions.html
func validateAwsAggregation(agg string) error {
	switch {
	case agg == "SampleCount":
		return nil
	case agg == "Sum":
		return nil
	case agg == "Average":
		return nil
	case agg == "Minimum":
		return nil
	case agg == "Maximum":
		return nil
	case agg == "IQM":
		return nil
	case strings.HasPrefix(agg, "p") && len(agg) > 1:
		return nil
	case strings.HasPrefix(agg, "TM") && len(agg) > 2:
		return nil
	case strings.HasPrefix(agg, "PR") && len(agg) > 2:
		return nil
	case strings.HasPrefix(agg, "TC") && len(agg) > 2:
		return nil
	case strings.HasPrefix(agg, "TS") && len(agg) > 2:
		return nil
	case strings.HasPrefix(agg, "WM") && len(agg) > 2:
		return nil
	default:
		return errors.Join(errInvalidAwsAggregation, fmt.Errorf("aggregation: %v", agg))
	}
}

func (a *AutoDiscoverConfig) DimensionsFilter() []types.DimensionFilter {
	if len(a.Dimensions) == 0 {
		return nil
	}

	filter := make([]types.DimensionFilter, 0, len(a.Dimensions))
	for _, d := range a.Dimensions {
		filter = append(filter, types.DimensionFilter{
			Name:  aws.String(d.Name),
			Value: aws.String(d.Value),
		})
	}
	return filter
}
