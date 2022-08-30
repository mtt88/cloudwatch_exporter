package io.prometheus.cloudwatch;

import java.util.Collections;
import java.util.List;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;

final class Config {

  private final List<MetricRule> rules;
  private final AwsClientConfig awsClientConfig;
  private final DimensionSource dimensionSource;

  Config(List<MetricRule> rules, AwsClientConfig awsClientConfig, DimensionSource dimensionSource) {
    this.rules = Collections.unmodifiableList(rules);
    this.awsClientConfig = awsClientConfig;
    this.dimensionSource = dimensionSource;
  }

  public List<MetricRule> getRules() {
    return rules;
  }

  public AwsClientConfig getAwsClientConfig() {
    return awsClientConfig;
  }

  public DimensionSource getDimensionSource() {
    return dimensionSource;
  }

  static final class AwsClientConfig {
    private final CloudWatchClient cloudWatchClient;
    private final ResourceGroupsTaggingApiClient taggingApiClient;

    AwsClientConfig(
        CloudWatchClient cloudWatchClient, ResourceGroupsTaggingApiClient taggingApiClient) {
      this.cloudWatchClient = cloudWatchClient;
      this.taggingApiClient = taggingApiClient;
    }

    public CloudWatchClient getCloudWatchClient() {
      return cloudWatchClient;
    }

    public ResourceGroupsTaggingApiClient getTaggingApiClient() {
      return taggingApiClient;
    }
  }
}
