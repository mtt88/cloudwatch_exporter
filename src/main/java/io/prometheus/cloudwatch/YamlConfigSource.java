package io.prometheus.cloudwatch;

import static io.prometheus.cloudwatch.Collectors.CLOUDWATCH_REQUESTS;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

class YamlConfigSource implements ConfigSource {

  private final Map<String, Object> config;
  private final Config.AwsClientConfig awsClientConfig;

  YamlConfigSource(Map<String, Object> yaml, Config.AwsClientConfig awsClientConfig) {
    this.config = yaml;
    this.awsClientConfig = awsClientConfig;
  }

  YamlConfigSource(Map<String, Object> yaml) {
    this(yaml, null);
  }

  static YamlConfigSource parseFile(String filePath) throws IOException {
    return YamlConfigSource.parseFile(filePath, null);
  }

  static YamlConfigSource parseFile(String filePath, Config.AwsClientConfig config)
      throws IOException {
    try (FileReader reader = new FileReader(filePath)) {
      return YamlConfigSource.parseReader(reader, config);
    }
  }

  static YamlConfigSource parseReader(Reader reader) {
    return YamlConfigSource.parseReader(reader, null);
  }

  static YamlConfigSource parseReader(Reader reader, Config.AwsClientConfig config) {
    var yaml = (Map<String, Object>) new Yaml().load(reader);
    return new YamlConfigSource(yaml, config);
  }

  static YamlConfigSource parseString(String yamlBody) {
    return YamlConfigSource.parseString(yamlBody, null);
  }

  static YamlConfigSource parseString(String yamlBody, Config.AwsClientConfig clientConfig) {
    var yaml = (Map<String, Object>) new Yaml().load(yamlBody);
    return new YamlConfigSource(yaml, clientConfig);
  }

  @Override
  public Config getConfig() {

    int defaultPeriod = 60;
    if (config.containsKey("period_seconds")) {
      defaultPeriod = ((Number) config.get("period_seconds")).intValue();
    }
    int defaultRange = 600;
    if (config.containsKey("range_seconds")) {
      defaultRange = ((Number) config.get("range_seconds")).intValue();
    }
    int defaultDelay = 600;
    if (config.containsKey("delay_seconds")) {
      defaultDelay = ((Number) config.get("delay_seconds")).intValue();
    }

    boolean defaultCloudwatchTimestamp = true;
    if (config.containsKey("set_timestamp")) {
      defaultCloudwatchTimestamp = (Boolean) config.get("set_timestamp");
    }

    boolean defaultUseGetMetricData = false;
    if (config.containsKey("use_get_metric_data")) {
      defaultUseGetMetricData = (Boolean) config.get("use_get_metric_data");
    }

    Duration defaultMetricCacheSeconds = Duration.ofSeconds(0);
    if (config.containsKey("list_metrics_cache_ttl")) {
      defaultMetricCacheSeconds =
          Duration.ofSeconds(((Number) config.get("list_metrics_cache_ttl")).intValue());
    }

    String region = (String) config.get("region");

    CloudWatchClient cloudWatchClient;
    if (awsClientConfig == null || awsClientConfig.getCloudWatchClient() == null) {
      CloudWatchClientBuilder clientBuilder = CloudWatchClient.builder();

      if (config.containsKey("role_arn")) {
        clientBuilder.credentialsProvider(getRoleCredentialProvider(config));
      }

      if (region != null) {
        clientBuilder.region(Region.of(region));
      }
      cloudWatchClient = clientBuilder.build();
    } else {
      cloudWatchClient = awsClientConfig.getCloudWatchClient();
    }

    ResourceGroupsTaggingApiClient taggingClient;
    if (awsClientConfig == null && awsClientConfig.getTaggingApiClient() == null) {
      ResourceGroupsTaggingApiClientBuilder taggingApiClientBuilder =
          ResourceGroupsTaggingApiClient.builder();

      if (config.containsKey("role_arn")) {
        taggingApiClientBuilder.credentialsProvider(getRoleCredentialProvider(config));
      }
      if (region != null) {
        taggingApiClientBuilder.region(Region.of(region));
      }
      taggingClient = taggingApiClientBuilder.build();
    } else {
      taggingClient = awsClientConfig.getTaggingApiClient();
    }

    if (!config.containsKey("metrics")) {
      throw new IllegalArgumentException("Must provide metrics");
    }

    CachingDimensionSource.DimensionCacheConfig metricCacheConfig =
        new CachingDimensionSource.DimensionCacheConfig(defaultMetricCacheSeconds);
    List<MetricRule> rules = new ArrayList<>();

    for (Object ruleObject : (List<Map<String, Object>>) config.get("metrics")) {
      Map<String, Object> yamlMetricRule = (Map<String, Object>) ruleObject;
      MetricRule rule = new MetricRule();
      rules.add(rule);
      if (!yamlMetricRule.containsKey("aws_namespace")
          || !yamlMetricRule.containsKey("aws_metric_name")) {
        throw new IllegalArgumentException("Must provide aws_namespace and aws_metric_name");
      }
      rule.awsNamespace = (String) yamlMetricRule.get("aws_namespace");
      rule.awsMetricName = (String) yamlMetricRule.get("aws_metric_name");
      if (yamlMetricRule.containsKey("help")) {
        rule.help = (String) yamlMetricRule.get("help");
      }
      if (yamlMetricRule.containsKey("aws_dimensions")) {
        rule.awsDimensions = (List<String>) yamlMetricRule.get("aws_dimensions");
      }
      if (yamlMetricRule.containsKey("aws_dimension_select")
          && yamlMetricRule.containsKey("aws_dimension_select_regex")) {
        throw new IllegalArgumentException(
            "Must not provide aws_dimension_select and aws_dimension_select_regex at the same time");
      }
      if (yamlMetricRule.containsKey("aws_dimension_select")) {
        rule.awsDimensionSelect =
            (Map<String, List<String>>) yamlMetricRule.get("aws_dimension_select");
      }
      if (yamlMetricRule.containsKey("aws_dimension_select_regex")) {
        rule.awsDimensionSelectRegex =
            (Map<String, List<String>>) yamlMetricRule.get("aws_dimension_select_regex");
      }
      if (yamlMetricRule.containsKey("aws_statistics")) {
        rule.awsStatistics = new ArrayList<>();
        for (String statistic : (List<String>) yamlMetricRule.get("aws_statistics")) {
          rule.awsStatistics.add(Statistic.fromValue(statistic));
        }
      } else if (!yamlMetricRule.containsKey("aws_extended_statistics")) {
        rule.awsStatistics = new ArrayList<>();
        for (String statistic :
            Arrays.asList("Sum", "SampleCount", "Minimum", "Maximum", "Average")) {
          rule.awsStatistics.add(Statistic.fromValue(statistic));
        }
      }
      if (yamlMetricRule.containsKey("aws_extended_statistics")) {
        rule.awsExtendedStatistics = (List<String>) yamlMetricRule.get("aws_extended_statistics");
      }
      if (yamlMetricRule.containsKey("period_seconds")) {
        rule.periodSeconds = ((Number) yamlMetricRule.get("period_seconds")).intValue();
      } else {
        rule.periodSeconds = defaultPeriod;
      }
      if (yamlMetricRule.containsKey("range_seconds")) {
        rule.rangeSeconds = ((Number) yamlMetricRule.get("range_seconds")).intValue();
      } else {
        rule.rangeSeconds = defaultRange;
      }
      if (yamlMetricRule.containsKey("delay_seconds")) {
        rule.delaySeconds = ((Number) yamlMetricRule.get("delay_seconds")).intValue();
      } else {
        rule.delaySeconds = defaultDelay;
      }
      if (yamlMetricRule.containsKey("set_timestamp")) {
        rule.cloudwatchTimestamp = (Boolean) yamlMetricRule.get("set_timestamp");
      } else {
        rule.cloudwatchTimestamp = defaultCloudwatchTimestamp;
      }
      if (yamlMetricRule.containsKey("use_get_metric_data")) {
        rule.useGetMetricData = (Boolean) yamlMetricRule.get("use_get_metric_data");
      } else {
        rule.useGetMetricData = defaultUseGetMetricData;
      }

      if (yamlMetricRule.containsKey("aws_tag_select")) {
        Map<String, Object> yamlAwsTagSelect =
            (Map<String, Object>) yamlMetricRule.get("aws_tag_select");
        if (!yamlAwsTagSelect.containsKey("resource_type_selection")
            || !yamlAwsTagSelect.containsKey("resource_id_dimension")) {
          throw new IllegalArgumentException(
              "Must provide resource_type_selection and resource_id_dimension");
        }
        TagSelect awsTagSelect = new TagSelect();
        rule.awsTagSelect = awsTagSelect;

        awsTagSelect.resourceTypeSelection =
            (String) yamlAwsTagSelect.get("resource_type_selection");
        awsTagSelect.resourceIdDimension = (String) yamlAwsTagSelect.get("resource_id_dimension");

        if (yamlAwsTagSelect.containsKey("tag_selections")) {
          awsTagSelect.tagSelections =
              (Map<String, List<String>>) yamlAwsTagSelect.get("tag_selections");
        }
      }

      if (yamlMetricRule.containsKey("list_metrics_cache_ttl")) {
        rule.listMetricsCacheTtl =
            Duration.ofSeconds(((Number) yamlMetricRule.get("list_metrics_cache_ttl")).intValue());
        metricCacheConfig.addOverride(rule);
      } else {
        rule.listMetricsCacheTtl = defaultMetricCacheSeconds;
      }
    }

    DimensionSource dimensionSource =
        new DefaultDimensionSource(cloudWatchClient, CLOUDWATCH_REQUESTS);
    if (defaultMetricCacheSeconds.toSeconds() > 0 || !metricCacheConfig.metricConfig.isEmpty()) {
      dimensionSource = CachingDimensionSource.create(dimensionSource, metricCacheConfig);
    }

    return new Config(
        rules, new Config.AwsClientConfig(cloudWatchClient, taggingClient), dimensionSource);
  }

  private AwsCredentialsProvider getRoleCredentialProvider(Map<String, Object> config) {
    StsClient stsClient =
        StsClient.builder().region(Region.of((String) config.get("region"))).build();
    AssumeRoleRequest assumeRoleRequest =
        AssumeRoleRequest.builder()
            .roleArn((String) config.get("role_arn"))
            .roleSessionName("cloudwatch_exporter")
            .build();
    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(stsClient)
        .refreshRequest(assumeRoleRequest)
        .build();
  }
}
