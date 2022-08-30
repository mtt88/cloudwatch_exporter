package io.prometheus.cloudwatch;

import static io.prometheus.cloudwatch.Collectors.*;

import io.prometheus.client.Collector;
import io.prometheus.client.Collector.Describable;
import io.prometheus.cloudwatch.DataGetter.MetricRuleData;
import java.io.Reader;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.Statistic;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;
import software.amazon.awssdk.services.resourcegroupstaggingapi.model.*;

public class CloudWatchCollector extends Collector implements Describable {
  private static final Logger LOGGER = Logger.getLogger(CloudWatchCollector.class.getName());

  private static final List<String> brokenDynamoMetrics =
      Arrays.asList(
          "ConsumedReadCapacityUnits", "ConsumedWriteCapacityUnits",
          "ProvisionedReadCapacityUnits", "ProvisionedWriteCapacityUnits",
          "ReadThrottleEvents", "WriteThrottleEvents");

  private final ConfigSource configSource;
  private Config config;

  protected CloudWatchCollector(ConfigSource configSource) {
    this.configSource = configSource;
    config = configSource.getConfig();
  }

  public CloudWatchCollector(Reader in) {
    this(new FallbackConfigSource(YamlConfigSource.parseReader(in)));
  }

  public CloudWatchCollector(String yamlConfig) {
    this(new FallbackConfigSource(YamlConfigSource.parseString(yamlConfig)));
  }

  public CloudWatchCollector(
      String yamlConfig,
      CloudWatchClient cloudWatchClient,
      ResourceGroupsTaggingApiClient taggingClient) {
    this(
        new ConfigSourceBuilder()
            .yaml(yamlConfig)
            .fallbackOnError()
            .setCloudWatchClient(cloudWatchClient)
            .setTaggingApiClient(taggingClient)
            .build());
  }

  /** Convenience function to run standalone. */
  public static void main(String[] args) {
    String region = "eu-west-1";
    if (args.length > 0) {
      region = args[0];
    }
    new BuildInfoCollector().register();
    var yamlConfig =
        ("{"
                + "`region`: `"
                + region
                + "`,"
                + "`metrics`: [{`aws_namespace`: `AWS/ELB`, `aws_metric_name`: `RequestCount`, `aws_dimensions`: [`AvailabilityZone`, `LoadBalancerName`]}] ,"
                + "}")
            .replace('`', '"');
    var jc = new CloudWatchCollector(YamlConfigSource.parseString(yamlConfig));
    for (MetricFamilySamples mfs : jc.collect()) {
      System.out.println(mfs);
    }
  }

  @Override
  public List<MetricFamilySamples> describe() {
    return Collections.emptyList();
  }

  protected void reloadConfig() {
    LOGGER.log(Level.INFO, "Reloading configuration");
    synchronized (config) {
      config = configSource.getConfig();
    }
  }

  private List<ResourceTagMapping> getResourceTagMappings(
      io.prometheus.cloudwatch.MetricRule rule, ResourceGroupsTaggingApiClient taggingClient) {
    if (rule.awsTagSelect == null) {
      return Collections.emptyList();
    }

    List<TagFilter> tagFilters = new ArrayList<>();
    if (rule.awsTagSelect.tagSelections != null) {
      for (Entry<String, List<String>> entry : rule.awsTagSelect.tagSelections.entrySet()) {
        tagFilters.add(TagFilter.builder().key(entry.getKey()).values(entry.getValue()).build());
      }
    }

    List<ResourceTagMapping> resourceTagMappings = new ArrayList<>();
    GetResourcesRequest.Builder requestBuilder =
        GetResourcesRequest.builder()
            .tagFilters(tagFilters)
            .resourceTypeFilters(rule.awsTagSelect.resourceTypeSelection);
    String paginationToken = "";
    do {
      requestBuilder.paginationToken(paginationToken);

      GetResourcesResponse response = taggingClient.getResources(requestBuilder.build());
      TAGGING_API_REQUESTS.labels("getResources", rule.awsTagSelect.resourceTypeSelection).inc();

      resourceTagMappings.addAll(response.resourceTagMappingList());

      paginationToken = response.paginationToken();
    } while (paginationToken != null && !paginationToken.isEmpty());

    return resourceTagMappings;
  }

  private List<String> extractResourceIds(List<ResourceTagMapping> resourceTagMappings) {
    List<String> resourceIds = new ArrayList<>();
    for (ResourceTagMapping resourceTagMapping : resourceTagMappings) {
      resourceIds.add(extractResourceIdFromArn(resourceTagMapping.resourceARN()));
    }
    return resourceIds;
  }

  private String toSnakeCase(String str) {
    return str.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase();
  }

  private String safeName(String s) {
    // Change invalid chars to underscore, and merge underscores.
    return s.replaceAll("[^a-zA-Z0-9:_]", "_").replaceAll("__+", "_");
  }

  private String safeLabelName(String s) {
    // Change invalid chars to underscore, and merge underscores.
    return s.replaceAll("[^a-zA-Z0-9_]", "_").replaceAll("__+", "_");
  }

  private String help(io.prometheus.cloudwatch.MetricRule rule, String unit, String statistic) {
    if (rule.help != null) {
      return rule.help;
    }
    return "CloudWatch metric "
        + rule.awsNamespace
        + " "
        + rule.awsMetricName
        + " Dimensions: "
        + rule.awsDimensions
        + " Statistic: "
        + statistic
        + " Unit: "
        + unit;
  }

  private String sampleLabelSuffixBy(Statistic s) {
    switch (s) {
      case SUM:
        return "_sum";
      case SAMPLE_COUNT:
        return "_sample_count";
      case MINIMUM:
        return "_minimum";
      case MAXIMUM:
        return "_maximum";
      case AVERAGE:
        return "_average";
      default:
        throw new RuntimeException("I did not expect this stats!");
    }
  }

  private void scrape(List<MetricFamilySamples> mfs, Config config) {
    Set<String> publishedResourceInfo = new HashSet<>();

    long start = System.currentTimeMillis();
    List<MetricFamilySamples.Sample> infoSamples = new ArrayList<>();

    for (io.prometheus.cloudwatch.MetricRule rule : config.getRules()) {
      String baseName =
          safeName(rule.awsNamespace.toLowerCase() + "_" + toSnakeCase(rule.awsMetricName));
      String jobName = safeName(rule.awsNamespace.toLowerCase());
      Map<Statistic, List<MetricFamilySamples.Sample>> baseSamples = new HashMap<>();
      for (Statistic s : Statistic.values()) {
        baseSamples.put(s, new ArrayList<>());
      }
      HashMap<String, List<MetricFamilySamples.Sample>> extendedSamples = new HashMap<>();

      String unit = null;

      if (rule.awsNamespace.equals("AWS/DynamoDB")
          && rule.awsDimensions != null
          && rule.awsDimensions.contains("GlobalSecondaryIndexName")
          && brokenDynamoMetrics.contains(rule.awsMetricName)) {
        baseName += "_index";
      }

      List<ResourceTagMapping> resourceTagMappings =
          getResourceTagMappings(rule, config.getAwsClientConfig().getTaggingApiClient());
      List<String> tagBasedResourceIds = extractResourceIds(resourceTagMappings);

      List<List<Dimension>> dimensionList =
          config.getDimensionSource().getDimensions(rule, tagBasedResourceIds).getDimensions();
      DataGetter dataGetter = null;
      if (rule.useGetMetricData) {
        dataGetter =
            new io.prometheus.cloudwatch.GetMetricDataDataGetter(
                config.getAwsClientConfig().getCloudWatchClient(),
                start,
                rule,
                CLOUDWATCH_REQUESTS,
                CLOUDWATCH_METRICS_REQUESTED,
                dimensionList);
      } else {
        dataGetter =
            new io.prometheus.cloudwatch.GetMetricStatisticsDataGetter(
                config.getAwsClientConfig().getCloudWatchClient(),
                start,
                rule,
                CLOUDWATCH_REQUESTS,
                CLOUDWATCH_METRICS_REQUESTED);
      }

      for (List<Dimension> dimensions : dimensionList) {
        MetricRuleData values = dataGetter.metricRuleDataFor(dimensions);
        if (values == null) {
          continue;
        }
        unit = values.unit;
        List<String> labelNames = new ArrayList<>();
        List<String> labelValues = new ArrayList<>();
        labelNames.add("job");
        labelValues.add(jobName);
        labelNames.add("instance");
        labelValues.add("");
        for (Dimension d : dimensions) {
          labelNames.add(safeLabelName(toSnakeCase(d.name())));
          labelValues.add(d.value());
        }

        Long timestamp = null;
        if (rule.cloudwatchTimestamp) {
          timestamp = values.timestamp.toEpochMilli();
        }

        // iterate over aws statistics
        for (Entry<Statistic, Double> e : values.statisticValues.entrySet()) {
          String suffix = sampleLabelSuffixBy(e.getKey());
          baseSamples
              .get(e.getKey())
              .add(
                  new MetricFamilySamples.Sample(
                      baseName + suffix, labelNames, labelValues, e.getValue(), timestamp));
        }

        // iterate over extended values
        for (Entry<String, Double> entry : values.extendedValues.entrySet()) {
          List<MetricFamilySamples.Sample> samples =
              extendedSamples.getOrDefault(entry.getKey(), new ArrayList<>());
          samples.add(
              new MetricFamilySamples.Sample(
                  baseName + "_" + safeName(toSnakeCase(entry.getKey())),
                  labelNames,
                  labelValues,
                  entry.getValue(),
                  timestamp));
          extendedSamples.put(entry.getKey(), samples);
        }
      }

      if (!baseSamples.get(Statistic.SUM).isEmpty()) {
        mfs.add(
            new MetricFamilySamples(
                baseName + "_sum",
                Type.GAUGE,
                help(rule, unit, "Sum"),
                baseSamples.get(Statistic.SUM)));
      }
      if (!baseSamples.get(Statistic.SAMPLE_COUNT).isEmpty()) {
        mfs.add(
            new MetricFamilySamples(
                baseName + "_sample_count",
                Type.GAUGE,
                help(rule, unit, "SampleCount"),
                baseSamples.get(Statistic.SAMPLE_COUNT)));
      }
      if (!baseSamples.get(Statistic.MINIMUM).isEmpty()) {
        mfs.add(
            new MetricFamilySamples(
                baseName + "_minimum",
                Type.GAUGE,
                help(rule, unit, "Minimum"),
                baseSamples.get(Statistic.MINIMUM)));
      }
      if (!baseSamples.get(Statistic.MAXIMUM).isEmpty()) {
        mfs.add(
            new MetricFamilySamples(
                baseName + "_maximum",
                Type.GAUGE,
                help(rule, unit, "Maximum"),
                baseSamples.get(Statistic.MAXIMUM)));
      }
      if (!baseSamples.get(Statistic.AVERAGE).isEmpty()) {
        mfs.add(
            new MetricFamilySamples(
                baseName + "_average",
                Type.GAUGE,
                help(rule, unit, "Average"),
                baseSamples.get(Statistic.AVERAGE)));
      }
      for (Entry<String, List<MetricFamilySamples.Sample>> entry : extendedSamples.entrySet()) {
        mfs.add(
            new MetricFamilySamples(
                baseName + "_" + safeName(toSnakeCase(entry.getKey())),
                Type.GAUGE,
                help(rule, unit, entry.getKey()),
                entry.getValue()));
      }

      // Add the "aws_resource_info" metric for existing tag mappings
      for (ResourceTagMapping resourceTagMapping : resourceTagMappings) {
        if (!publishedResourceInfo.contains(resourceTagMapping.resourceARN())) {
          List<String> labelNames = new ArrayList<>();
          List<String> labelValues = new ArrayList<>();
          labelNames.add("job");
          labelValues.add(jobName);
          labelNames.add("instance");
          labelValues.add("");
          labelNames.add("arn");
          labelValues.add(resourceTagMapping.resourceARN());
          labelNames.add(safeLabelName(toSnakeCase(rule.awsTagSelect.resourceIdDimension)));
          labelValues.add(extractResourceIdFromArn(resourceTagMapping.resourceARN()));
          for (Tag tag : resourceTagMapping.tags()) {
            // Avoid potential collision between resource tags and other metric labels by adding the
            // "tag_" prefix
            // The AWS tags are case sensitive, so to avoid loosing information and label
            // collisions, tag keys are not snaked cased
            labelNames.add("tag_" + safeLabelName(tag.key()));
            labelValues.add(tag.value());
          }

          infoSamples.add(
              new MetricFamilySamples.Sample("aws_resource_info", labelNames, labelValues, 1));

          publishedResourceInfo.add(resourceTagMapping.resourceARN());
        }
      }
    }
    mfs.add(
        new MetricFamilySamples(
            "aws_resource_info",
            Type.GAUGE,
            "AWS information available for resource",
            infoSamples));
  }

  public List<MetricFamilySamples> collect() {
    long start = System.nanoTime();
    double error = 0;
    List<MetricFamilySamples> mfs = new ArrayList<>();
    try {
      scrape(mfs, this.config);
    } catch (Exception e) {
      error = 1;
      LOGGER.log(Level.WARNING, "CloudWatch scrape failed", e);
    }
    List<MetricFamilySamples.Sample> samples = new ArrayList<>();
    samples.add(
        new MetricFamilySamples.Sample(
            "cloudwatch_exporter_scrape_duration_seconds",
            new ArrayList<>(),
            new ArrayList<>(),
            (System.nanoTime() - start) / 1.0E9));
    mfs.add(
        new MetricFamilySamples(
            "cloudwatch_exporter_scrape_duration_seconds",
            Type.GAUGE,
            "Time this CloudWatch scrape took, in seconds.",
            samples));

    samples = new ArrayList<>();
    samples.add(
        new MetricFamilySamples.Sample(
            "cloudwatch_exporter_scrape_error", new ArrayList<>(), new ArrayList<>(), error));
    mfs.add(
        new MetricFamilySamples(
            "cloudwatch_exporter_scrape_error",
            Type.GAUGE,
            "Non-zero if this scrape failed.",
            samples));
    return mfs;
  }

  private String extractResourceIdFromArn(String arn) {
    // ARN parsing is based on
    // https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
    String[] arnArray = arn.split(":");
    String resourceId = arnArray[arnArray.length - 1];
    if (resourceId.contains("/")) {
      String[] resourceArray = resourceId.split("/", 2);
      resourceId = resourceArray[resourceArray.length - 1];
    }
    return resourceId;
  }
}
