package io.prometheus.cloudwatch;

import io.prometheus.client.Counter;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.*;

class GetMetricDataDataGetter implements DataGetter {

  private static final int MAX_QUERIES_PER_REQUEST = 500;
  // https://aws.amazon.com/cloudwatch/pricing/
  private static final int MAX_STATS_PER_BILLED_METRIC_REQUEST = 5;
  private final long start;
  private final io.prometheus.cloudwatch.MetricRule rule;
  private final CloudWatchClient client;
  private final Counter apiRequestsCounter;
  private final Counter metricsRequestedCounter;
  private final Map<String, MetricRuleData> results;
  private double metricRequestedForBilling;

  private static String dimensionToString(Dimension d) {
    return String.format("%s=%s", d.name(), d.value());
  }

  private static String dimensionsToKey(List<Dimension> dimentions) {
    return dimentions.stream()
        .map(GetMetricDataDataGetter::dimensionToString)
        .sorted()
        .collect(Collectors.joining(","));
  }

  private List<String> buildStatsList(io.prometheus.cloudwatch.MetricRule rule) {
    List<String> stats = new ArrayList<>();
    if (rule.awsStatistics != null) {
      stats.addAll(
          rule.awsStatistics.stream().map(Statistic::toString).collect(Collectors.toList()));
    }
    if (rule.awsExtendedStatistics != null) {
      stats.addAll(rule.awsExtendedStatistics);
    }
    return stats;
  }

  private List<MetricDataQuery> buildMetricDataQueries(
      io.prometheus.cloudwatch.MetricRule rule, List<List<Dimension>> dimensionsList) {
    List<MetricDataQuery> queries = new ArrayList<>();
    List<String> stats = buildStatsList(rule);
    for (String stat : stats) {
      for (List<Dimension> dl : dimensionsList) {
        Metric metric = buildMetric(dl);
        MetricStat metricStat = buildMetricStat(stat, metric);
        MetricDataQuery query = buildQuery(stat, dl, metricStat);
        queries.add(query);
      }
    }
    double metricRequested = Math.ceil((double) stats.size() / MAX_STATS_PER_BILLED_METRIC_REQUEST);
    metricRequestedForBilling += metricRequested;
    return queries;
  }

  private MetricDataQuery buildQuery(String stat, List<Dimension> dl, MetricStat metric) {
    // random id - we don't care about it
    String id = "i" + UUID.randomUUID().toString().replace("-", "");
    MetricDataQuery.Builder builder = MetricDataQuery.builder();
    builder.id(id);

    // important - used to locate back the results
    String label = MetricLabels.labelFor(stat, dl);
    builder.label(label);
    builder.metricStat(metric);
    return builder.build();
  }

  private MetricStat buildMetricStat(String stat, Metric metric) {
    MetricStat.Builder builder = MetricStat.builder();
    builder.period(rule.periodSeconds);
    builder.stat(stat);
    builder.metric(metric);
    return builder.build();
  }

  private Metric buildMetric(List<Dimension> dl) {
    Metric.Builder builder = Metric.builder();
    builder.namespace(rule.awsNamespace);
    builder.metricName(rule.awsMetricName);
    builder.dimensions(dl);
    return builder.build();
  }

  public static <T> List<List<T>> partitionByMaxSize(List<T> list, int maxPartitionSize) {
    List<List<T>> partitions = new ArrayList<>();
    List<T> remaining = list;
    while (remaining.size() > 0) {
      if (remaining.size() < maxPartitionSize) {
        partitions.add(remaining);
        break;
      } else {
        partitions.add(remaining.subList(0, maxPartitionSize));
        remaining = remaining.subList(maxPartitionSize, remaining.size());
      }
    }
    return partitions;
  }

  private List<GetMetricDataRequest> buildMetricDataRequests(
      io.prometheus.cloudwatch.MetricRule rule, List<List<Dimension>> dimensionsList) {
    Date startDate = new Date(start - 1000L * rule.delaySeconds);
    Date endDate = new Date(start - 1000L * (rule.delaySeconds + rule.rangeSeconds));
    GetMetricDataRequest.Builder builder = GetMetricDataRequest.builder();
    builder.endTime(startDate.toInstant());
    builder.startTime(endDate.toInstant());
    builder.scanBy(ScanBy.TIMESTAMP_DESCENDING);
    List<MetricDataQuery> queries = buildMetricDataQueries(rule, dimensionsList);
    List<GetMetricDataRequest> requests = new ArrayList<>();
    for (List<MetricDataQuery> queriesPartition :
        partitionByMaxSize(queries, MAX_QUERIES_PER_REQUEST)) {
      requests.add(builder.metricDataQueries(queriesPartition).build());
    }
    return requests;
  }

  private Map<String, MetricRuleData> fetchAllDataPoints(List<List<Dimension>> dimensionsList) {
    List<MetricDataResult> results = new ArrayList<>();
    for (GetMetricDataRequest request : buildMetricDataRequests(rule, dimensionsList)) {
      GetMetricDataResponse response = client.getMetricData(request);
      apiRequestsCounter.labels("getMetricData", rule.awsNamespace).inc();
      results.addAll(response.metricDataResults());
    }
    metricsRequestedCounter
        .labels(rule.awsMetricName, rule.awsNamespace)
        .inc(metricRequestedForBilling);
    return toMap(results);
  }

  private Map<String, MetricRuleData> toMap(List<MetricDataResult> metricDataResults) {
    Map<String, MetricRuleData> res = new HashMap<>();
    for (MetricDataResult dataResult : metricDataResults) {
      if (dataResult.timestamps().isEmpty() || dataResult.values().isEmpty()) {
        continue;
      }
      StatAndDimensions statAndDimensions = MetricLabels.decode(dataResult.label());
      String statString = statAndDimensions.stat;
      String labelsKey = statAndDimensions.dimetionsAsString;
      Instant timestamp = dataResult.timestamps().get(0);
      Double value = dataResult.values().get(0);
      MetricRuleData metricRuleData =
          res.getOrDefault(labelsKey, new MetricRuleData(timestamp, "N/A"));
      Statistic stat = Statistic.fromValue(statString);
      if (stat == Statistic.UNKNOWN_TO_SDK_VERSION) {
        metricRuleData.extendedValues.put(statString, value);
      } else {
        metricRuleData.statisticValues.put(stat, value);
      }
      res.put(labelsKey, metricRuleData);
    }
    return res;
  }

  GetMetricDataDataGetter(
      CloudWatchClient client,
      long start,
      io.prometheus.cloudwatch.MetricRule rule,
      Counter apiRequestsCounter,
      Counter metricsRequestedCounter,
      List<List<Dimension>> dimensionsList) {
    this.client = client;
    this.start = start;
    this.rule = rule;
    this.apiRequestsCounter = apiRequestsCounter;
    this.metricsRequestedCounter = metricsRequestedCounter;
    this.metricRequestedForBilling = 0d;
    this.results = fetchAllDataPoints(dimensionsList);
  }

  @Override
  public MetricRuleData metricRuleDataFor(List<Dimension> dimensions) {
    return results.get(dimensionsToKey(dimensions));
  }

  private static class StatAndDimensions {
    String dimetionsAsString;
    String stat;

    StatAndDimensions(String stat, String dimetionsAsString) {
      this.stat = stat;
      this.dimetionsAsString = dimetionsAsString;
    }
  }

  static class MetricLabels {
    static String labelFor(String stat, List<Dimension> dimensions) {
      return String.format("%s/%s", stat, dimensionsToKey(dimensions));
    }

    static StatAndDimensions decode(String label) {
      String[] labelParts = label.split("/", 2);
      if (labelParts.length != 2) {
        throw new UnexpectedLabel(label);
      }
      String statString = labelParts[0];
      String labelsKey = labelParts[1];
      return new StatAndDimensions(statString, labelsKey);
    }

    static class UnexpectedLabel extends RuntimeException {
      public UnexpectedLabel(String label) {
        super(String.format("Cannot decode label %s", label));
      }
    }
  }
}
