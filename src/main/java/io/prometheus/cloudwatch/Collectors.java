package io.prometheus.cloudwatch;

import io.prometheus.client.Counter;

final class Collectors {
  static final Counter CLOUDWATCH_REQUESTS =
      Counter.build()
          .labelNames("action", "namespace")
          .name("cloudwatch_requests_total")
          .help("API requests made to CloudWatch")
          .register();

  static final Counter CLOUDWATCH_METRICS_REQUESTED =
      Counter.build()
          .labelNames("metric_name", "namespace")
          .name("cloudwatch_metrics_requested_total")
          .help("Metrics requested by either GetMetricStatistics or GetMetricData")
          .register();

  static final Counter TAGGING_API_REQUESTS =
      Counter.build()
          .labelNames("action", "resource_type")
          .name("tagging_api_requests_total")
          .help("API requests made to the Resource Groups Tagging API")
          .register();
}
