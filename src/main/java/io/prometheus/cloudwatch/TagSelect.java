package io.prometheus.cloudwatch;

import java.util.List;
import java.util.Map;

class TagSelect {
  String resourceTypeSelection;
  String resourceIdDimension;
  Map<String, List<String>> tagSelections;
}
