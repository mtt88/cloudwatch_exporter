package io.prometheus.cloudwatch;

import java.io.IOException;
import java.io.Reader;
import java.util.function.Function;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;

class ConfigSourceBuilder {

  private CloudWatchClient cloudWatchClient;
  private ResourceGroupsTaggingApiClient taggingApiClient;
  private boolean fallbackOnError = true;
  private Function<Config.AwsClientConfig, ConfigSource> create;

  ConfigSourceBuilder setCloudWatchClient(CloudWatchClient cloudWatchClient) {
    this.cloudWatchClient = cloudWatchClient;
    return this;
  }

  ConfigSourceBuilder setTaggingApiClient(ResourceGroupsTaggingApiClient taggingApiClient) {
    this.taggingApiClient = taggingApiClient;
    return this;
  }

  ConfigSourceBuilder fallbackOnError() {
    return this.fallbackOnError(true);
  }

  ConfigSourceBuilder fallbackOnError(boolean fallbackOnError) {
    this.fallbackOnError = fallbackOnError;
    return this;
  }

  ConfigSourceBuilder yaml(String yaml) {
    this.create = (config) -> YamlConfigSource.parseString(yaml, config);
    return this;
  }

  ConfigSourceBuilder yamlFile(String filePath) {
    this.create =
        (config) -> {
          try {
            return YamlConfigSource.parseFile(filePath, config);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        };
    return this;
  }

  ConfigSourceBuilder yamlReader(Reader reader) {
    this.create = (config) -> YamlConfigSource.parseReader(reader, config);
    return this;
  }

  ConfigSource build() {
    ConfigSource configSource;
    if (taggingApiClient != null || cloudWatchClient != null) {
      configSource =
          this.create.apply(
              new Config.AwsClientConfig(this.cloudWatchClient, this.taggingApiClient));
    } else {
      configSource = this.create.apply(null);
    }
    if (fallbackOnError) {
      configSource = new FallbackConfigSource(configSource);
    }
    return configSource;
  }
}
