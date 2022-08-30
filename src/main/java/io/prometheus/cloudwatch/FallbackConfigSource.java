package io.prometheus.cloudwatch;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class FallbackConfigSource implements ConfigSource {
  private static final Logger LOGGER = Logger.getLogger(FallbackConfigSource.class.getName());

  private Config config;
  private final ConfigSource configSource;

  public FallbackConfigSource(ConfigSource configSource) {
    this.configSource = configSource;
  }

  @Override
  public Config getConfig() {
    try {
      config = configSource.getConfig();
    } catch (Exception ex) {
      LOGGER.log(Level.WARNING, "Unable to load config, using existing config", ex);
    }
    return config;
  }
}
