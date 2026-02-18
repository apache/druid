/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.emitter.core;

/**
 */

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.slf4j.MarkerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class LoggingEmitter implements Emitter
{
  private static final Logger LOGGER = new Logger(LoggingEmitter.class);
  private static final String DEFAULT_ALLOWED_METRICS_RESOURCE = "defaultMetrics.json";

  private final Logger log;
  private final Level level;
  private final ObjectMapper jsonMapper;
  @Nullable
  private final Set<String> allowedMetrics;

  private final AtomicBoolean started = new AtomicBoolean(false);

  public LoggingEmitter(LoggingEmitterConfig config, ObjectMapper jsonMapper)
  {
    this(
        new Logger(config.getLoggerClass()),
        Level.toLevel(config.getLogLevel()),
        jsonMapper,
        config.shouldFilterMetrics(),
        config.getAllowedMetricsPath()
    );
  }

  public LoggingEmitter(Logger log, Level level, ObjectMapper jsonMapper)
  {
    this(log, level, jsonMapper, false, null);
  }

  public LoggingEmitter(
      Logger log,
      Level level,
      ObjectMapper jsonMapper,
      boolean shouldFilterMetrics,
      @Nullable String allowedMetricsPath
  )
  {
    this.log = log;
    this.level = level;
    this.jsonMapper = jsonMapper;
    this.allowedMetrics = shouldFilterMetrics ? loadAllowedMetrics(allowedMetricsPath, jsonMapper) : null;
  }

  /**
   * Loads the allowed metric names from a JSON file. If the path is null or empty,
   * loads from the bundled classpath resource (defaultMetrics.json). If a custom
   * path is provided but the file is missing, logs a warning and falls back to
   * the default classpath resource.
   */
  private static Set<String> loadAllowedMetrics(@Nullable String path, ObjectMapper jsonMapper)
  {
    final InputStream is = openAllowedMetricsStream(path);
    try {
      final Map<String, Object> metricsMap = jsonMapper.readValue(is, new TypeReference<Map<String, Object>>() {});
      return Collections.unmodifiableSet(metricsMap.keySet());
    }
    catch (IOException e) {
      final String source = path == null || Strings.isNullOrEmpty(path) ? DEFAULT_ALLOWED_METRICS_RESOURCE : path;
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(e, "Allowed metrics file must be a JSON object with metric names as keys; failed to parse [%s]", source);
    }
  }

  /**
   * Opens the allowed metrics configuration stream. Uses classpath resource when
   * path is null/empty. When a custom path is specified but the file is missing,
   * logs a warning and falls back to the default classpath resource.
   */
  private static InputStream openAllowedMetricsStream(@Nullable String path)
  {
    if (Strings.isNullOrEmpty(path)) {
      LOGGER.info("Using default allowed metrics configuration from classpath resource [%s]", DEFAULT_ALLOWED_METRICS_RESOURCE);
      return openDefaultAllowedMetricsResource();
    }
    try {
      final InputStream is = new FileInputStream(new File(path));
      LOGGER.info("Using allowed metrics configuration at [%s]", path);
      return is;
    }
    catch (FileNotFoundException e) {
      LOGGER.warn(e, "Allowed metrics file [%s] not found, falling back to default classpath resource [%s]",
                  path, DEFAULT_ALLOWED_METRICS_RESOURCE);
      return openDefaultAllowedMetricsResource();
    }
  }

  private static InputStream openDefaultAllowedMetricsResource()
  {
    final InputStream is = LoggingEmitter.class.getClassLoader().getResourceAsStream(DEFAULT_ALLOWED_METRICS_RESOURCE);
    if (is == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build("Could not find default allowed metrics resource [%s] on classpath", DEFAULT_ALLOWED_METRICS_RESOURCE);
    }
    return is;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    final boolean alreadyStarted = started.getAndSet(true);
    if (!alreadyStarted) {
      final String message = "Start: started [%s]";
      switch (level) {
        case TRACE:
          if (log.isTraceEnabled()) {
            log.trace(message, started.get());
          }
          break;
        case DEBUG:
          if (log.isDebugEnabled()) {
            log.debug(message, started.get());
          }
          break;
        case INFO:
          if (log.isInfoEnabled()) {
            log.info(message, started.get());
          }
          break;
        case WARN:
          log.warn(message, started.get());
          break;
        case ERROR:
          log.error(message, started.get());
          break;
      }
    }
  }

  @Override
  public void emit(Event event)
  {
    synchronized (started) {
      if (!started.get()) {
        throw new RejectedExecutionException("Service not started.");
      }
    }

    // Allowlist filtering: only applies to ServiceMetricEvents.
    // Non-metric events (alerts, etc.) always pass through.
    if (allowedMetrics != null && event instanceof ServiceMetricEvent) {
      final String metricName = ((ServiceMetricEvent) event).getMetric();
      if (!allowedMetrics.contains(metricName)) {
        return;
      }
    }

    try {
      switch (level) {
        case TRACE:
          if (log.isTraceEnabled()) {
            log.trace(MarkerFactory.getMarker(event.getFeed()), jsonMapper.writeValueAsString(event));
          }
          break;
        case DEBUG:
          if (log.isDebugEnabled()) {
            log.debug(MarkerFactory.getMarker(event.getFeed()), jsonMapper.writeValueAsString(event));
          }
          break;
        case INFO:
          if (log.isInfoEnabled()) {
            log.info(MarkerFactory.getMarker(event.getFeed()), jsonMapper.writeValueAsString(event));
          }
          break;
        case WARN:
          log.warn(MarkerFactory.getMarker(event.getFeed()), jsonMapper.writeValueAsString(event));
          break;
        case ERROR:
          log.error(MarkerFactory.getMarker(event.getFeed()), jsonMapper.writeValueAsString(event));
          break;
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to generate json");
    }
  }

  @Override
  public void flush()
  {

  }

  @Override
  @LifecycleStop
  public void close()
  {
    final boolean wasStarted = started.getAndSet(false);
    if (wasStarted) {
      final String message = "Close: started [%s]";
      switch (level) {
        case TRACE:
          if (log.isTraceEnabled()) {
            log.trace(message, started.get());
          }
          break;
        case DEBUG:
          if (log.isDebugEnabled()) {
            log.debug(message, started.get());
          }
          break;
        case INFO:
          if (log.isInfoEnabled()) {
            log.info(message, started.get());
          }
          break;
        case WARN:
          log.warn(message, started.get());
          break;
        case ERROR:
          log.error(message, started.get());
          break;
      }
    }
  }

  @Override
  public String toString()
  {
    return "LoggingEmitter{" +
           "log=" + log +
           ", level=" + level +
           '}';
  }

  public enum Level
  {
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR;

    public static Level toLevel(String name)
    {
      return Level.valueOf(StringUtils.toUpperCase(name));
    }
  }
}
