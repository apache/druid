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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.slf4j.MarkerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class LoggingEmitter implements Emitter
{
  private static final String DEFAULT_METRIC_ALLOWLIST_PATH = "defaultLoggingMetricNames.json";

  private final Logger log;
  private final Level level;
  private final ObjectMapper jsonMapper;
  private final boolean filterMetrics;
  private final Set<String> metricAllowlist;

  private final AtomicBoolean started = new AtomicBoolean(false);

  public LoggingEmitter(LoggingEmitterConfig config, ObjectMapper jsonMapper)
  {
    this(
        new Logger(config.getLoggerClass()),
        Level.toLevel(config.getLogLevel()),
        jsonMapper,
        config.isFilterMetrics(),
        config.isFilterMetrics() ? loadMetricAllowlist(jsonMapper, config.getMetricAllowlistPath()) : ImmutableSet.of()
    );
  }

  public LoggingEmitter(Logger log, Level level, ObjectMapper jsonMapper)
  {
    this(log, level, jsonMapper, false, ImmutableSet.of());
  }

  public LoggingEmitter(Logger log, Level level, ObjectMapper jsonMapper, boolean filterMetrics, Set<String> metricAllowlist)
  {
    this.log = log;
    this.level = level;
    this.jsonMapper = jsonMapper;
    this.filterMetrics = filterMetrics;
    this.metricAllowlist = metricAllowlist;
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
    if (shouldFilterOutMetric(event)) {
      return;
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
           ", filterMetrics=" + filterMetrics +
           '}';
  }

  private boolean shouldFilterOutMetric(Event event)
  {
    if (!filterMetrics || !(event instanceof ServiceMetricEvent)) {
      return false;
    }
    final ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
    return !metricAllowlist.contains(metricEvent.getMetric());
  }

  private static Set<String> loadMetricAllowlist(ObjectMapper mapper, String metricAllowlistPath)
  {
    final String source = Strings.isNullOrEmpty(metricAllowlistPath) ? DEFAULT_METRIC_ALLOWLIST_PATH : metricAllowlistPath;
    try (final InputStream is = openAllowlistFile(metricAllowlistPath)) {
      final JsonNode metricConfig = mapper.readTree(is);
      if (metricConfig.isArray()) {
        final ImmutableSet.Builder<String> metricNames = ImmutableSet.builder();
        for (JsonNode metric : metricConfig) {
          if (!metric.isTextual()) {
            throw new ISE("Metric allowlist file [%s] contains a non-string metric name", source);
          }
          metricNames.add(metric.asText());
        }
        return metricNames.build();
      } else {
        throw new ISE("Metric allowlist file [%s] must be a JSON array of metric names", source);
      }
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metric allowlist file [%s]", source);
    }
  }

  private static InputStream openAllowlistFile(String metricAllowlistPath) throws IOException
  {
    if (Strings.isNullOrEmpty(metricAllowlistPath)) {
      return openDefaultAllowlistFile();
    }
    try {
      return new FileInputStream(metricAllowlistPath);
    }
    catch (FileNotFoundException e) {
      return openDefaultAllowlistFile();
    }
  }

  private static InputStream openDefaultAllowlistFile()
  {
    final InputStream is = LoggingEmitter.class.getClassLoader().getResourceAsStream(DEFAULT_METRIC_ALLOWLIST_PATH);
    if (is == null) {
      throw new ISE("Metric allowlist file [%s] not found on classpath", DEFAULT_METRIC_ALLOWLIST_PATH);
    }
    return is;
  }

  @VisibleForTesting
  Set<String> getMetricAllowlist()
  {
    return metricAllowlist;
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
