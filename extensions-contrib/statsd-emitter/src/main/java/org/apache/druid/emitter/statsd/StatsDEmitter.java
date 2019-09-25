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

package org.apache.druid.emitter.statsd;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.timgroup.statsd.Event.AlertType;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.AlertEvent;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 */
public class StatsDEmitter implements Emitter
{

  private static final Logger log = new Logger(StatsDEmitter.class);
  private static final char DRUID_METRIC_SEPARATOR = '/';
  private static final String DRUID_DEFAULT_PREFIX = "druid";
  private static final Pattern STATSD_SEPARATOR = Pattern.compile("[:|]");
  private static final Pattern BLANK = Pattern.compile("\\s+");
  private static final String[] EMPTY_ARRAY = new String[0];
  private static final String TAG_HOSTNAME = "hostname";
  private static final String TAG_SERVICE = "druid_service";
  private static final String TAG_FEED = "feed";
  private static final String TAG_SEVERITY = "severity";

  static StatsDEmitter of(StatsDEmitterConfig config, ObjectMapper mapper)
  {
    NonBlockingStatsDClient client = new NonBlockingStatsDClient(
        config.getPrefix(),
        config.getHostname(),
        config.getPort(),
        config.isDogstatsd() ? config.getDogstatsdConstantTags().toArray(new String[0]) : EMPTY_ARRAY,
        new StatsDClientErrorHandler()
        {
          private int exceptionCount = 0;

          @Override
          public void handle(Exception exception)
          {
            if (exceptionCount % 1000 == 0) {
              log.error(exception, "Error sending metric to StatsD.");
            }
            exceptionCount += 1;
          }
        }
    );
    return new StatsDEmitter(config, mapper, client);
  }

  private final StatsDClient statsd;
  private final StatsDEmitterConfig config;
  private final DimensionConverter converter;
  private final ObjectMapper mapper;

  public StatsDEmitter(StatsDEmitterConfig config, ObjectMapper mapper, StatsDClient client)
  {
    this.config = config;
    this.converter = new DimensionConverter(mapper, config.getDimensionMapPath());
    this.statsd = client;
    this.mapper = mapper;
  }

  @Override
  public void start()
  {
  }

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      emitMetric((ServiceMetricEvent) event);
    } else if (event instanceof AlertEvent && config.isDogstatsd() && config.isDogstatsdEvents()) {
      emitAlert((AlertEvent) event);
    }
  }

  void emitMetric(ServiceMetricEvent metricEvent)
  {
    String host = metricEvent.getHost();
    String service = metricEvent.getService();
    String metric = metricEvent.getMetric();
    Map<String, Object> userDims = metricEvent.getUserDims();
    Number value = metricEvent.getValue();

    ImmutableList.Builder<String> nameBuilder = new ImmutableList.Builder<>();
    ImmutableMap.Builder<String, String> dimsBuilder = new ImmutableMap.Builder<>();

    if (config.isDogstatsd() && config.isDogstatsdServiceAsTag()) {
      dimsBuilder.put(TAG_SERVICE, service);
      nameBuilder.add(DRUID_DEFAULT_PREFIX);
    } else {
      nameBuilder.add(service);
    }
    nameBuilder.add(metric);

    StatsDMetric statsDMetric = converter.addFilteredUserDims(service, metric, userDims, dimsBuilder);

    if (statsDMetric != null) {
      List<String> fullNameList;
      String[] tags;
      if (config.isDogstatsd()) {
        if (config.getIncludeHost()) {
          dimsBuilder.put(TAG_HOSTNAME, host);
        }

        fullNameList = nameBuilder.build();
        tags = tagsFromMap(dimsBuilder.build());
      } else {
        ImmutableList.Builder<String> fullNameBuilder = new ImmutableList.Builder<>();
        if (config.getIncludeHost()) {
          fullNameBuilder.add(host);
        }
        fullNameBuilder.addAll(nameBuilder.build());
        fullNameBuilder.addAll(dimsBuilder.build().values());

        fullNameList = fullNameBuilder.build();
        tags = EMPTY_ARRAY;
      }

      String fullName = Joiner.on(config.getSeparator()).join(fullNameList);
      fullName = StringUtils.replaceChar(fullName, DRUID_METRIC_SEPARATOR, config.getSeparator());
      fullName = STATSD_SEPARATOR.matcher(fullName).replaceAll(config.getSeparator());
      fullName = BLANK.matcher(fullName).replaceAll(config.getBlankHolder());

      if (config.isDogstatsd() && (value instanceof Float || value instanceof Double)) {
        switch (statsDMetric.type) {
          case count:
            statsd.count(fullName, value.doubleValue(), tags);
            break;
          case timer:
            statsd.time(fullName, value.longValue(), tags);
            break;
          case gauge:
            statsd.gauge(fullName, value.doubleValue(), tags);
            break;
        }
      } else {
        long val = statsDMetric.convertRange && !config.isDogstatsd() ?
            Math.round(value.doubleValue() * 100) :
            value.longValue();

        switch (statsDMetric.type) {
          case count:
            statsd.count(fullName, val, tags);
            break;
          case timer:
            statsd.time(fullName, val, tags);
            break;
          case gauge:
            statsd.gauge(fullName, val, tags);
            break;
        }
      }
    } else {
      log.debug("Service=[%s], Metric=[%s] has no StatsD type mapping", service, metric);
    }
  }

  void emitAlert(AlertEvent alertEvent)
  {
    ImmutableMap.Builder<String, String> tagBuilder = ImmutableMap.builder();

    tagBuilder
        .put(TAG_FEED, alertEvent.getFeed())
        .put(TAG_SERVICE, alertEvent.getService())
        .put(TAG_SEVERITY, alertEvent.getSeverity().toString());
    if (config.getIncludeHost()) {
      tagBuilder.put(TAG_HOSTNAME, alertEvent.getHost());
    }

    String text;
    try {
      text = mapper.writeValueAsString(alertEvent.getDataMap());
    }
    catch (JsonProcessingException e) {
      log.error(e, "Unable to convert alert data to json");
      text = "Unable to convert alert data to JSON: " + e.getMessage();
    }
    statsd.recordEvent(
        com.timgroup.statsd.Event
            .builder()
            .withDate(alertEvent.getCreatedTime().getMillis())
            .withAlertType(alertType(alertEvent.getSeverity()))
            .withPriority(com.timgroup.statsd.Event.Priority.NORMAL)
            .withTitle(alertEvent.getDescription())
            .withText(text)
            .build(),
        tagsFromMap(tagBuilder.build())
    );
  }

  private static String[] tagsFromMap(Map<String, String> tags)
  {
    return tags.entrySet()
               .stream()
               .map(e -> e.getKey() + ":" + e.getValue())
               .toArray(String[]::new);
  }

  private static AlertType alertType(AlertEvent.Severity severity)
  {
    switch (severity) {
      case ANOMALY:
        return AlertType.WARNING;
      case COMPONENT_FAILURE:
      case SERVICE_FAILURE:
        return AlertType.ERROR;
      default:
        return AlertType.INFO;
    }
  }

  @Override
  public void flush()
  {
  }

  @Override
  public void close()
  {
    statsd.stop();
  }

}
