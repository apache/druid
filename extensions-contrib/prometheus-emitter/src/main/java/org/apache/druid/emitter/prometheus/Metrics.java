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

package org.apache.druid.emitter.prometheus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.regex.Pattern;

public class Metrics
{

  private static final Logger log = new Logger(Metrics.class);
  private final Map<String, DimensionsAndCollector> registeredMetrics;
  private final ObjectMapper mapper = new ObjectMapper();
  public static final Pattern PATTERN = Pattern.compile("[^a-zA-Z_:][^a-zA-Z0-9_:]*");

  private static final String TAG_HOSTNAME = "host_name";
  private static final String TAG_SERVICE = "druid_service";

  public DimensionsAndCollector getByName(String name, String service)
  {
    if (registeredMetrics.containsKey(name)) {
      return registeredMetrics.get(name);
    } else {
      return registeredMetrics.getOrDefault(service + "_" + name, null);
    }
  }

  public Metrics(String namespace, String path, boolean isAddHostAsLabel, boolean isAddServiceAsLabel)
  {
    Map<String, DimensionsAndCollector> registeredMetrics = new HashMap<>();
    Map<String, Metric> metrics = readConfig(path);
    for (Map.Entry<String, Metric> entry : metrics.entrySet()) {
      String name = entry.getKey();
      Metric metric = entry.getValue();
      Metric.Type type = metric.type;

      if (isAddHostAsLabel) {
        metric.dimensions.add(TAG_HOSTNAME);
      }

      if (isAddServiceAsLabel) {
        metric.dimensions.add(TAG_SERVICE);
      }

      String[] dimensions = metric.dimensions.toArray(new String[0]);
      String formattedName = PATTERN.matcher(StringUtils.toLowerCase(name)).replaceAll("_");
      SimpleCollector collector = null;
      if (Metric.Type.count.equals(type)) {
        collector = new Counter.Builder()
            .namespace(namespace)
            .name(formattedName)
            .labelNames(dimensions)
            .help(metric.help)
            .register();
      } else if (Metric.Type.gauge.equals(type)) {
        collector = new Gauge.Builder()
            .namespace(namespace)
            .name(formattedName)
            .labelNames(dimensions)
            .help(metric.help)
            .register();
      } else if (Metric.Type.timer.equals(type)) {
        collector = new Histogram.Builder()
            .namespace(namespace)
            .name(formattedName)
            .labelNames(dimensions)
            .buckets(.1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 30, 60, 120, 300)
            .help(metric.help)
            .register();
      } else {
        log.error("Unrecognized metric type [%s]", type);
      }

      if (collector != null) {
        registeredMetrics.put(name, new DimensionsAndCollector(dimensions, collector, metric.conversionFactor));
      }
    }
    this.registeredMetrics = Collections.unmodifiableMap(registeredMetrics);
  }

  private Map<String, Metric> readConfig(String path)
  {
    try {
      InputStream is;
      if (Strings.isNullOrEmpty(path)) {
        log.info("Using default metric configuration");
        is = this.getClass().getClassLoader().getResourceAsStream("defaultMetrics.json");
      } else {
        log.info("Using metric configuration at [%s]", path);
        is = new FileInputStream(new File(path));
      }
      return mapper.readerFor(new TypeReference<Map<String, Metric>>()
      {
      }).readValue(is);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metric configuration");
    }
  }

  public Map<String, DimensionsAndCollector> getRegisteredMetrics()
  {
    return registeredMetrics;
  }

  public static class Metric
  {
    public final SortedSet<String> dimensions;
    public final Type type;
    public final String help;
    public final double conversionFactor;

    @JsonCreator
    public Metric(
        @JsonProperty("dimensions") SortedSet<String> dimensions,
        @JsonProperty("type") Type type,
        @JsonProperty("help") String help,
        @JsonProperty("conversionFactor") double conversionFactor
    )
    {
      this.dimensions = dimensions;
      this.type = type;
      this.help = help;
      this.conversionFactor = conversionFactor;
    }

    public enum Type
    {
      count, gauge, timer
    }
  }
}
