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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 */
public class DimensionConverter
{

  private static final Logger log = new Logger(DimensionConverter.class);
  private Map<String, StatsDMetric> metricMap;
  private final List<Map.Entry<String, StatsDMetric>> serviceMetricEntries;

  public DimensionConverter(ObjectMapper mapper, String dimensionMapPath)
  {
    metricMap = readMap(mapper, dimensionMapPath);
    serviceMetricEntries = metricMap.entrySet()
                                    .stream()
                                    .filter(entry -> entry.getKey().indexOf('-') > 0)
                                    .collect(Collectors.toList());
  }

  @Nullable
  public StatsDMetric addFilteredUserDims(
      String service,
      String metric,
      Map<String, Object> userDims,
      ImmutableMap.Builder<String, String> builder
  )
  {
     /*
        Find the metric in the map. If we cant find it try to look it up prefixed by the service name.
        This is because some metrics are reported differently, but with the same name, from different services.
       */
    StatsDMetric statsDMetric = null;
    if (metricMap.containsKey(metric)) {
      statsDMetric = metricMap.get(metric);
    } else {
      statsDMetric = findServiceMetric(service, metric);
    }
    if (statsDMetric != null) {
      for (String dim : statsDMetric.dimensions) {
        if (userDims.containsKey(dim)) {
          builder.put(dim, userDims.get(dim).toString());
        }
      }
      return statsDMetric;
    } else {
      return null;
    }
  }

  /**
   * Finds the service-specific mapping {@code <mapped service>-<metric>} whose mapped service is the longest prefix of
   * {@code service}, so a service such as {@code druid/historical/glacier} or {@code druid/historical-hot} uses the
   * {@code druid/historical} mapping.
   */
  @Nullable
  private StatsDMetric findServiceMetric(String service, String metric)
  {
    StatsDMetric match = null;
    int matchLength = 0;
    for (Map.Entry<String, StatsDMetric> entry : serviceMetricEntries) {
      final String key = entry.getKey();
      final int mappedServiceLength = key.length() - metric.length() - 1;
      if (mappedServiceLength > matchLength
          && key.endsWith(metric)
          && key.charAt(mappedServiceLength) == '-'
          && service.regionMatches(0, key, 0, mappedServiceLength)) {
        match = entry.getValue();
        matchLength = mappedServiceLength;
      }
    }
    return match;
  }

  private Map<String, StatsDMetric> readMap(ObjectMapper mapper, String dimensionMapPath)
  {
    try {
      InputStream is;
      if (Strings.isNullOrEmpty(dimensionMapPath)) {
        log.info("Using default metric dimension and types");
        is = this.getClass().getClassLoader().getResourceAsStream("defaultMetricDimensions.json");
      } else {
        log.info("Using metric dimensions at types at [%s]", dimensionMapPath);
        is = new FileInputStream(new File(dimensionMapPath));
      }
      return mapper.readerFor(new TypeReference<Map<String, StatsDMetric>>()
      {
      }).readValue(is);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metric dimensions and types");
    }
  }
}
