/*
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.druid.emitter.statsd;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 */
public class DimensionConverter
{

  private final static Logger log = new Logger(DimensionConverter.class);
  private Map<String, StatsDMetric> metricMap;

  public DimensionConverter(ObjectMapper mapper, String dimensionMapPath)
  {
    metricMap = readMap(mapper, dimensionMapPath);
  }

  public StatsDMetric.Type addFilteredUserDims(String service, String metric, Map<String, Object> userDims, ImmutableList.Builder<String> builder)
  {
     /*
        Find the metric in the map. If we cant find it try to look it up prefixed by the service name.
        This is because some metrics are reported differently, but with the same name, from different services.
       */
    StatsDMetric statsDMetric = null;
    if (metricMap.containsKey(metric)) {
      statsDMetric = metricMap.get(metric);
    } else if (metricMap.containsKey(service + "-" + metric)) {
      statsDMetric = metricMap.get(service + "-" + metric);
    }
    if (statsDMetric != null) {
      for (String dim : statsDMetric.dimensions) {
        if (userDims.containsKey(dim)) {
          builder.add(userDims.get(dim).toString());
        }
      }
      return statsDMetric.type;
    } else {
      return null;
    }
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
      return mapper.reader(new TypeReference<Map<String, StatsDMetric>>()
      {
      }).readValue(is);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metric dimensions and types");
    }
  }
}
