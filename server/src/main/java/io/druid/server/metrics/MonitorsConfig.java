/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.metamx.metrics.Monitor;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class MonitorsConfig
{
  public final static String METRIC_DIMENSION_PREFIX = "druid.metrics.emitter.dimension.";

  @JsonProperty("monitors")
  @NotNull
  private List<Class<? extends Monitor>> monitors = Lists.newArrayList();

  public List<Class<? extends Monitor>> getMonitors()
  {
    return monitors;
  }

  @Override
  public String toString()
  {
    return "MonitorsConfig{" +
           "monitors=" + monitors +
           '}';
  }

  public static Map<String, String[]> extractDimensions(Properties props, List<String> dimensions)
  {
    Map<String, String[]> dimensionsMap = new HashMap<>();
    for (String property : props.stringPropertyNames()) {
      if (property.startsWith(MonitorsConfig.METRIC_DIMENSION_PREFIX)) {
        String dimension = property.substring(MonitorsConfig.METRIC_DIMENSION_PREFIX.length());
        if (dimensions.contains(dimension)) {
          dimensionsMap.put(
              dimension,
              new String[]{props.getProperty(property)}
          );
        }
      }
    }
    return dimensionsMap;
  }
}
