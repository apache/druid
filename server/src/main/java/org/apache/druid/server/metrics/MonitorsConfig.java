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

package org.apache.druid.server.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.query.DruidMetrics;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public class MonitorsConfig
{
  private static final Logger log = new Logger(MonitorsConfig.class);

  public static final String METRIC_DIMENSION_PREFIX = "druid.metrics.emitter.dimension.";

  /**
   * Prior to 0.12.0, Druid used Monitor classes from the `com.metamx.metrics` package.
   * In 0.12.0, these Monitor classes were moved to the Druid repo under `org.apache.druid.java.util.metrics`.
   * In 0.13.0, they were moved again to `org.apache.druid.java.util.metrics`.
   * See https://github.com/apache/druid/pull/5289 and https://github.com/apache/druid/pull/6266
   * for details.
   *
   * We automatically adjust old package references to `org.apache.druid.java.util.metrics` for backwards
   * compatibility purposes, easing the upgrade process for users.
   */
  public static final String METAMX_PACKAGE = "com.metamx.metrics";
  public static final String IO_DRUID_PACKAGE = "io.druid.java.util.metrics";
  public static final String APACHE_DRUID_PACKAGE_NAME = "org.apache.druid.java.util.metrics";

  @JsonProperty("monitors")
  @NotNull
  private List<Class<? extends Monitor>> monitors;

  public List<Class<? extends Monitor>> getMonitors()
  {
    return monitors;
  }

  public MonitorsConfig(
      @JsonProperty("monitors") List<String> monitorNames
  )
  {
    monitors = getMonitorsFromNames(monitorNames);
  }

  @Override
  public String toString()
  {
    return "MonitorsConfig{" +
           "monitors=" + monitors +
           '}';
  }

  public static Map<String, String[]> mapOfDatasourceAndTaskID(final String datasource, final String taskId)
  {
    final ImmutableMap.Builder<String, String[]> builder = ImmutableMap.builder();
    if (datasource != null) {
      builder.put(DruidMetrics.DATASOURCE, new String[]{datasource});
    }
    if (taskId != null) {
      builder.put(DruidMetrics.ID, new String[]{taskId});
    }
    return builder.build();
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

  private static List<Class<? extends Monitor>> getMonitorsFromNames(List<String> monitorNames)
  {
    List<Class<? extends Monitor>> monitors = new ArrayList<>();
    if (monitorNames == null) {
      return monitors;
    }
    try {
      for (String monitorName : monitorNames) {
        final String effectiveMonitorName = StringUtils.replace(
            StringUtils.replace(monitorName, METAMX_PACKAGE, APACHE_DRUID_PACKAGE_NAME),
            IO_DRUID_PACKAGE,
            APACHE_DRUID_PACKAGE_NAME
        );
        if (!effectiveMonitorName.equals(monitorName)) {
          log.warn(
              "Deprecated Monitor class name[%s] found, please use name[%s] instead.",
              monitorName,
              effectiveMonitorName
          );
        }
        Class<? extends Monitor> monitorClass = (Class<? extends Monitor>) Class.forName(effectiveMonitorName);
        monitors.add(monitorClass);
      }
      return monitors;
    }
    catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }
}
