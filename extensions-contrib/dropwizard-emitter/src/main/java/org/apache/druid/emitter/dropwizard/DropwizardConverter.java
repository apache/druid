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

package org.apache.druid.emitter.dropwizard;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.curator.shaded.com.google.common.io.Closeables;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 *
 */
public class DropwizardConverter
{
  private static final Logger log = new Logger(DropwizardConverter.class);
  private final Map<String, DropwizardMetricSpec> metricMap;

  public DropwizardConverter(ObjectMapper mapper, String dimensionMapPath)
  {
    metricMap = readMap(mapper, dimensionMapPath);
  }

  /**
   * Filters user dimensions for given metric and adds them to filteredDimensions.
   * Returns null if there is no mapping present for the given metric.
   */
  @Nullable
  public DropwizardMetricSpec addFilteredUserDims(
      String service,
      String metric,
      Map<String, Object> userDims,
      Map<String, String> filteredDimensions
  )
  {

    // Find the metric in the map. If we cant find it try to look it up prefixed by the service name.
    // This is because some metrics are reported differently, but with the same name, from different services.
    DropwizardMetricSpec metricSpec = null;
    DropwizardMetricSpec dropwizardMetricSpec = metricMap.get(metric);
    if (dropwizardMetricSpec != null) {
      metricSpec = dropwizardMetricSpec;
    } else if (metricMap.containsKey(service + "-" + metric)) {
      metricSpec = metricMap.get(service + "-" + metric);
    }
    if (metricSpec != null) {
      for (String dim : metricSpec.getDimensions()) {
        if (userDims.containsKey(dim)) {
          filteredDimensions.put(dim, userDims.get(dim).toString());
        }
      }
      return metricSpec;
    } else {
      // No mapping found for given metric, return null
      return null;
    }
  }

  private Map<String, DropwizardMetricSpec> readMap(ObjectMapper mapper, String dimensionMapPath)
  {
    InputStream is = null;
    try {
      if (Strings.isNullOrEmpty(dimensionMapPath)) {
        log.info("Using default metric dimension and types");
        is = this.getClass().getClassLoader().getResourceAsStream("defaultMetricDimensions.json");
      } else {
        log.info("Using metric dimensions at types at [%s]", dimensionMapPath);
        is = new FileInputStream(new File(dimensionMapPath));
      }
      return mapper.readerFor(new TypeReference<Map<String, DropwizardMetricSpec>>()
      {
      }).readValue(is);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metric dimensions and types");
    }
    finally {
      Closeables.closeQuietly(is);
    }
  }
}
