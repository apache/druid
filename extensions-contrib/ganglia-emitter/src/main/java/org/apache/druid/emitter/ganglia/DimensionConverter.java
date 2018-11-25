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

package org.apache.druid.emitter.ganglia;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Map;

public class DimensionConverter
{
  private static final Logger log = new Logger(DimensionConverter.class);

  public GangliaMetric addFilteredUserDims(
      String service,
      String metric,
      Map<String, Object> userDims,
      ImmutableList.Builder<String> builder,
      Map<String, GangliaMetric> metricMap
  )
  {
     /*
        Find the metric in the map. If we cant find it try to look it up prefixed by the service name.
        This is because some metrics are reported differently, but with the same name, from different services.
       */
    GangliaMetric gangliaMetric = null;
    if (metricMap.containsKey(metric)) {
      gangliaMetric = metricMap.get(metric);
      log.debug("metric : " + metric + " gangliaMetric.dimensions:" + gangliaMetric.dimensions + " dim: " + userDims);
    } else if (metricMap.containsKey(service + "-" + metric)) {
      gangliaMetric = metricMap.get(service + "-" + metric);
      log.debug(" server metric : " + metric + " gangliaMetric.dimensions:" + gangliaMetric.dimensions + " dim: " + userDims);
    } else {
      log.debug("not found");
    }
    if (gangliaMetric != null) {
      for (String dim : gangliaMetric.dimensions) {
        if (userDims.containsKey(dim)) {
          builder.add(userDims.get(dim).toString());
        }
      }
      return gangliaMetric;
    }
    return null;
  }
}
