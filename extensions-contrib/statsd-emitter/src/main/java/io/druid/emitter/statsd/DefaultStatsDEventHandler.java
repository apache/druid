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

package io.druid.emitter.statsd;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import com.timgroup.statsd.StatsDClient;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.Event;
import io.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.Map;

public class DefaultStatsDEventHandler implements StatsDEventHandler
{
  private static final Logger log = new Logger(DefaultStatsDEventHandler.class);
  private static final String DRUID_METRIC_SEPARATOR = "\\/";
  private static final String STATSD_SEPARATOR = ":|\\|";
  private static final String BLANK = "\\s+";

  @Override
  public void handleEvent(
      StatsDClient statsDClient,
      Event event,
      StatsDEmitterConfig config,
      Map<String, StatsDDimension> filterMap
  )
  {
    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
      String metric = metricEvent.getMetric();
      String service = metricEvent.getService();

      /*
      Find the metric in the map. If we cant find it try to look it up prefixed by the service name.
      This is because some metrics are reported differently, but with the same name, from different services.
      */
      StatsDDimension statsDDimension = null;
      if (filterMap.containsKey(metric)) {
        statsDDimension = filterMap.get(metric);
      } else if (filterMap.containsKey(service + "-" + metric)) {
        statsDDimension = filterMap.get(service + "-" + metric);
      }
      if (statsDDimension != null) {
        Map<String, Object> userDims = metricEvent.getUserDims();
        Number value = metricEvent.getValue();
        String host = metricEvent.getHost();

        ImmutableList.Builder<String> nameBuilder = new ImmutableList.Builder<>();
        if (config.getIncludeHost()) {
          nameBuilder.add(host);
        }
        nameBuilder.add(service);
        nameBuilder.add(metric);
        for (String dim : statsDDimension.dimensions) {
          if (userDims.containsKey(dim)) {
            nameBuilder.add(userDims.get(dim).toString());
          }
        }
        String fullName = Joiner.on(config.getSeparator())
                                .join(nameBuilder.build())
                                .replaceAll(DRUID_METRIC_SEPARATOR, config.getSeparator())
                                .replaceAll(STATSD_SEPARATOR, config.getSeparator())
                                .replaceAll(BLANK, config.getBlankHolder());
        long val = statsDDimension.convertRange ? Math.round(value.doubleValue() * 100) : value.longValue();
        switch (statsDDimension.type) {
          case count:
            statsDClient.count(fullName, val);
            break;
          case timer:
            statsDClient.time(fullName, val);
            break;
          case gauge:
            statsDClient.gauge(fullName, val);
            break;
        }
      }
      log.debug("Metric=[%s] has no StatsD type mapping", filterMap);
    } else {
      log.debug("Ignored, don't know how to handle events of feed type [%s]", event.getFeed());
    }
  }
}
