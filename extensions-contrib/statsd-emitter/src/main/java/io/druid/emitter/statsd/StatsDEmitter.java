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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.util.Map;

/**
 */
public class StatsDEmitter implements Emitter
{

  private final static Logger log = new Logger(StatsDEmitter.class);
  private final static String DRUID_METRIC_SEPARATOR = "\\/";
  private final static String STATSD_SEPARATOR = ":|\\|";

  private final StatsDClient statsd;
  private final StatsDEmitterConfig config;
  private final DimensionConverter converter;

  public StatsDEmitter(StatsDEmitterConfig config, ObjectMapper mapper)
  {
    this(config, mapper,
         new NonBlockingStatsDClient(
             config.getPrefix(),
             config.getHostname(),
             config.getPort(),
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
         )
    );
  }

  public StatsDEmitter(StatsDEmitterConfig config, ObjectMapper mapper, StatsDClient client)
  {
    this.config = config;
    this.converter = new DimensionConverter(mapper, config.getDimensionMapPath());
    this.statsd = client;
  }

  @Override
  public void start() {}

  @Override
  public void emit(Event event)
  {
    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
      String host = metricEvent.getHost();
      String service = metricEvent.getService();
      String metric = metricEvent.getMetric();
      Map<String, Object> userDims = metricEvent.getUserDims();
      Number value = metricEvent.getValue();

      ImmutableList.Builder<String> nameBuilder = new ImmutableList.Builder<>();
      if (config.getIncludeHost()) {
        nameBuilder.add(host);
      }
      nameBuilder.add(service);
      nameBuilder.add(metric);

      StatsDMetric statsDMetric = converter.addFilteredUserDims(service, metric, userDims, nameBuilder);

      if (statsDMetric != null) {

        String fullName = Joiner.on(config.getSeparator())
                                .join(nameBuilder.build())
                                .replaceAll(DRUID_METRIC_SEPARATOR, config.getSeparator())
                                .replaceAll(STATSD_SEPARATOR, config.getSeparator());

        long val = statsDMetric.convertRange ? Math.round(value.doubleValue() * 100) : value.longValue();
        switch (statsDMetric.type) {
          case count:
            statsd.count(fullName, val);
            break;
          case timer:
            statsd.time(fullName, val);
            break;
          case gauge:
            statsd.gauge(fullName, val);
            break;
        }
      } else {
        log.debug("Metric=[%s] has no StatsD type mapping", statsDMetric);
      }
    }
  }

  @Override
  public void flush() throws IOException {}

  @Override
  public void close() throws IOException
  {
    statsd.stop();
  }

}
