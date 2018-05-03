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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientErrorHandler;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.Event;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 */
public class StatsDEmitter implements Emitter
{
  private static final Logger log = new Logger(StatsDEmitter.class);

  static final StatsDEmitter of(StatsDEmitterConfig config, ObjectMapper mapper)
  {
    NonBlockingStatsDClient client = new NonBlockingStatsDClient(
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
    );
    return new StatsDEmitter(config, mapper, client);
  }

  private final StatsDClient statsd;
  private final StatsDEmitterConfig config;
  private final StatsDEventHandler eventHandler;
  private final Map<String, StatsDDimension> dimensionMap;

  public StatsDEmitter(StatsDEmitterConfig config, ObjectMapper mapper, StatsDClient client)
  {
    this.config = config;
    this.dimensionMap = readMap(mapper, config.getDimensionMapPath());
    this.eventHandler = config.getEventHandler();
    this.statsd = client;
  }

  @Override
  public void start() {}

  @Override
  public void emit(Event event)
  {
    eventHandler.handleEvent(statsd, event, config, dimensionMap);
  }

  @Override
  public void flush() {}

  @Override
  public void close()
  {
    statsd.stop();
  }

  private Map<String, StatsDDimension> readMap(ObjectMapper mapper, String dimensionMapPath)
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
      return mapper.reader(new TypeReference<Map<String, StatsDDimension>>()
      {
      }).readValue(is);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to parse metric dimensions and types");
    }
  }

}
