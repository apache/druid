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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class PrometheusEmitter implements Emitter
{

  private static final Logger log = new Logger(PrometheusEmitter.class);

  private final PrometheusReporter reporter;
  private final AtomicBoolean starter = new AtomicBoolean(false);


  public PrometheusEmitter(PrometheusEmitterConfig config, ObjectMapper mapper)
  {

    this.reporter = new PrometheusReporter(mapper,
        config.getMetricMapPath(),
        config.getHost(),
        config.getPort(),
        config.getNameSpace());

  }

  @Override
  public void start()
  {
    synchronized (starter) {
      if (!starter.get()) {
        log.info("prometheus emitter started");
        starter.set(true);
      }
    }
  }

  @Override
  public void emit(Event event)
  {
    if (!starter.get()) {
      throw new ISE("WTF emit was called while service is not started yet");
    }

    if (event instanceof ServiceMetricEvent) {
      ServiceMetricEvent metricEvent = (ServiceMetricEvent) event;
      reporter.emitMetric(metricEvent);

      reporter.push(metricEvent.getHost());
    }
  }

  @Override
  public void flush() throws IOException
  {
    if (starter.get()) {
      log.info("druid prometheus emitter flush");
    }
  }

  @Override
  public void close() throws IOException
  {
    if (starter.get()) {
      log.info("druid prometheus emitter close");
      starter.set(false);
    }
  }
}
