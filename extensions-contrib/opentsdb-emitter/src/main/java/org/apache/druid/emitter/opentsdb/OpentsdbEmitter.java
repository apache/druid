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

package org.apache.druid.emitter.opentsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.concurrent.atomic.AtomicBoolean;

public class OpentsdbEmitter implements Emitter
{
  private static final Logger log = new Logger(OpentsdbEmitter.class);

  private final OpentsdbSender sender;
  private final EventConverter converter;
  private final AtomicBoolean started = new AtomicBoolean(false);

  public OpentsdbEmitter(OpentsdbEmitterConfig config, ObjectMapper mapper)
  {
    this.sender = new OpentsdbSender(
        config.getHost(),
        config.getPort(),
        config.getConnectionTimeout(),
        config.getReadTimeout(),
        config.getFlushThreshold(),
        config.getMaxQueueSize(),
        config.getConsumeDelay()
    );
    this.converter = new EventConverter(mapper, config.getMetricMapPath(), config.getNamespacePrefix());
  }

  @Override
  public void start()
  {
    synchronized (started) {
      if (!started.get()) {
        log.info("Starting Opentsdb Emitter.");
        sender.start();
        started.set(true);
      }
    }
  }

  @Override
  public void emit(Event event)
  {
    if (!started.get()) {
      throw new ISE("WTF emit was called while service is not started yet");
    }
    if (event instanceof ServiceMetricEvent) {
      OpentsdbEvent opentsdbEvent = converter.convert((ServiceMetricEvent) event);
      if (opentsdbEvent != null) {
        sender.enqueue(opentsdbEvent);
      } else {
        log.debug(
            "Metric=[%s] has not been configured to be emitted to opentsdb",
            ((ServiceMetricEvent) event).getMetric()
        );
      }
    }
  }

  @Override
  public void flush()
  {
    if (started.get()) {
      sender.flush();
    }
  }

  @Override
  public void close()
  {
    if (started.get()) {
      sender.close();
      started.set(false);
    }
  }
}
