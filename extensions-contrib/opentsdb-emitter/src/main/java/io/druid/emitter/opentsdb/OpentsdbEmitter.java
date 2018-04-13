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

package io.druid.emitter.opentsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.Event;
import io.druid.java.util.emitter.service.ServiceMetricEvent;

public class OpentsdbEmitter implements Emitter
{
  private static final Logger log = new Logger(OpentsdbEmitter.class);

  private final OpentsdbSender sender;
  private final EventConverter converter;

  public OpentsdbEmitter(OpentsdbEmitterConfig config, ObjectMapper mapper)
  {
    this.sender = new OpentsdbSender(
        config.getHost(),
        config.getPort(),
        config.getConnectionTimeout(),
        config.getReadTimeout(),
        config.getFlushThreshold(),
        config.getMaxQueueSize()
    );
    this.converter = new EventConverter(mapper, config.getMetricMapPath());
  }

  @Override
  public void start()
  {
  }

  @Override
  public void emit(Event event)
  {
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
    sender.flush();
  }

  @Override
  public void close()
  {
    sender.close();
  }
}
