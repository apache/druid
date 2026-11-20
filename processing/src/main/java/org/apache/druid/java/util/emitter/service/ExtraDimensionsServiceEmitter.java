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

package org.apache.druid.java.util.emitter.service;

import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.emitter.core.NoopEmitter;
import org.apache.druid.query.QueryMetrics;

import java.io.IOException;
import java.util.Map;

/**
 * {@link ServiceEmitter} that adds extra dimensions to each metric before passing it to a delegate. A dimension the
 * metric already has is not replaced. Useful for adding dimensions to metrics emitted by code that has no way to set
 * them, such as {@link QueryMetrics}.
 */
public class ExtraDimensionsServiceEmitter extends ServiceEmitter
{
  private final ServiceEmitter delegate;
  private final Map<String, Object> dimensions;

  public ExtraDimensionsServiceEmitter(final ServiceEmitter delegate, final Map<String, Object> dimensions)
  {
    // The service and host are unused, since events go to the delegate, which adds its own.
    super("unused", "unused", new NoopEmitter());
    this.delegate = delegate;
    this.dimensions = Map.copyOf(dimensions);
  }

  @Override
  public void emit(final Event event)
  {
    delegate.emit(event);
  }

  @Override
  public <E extends Event> void emit(final ServiceEventBuilder<E> builder)
  {
    if (builder instanceof ServiceMetricEvent.Builder) {
      final ServiceMetricEvent.Builder metricBuilder = (ServiceMetricEvent.Builder) builder;
      for (final Map.Entry<String, Object> entry : dimensions.entrySet()) {
        if (metricBuilder.getDimension(entry.getKey()) == null) {
          metricBuilder.setDimension(entry.getKey(), entry.getValue());
        }
      }
    }
    delegate.emit(builder);
  }

  @Override
  public void flush() throws IOException
  {
    delegate.flush();
  }
}
