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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.Event;

import java.io.IOException;

public class ServiceEmitter implements Emitter
{
  private final ImmutableMap<String, String> serviceDimensions;
  private final Emitter emitter;

  public ServiceEmitter(String service, String host, Emitter emitter)
  {
    this(service, host, emitter, ImmutableMap.of());
  }

  public ServiceEmitter(
      String service,
      String host,
      Emitter emitter,
      ImmutableMap<String, String> otherServiceDimensions
  )
  {
    this.serviceDimensions = ImmutableMap
        .<String, String>builder()
        .put("service", Preconditions.checkNotNull(service))
        .put("host", Preconditions.checkNotNull(host))
        .putAll(otherServiceDimensions)
        .build();
    this.emitter = emitter;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    emitter.start();
  }

  @Override
  public void emit(Event event)
  {
    emitter.emit(event);
  }

  public void emit(ServiceEventBuilder builder)
  {
    emit(builder.build(serviceDimensions));
  }

  @Override
  public void flush() throws IOException
  {
    emitter.flush();
  }

  @Override
  @LifecycleStop
  public void close() throws IOException
  {
    emitter.close();
  }

  @Override
  public String toString()
  {
    return "ServiceEmitter{" +
           "serviceDimensions=" + serviceDimensions +
           ", emitter=" + emitter +
           '}';
  }
}
