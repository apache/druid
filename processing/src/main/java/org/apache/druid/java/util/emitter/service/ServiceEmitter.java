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
import org.apache.druid.java.util.metrics.NoopTaskHolder;
import org.apache.druid.java.util.metrics.TaskHolder;

import java.io.IOException;

public class ServiceEmitter implements Emitter
{
  private final Emitter emitter;
  private final String service;
  private final ImmutableMap<String, String> otherServiceDimensions;
  private final String host;
  private final TaskHolder taskHolder;

  /**
   * This is initialized in {@link #start()} rather than in the constructor, since calling {@link TaskHolder#getMetricDimensions()}
   * may introduce cyclic dependencies. So we defer initialization until {@link #start()} which is {@link LifecycleStart} managed.
   */
  private ImmutableMap<String, String> serviceDimensions;

  public ServiceEmitter(String service, String host, Emitter emitter)
  {
    this(service, host, emitter, ImmutableMap.of(), new NoopTaskHolder());
  }

  public ServiceEmitter(
      String service,
      String host,
      Emitter emitter,
      ImmutableMap<String, String> otherServiceDimensions,
      TaskHolder taskHolder
  )
  {
    this.service = Preconditions.checkNotNull(service, "service should be non-null");
    this.host = Preconditions.checkNotNull(host, "host should be non-null");
    this.otherServiceDimensions = otherServiceDimensions;
    this.emitter = emitter;
    this.taskHolder = taskHolder;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    serviceDimensions = ImmutableMap
        .<String, String>builder()
        .put(Event.SERVICE, service)
        .put(Event.HOST, host)
        .putAll(otherServiceDimensions)
        .putAll(taskHolder.getMetricDimensions())
        .build();
    emitter.start();
  }

  @Override
  public void emit(Event event)
  {
    emitter.emit(event);
  }

  public <E extends Event> void emit(ServiceEventBuilder<E> builder)
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
