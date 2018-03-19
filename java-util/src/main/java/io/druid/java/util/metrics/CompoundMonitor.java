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

package io.druid.java.util.metrics;

import com.google.common.collect.Lists;
import io.druid.java.util.emitter.service.ServiceEmitter;

import java.util.Arrays;
import java.util.List;

public abstract class CompoundMonitor implements Monitor
{
  private final List<Monitor> monitors;

  public CompoundMonitor(List<Monitor> monitors)
  {
    this.monitors = monitors;
  }

  public CompoundMonitor(Monitor... monitors)
  {
    this(Arrays.asList(monitors));
  }

  @Override
  public void start()
  {
    for (Monitor monitor : monitors) {
      monitor.start();
    }
  }

  @Override
  public void stop()
  {
    for (Monitor monitor : monitors) {
      monitor.stop();
    }
  }

  @Override
  public boolean monitor(final ServiceEmitter emitter)
  {
    return shouldReschedule(Lists.transform(monitors, monitor -> monitor.monitor(emitter)));
  }

  public abstract boolean shouldReschedule(List<Boolean> reschedules);
}
