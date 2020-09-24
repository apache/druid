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

package org.apache.druid.server.metrics;

import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

public class TaskSlotCountStatsMonitor extends AbstractMonitor
{
  private final TaskSlotCountStatsProvider statsProvider;

  @Inject
  public TaskSlotCountStatsMonitor(
      TaskSlotCountStatsProvider statsProvider
  )
  {
    this.statsProvider = statsProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emit(emitter, "taskSlot/total/count", statsProvider.getTotalTaskSlotCount());
    emit(emitter, "taskSlot/idle/count", statsProvider.getIdleTaskSlotCount());
    emit(emitter, "taskSlot/used/count", statsProvider.getUsedTaskSlotCount());
    emit(emitter, "taskSlot/lazy/count", statsProvider.getLazyTaskSlotCount());
    emit(emitter, "taskSlot/blacklisted/count", statsProvider.getBlacklistedTaskSlotCount());
    return true;
  }

  private void emit(ServiceEmitter emitter, String key, Long count)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    if (count != null) {
      emitter.emit(builder.build(key, count.longValue()));
    }
  }
}
