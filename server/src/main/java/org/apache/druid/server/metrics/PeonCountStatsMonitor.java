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

public class PeonCountStatsMonitor extends AbstractMonitor
{
  private final PeonCountStatsProvider statsProvider;

  @Inject
  public PeonCountStatsMonitor(
          PeonCountStatsProvider statsProvider
  )
  {
    this.statsProvider = statsProvider;
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    emit(emitter, "peon/total/count", statsProvider.getTotalPeonCount());
    emit(emitter, "peon/idle/count", statsProvider.getIdlePeonCount());
    emit(emitter, "peon/used/count", statsProvider.getUsedPeonCount());
    emit(emitter, "peon/lazy/count", statsProvider.getLazyPeonCount());
    emit(emitter, "peon/blacklisted/count", statsProvider.getBlacklistedPeonCount());
    return true;
  }

  private void emit(ServiceEmitter emitter, String key, long count)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    emitter.emit(builder.build(key, count));
  }

}
