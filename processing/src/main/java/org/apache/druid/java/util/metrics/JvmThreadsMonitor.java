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

package org.apache.druid.java.util.metrics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Map;

public class JvmThreadsMonitor extends FeedDefiningMonitor
{
  private final Map<String, String[]> dimensions;

  private int lastLiveThreads = 0;
  private long lastStartedThreads = 0;

  public JvmThreadsMonitor(Map<String, String[]> dimensions)
  {
    this(dimensions, DEFAULT_METRICS_FEED);
  }

  public JvmThreadsMonitor(Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    Preconditions.checkNotNull(dimensions, "dimensions");
    this.dimensions = ImmutableMap.copyOf(dimensions);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    final ServiceMetricEvent.Builder builder = builder();
    MonitorUtils.addDimensionsToBuilder(builder, dimensions);

    // Because between next two calls on ThreadMXBean new threads can be started we can observe some inconsistency
    // in counters values and finished counter could be even negative
    int newLiveThreads = threadBean.getThreadCount();
    long newStartedThreads = threadBean.getTotalStartedThreadCount();

    long startedThreadsDiff = newStartedThreads - lastStartedThreads;

    emitter.emit(builder.build("jvm/threads/started", startedThreadsDiff));
    emitter.emit(builder.build("jvm/threads/finished", lastLiveThreads + startedThreadsDiff - newLiveThreads));
    emitter.emit(builder.build("jvm/threads/live", newLiveThreads));
    emitter.emit(builder.build("jvm/threads/liveDaemon", threadBean.getDaemonThreadCount()));

    emitter.emit(builder.build("jvm/threads/livePeak", threadBean.getPeakThreadCount()));
    threadBean.resetPeakThreadCount();

    lastStartedThreads = newStartedThreads;
    lastLiveThreads = newLiveThreads;

    return true;
  }
}
