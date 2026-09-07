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

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class JvmThreadsMonitor extends FeedDefiningMonitor
{
  private int lastLiveThreads = 0;
  private long lastStartedThreads = 0;

  public JvmThreadsMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  public JvmThreadsMonitor(String feed)
  {
    super(feed);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    final ServiceMetricEvent.Builder builder = builder();

    // Because between next two calls on ThreadMXBean new threads can be started we can observe some inconsistency
    // in counters values and finished counter could be even negative
    int newLiveThreads = threadBean.getThreadCount();
    long newStartedThreads = threadBean.getTotalStartedThreadCount();

    long startedThreadsDiff = newStartedThreads - lastStartedThreads;

    emitter.emit(builder.setMetric("jvm/threads/started", startedThreadsDiff));
    emitter.emit(builder.setMetric("jvm/threads/finished", lastLiveThreads + startedThreadsDiff - newLiveThreads));
    emitter.emit(builder.setMetric("jvm/threads/live", newLiveThreads));
    emitter.emit(builder.setMetric("jvm/threads/liveDaemon", threadBean.getDaemonThreadCount()));

    emitter.emit(builder.setMetric("jvm/threads/livePeak", threadBean.getPeakThreadCount()));
    threadBean.resetPeakThreadCount();

    lastStartedThreads = newStartedThreads;
    lastLiveThreads = newLiveThreads;

    return true;
  }
}
