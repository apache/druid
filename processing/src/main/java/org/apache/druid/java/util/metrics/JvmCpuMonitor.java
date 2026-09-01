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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import oshi.SystemInfo;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import java.util.Map;

public class JvmCpuMonitor extends FeedDefiningMonitor
{
  private static final Logger log = new Logger(JvmCpuMonitor.class);

  private final OperatingSystem operatingSystem;
  private final int currentProcessId;
  private final KeyedDiff diff = new KeyedDiff();
  private OSProcess previousProcess;

  public JvmCpuMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  public JvmCpuMonitor(String feed)
  {
    this(feed, new SystemInfo().getOperatingSystem());
  }

  @VisibleForTesting
  JvmCpuMonitor(String feed, OperatingSystem operatingSystem)
  {
    super(feed);
    this.operatingSystem = operatingSystem;
    this.currentProcessId = operatingSystem.getProcessId();
    this.previousProcess = operatingSystem.getProcess(currentProcessId);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    final OSProcess currentProcess = operatingSystem.getProcess(currentProcessId);
    if (currentProcess == null) {
      log.error("Unable to get current process CPU metrics");
      return true;
    }

    final ServiceMetricEvent.Builder builder = builder();
    final long userTime = currentProcess.getUserTime();
    final long sysTime = currentProcess.getKernelTime();
    final Map<String, Long> procDiff = diff.to(
        "proc/cpu", ImmutableMap.of(
            "jvm/cpu/total", userTime + sysTime,
            "jvm/cpu/sys", sysTime,
            "jvm/cpu/user", userTime
        )
    );
    if (procDiff != null) {
      for (Map.Entry<String, Long> entry : procDiff.entrySet()) {
        emitter.emit(builder.setMetric(entry.getKey(), entry.getValue()));
      }
    }

    final double cpuLoad = currentProcess.getProcessCpuLoadBetweenTicks(previousProcess);
    if (cpuLoad >= 0) {
      emitter.emit(builder.setMetric("jvm/cpu/percent", cpuLoad * 100));
    }

    previousProcess = currentProcess;
    return true;
  }
}
