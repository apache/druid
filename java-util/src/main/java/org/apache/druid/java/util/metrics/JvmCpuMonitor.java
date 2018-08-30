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
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.hyperic.sigar.ProcCpu;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.util.Map;

public class JvmCpuMonitor extends FeedDefiningMonitor
{
  private static final Logger log = new Logger(JvmCpuMonitor.class);

  private final Sigar sigar = SigarUtil.getSigar();
  private final long currentProcessId = sigar.getPid();

  private final KeyedDiff diff = new KeyedDiff();

  private Map<String, String[]> dimensions;

  public JvmCpuMonitor()
  {
    this(ImmutableMap.of());
  }

  public JvmCpuMonitor(Map<String, String[]> dimensions)
  {
    this(dimensions, DEFAULT_METRICS_FEED);
  }

  public JvmCpuMonitor(Map<String, String[]> dimensions, String feed)
  {
    super(feed);
    Preconditions.checkNotNull(dimensions);
    this.dimensions = ImmutableMap.copyOf(dimensions);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    // process CPU
    try {
      ProcCpu procCpu = sigar.getProcCpu(currentProcessId);
      final ServiceMetricEvent.Builder builder = builder();
      MonitorUtils.addDimensionsToBuilder(builder, dimensions);
      // delta for total, sys, user
      Map<String, Long> procDiff = diff.to(
          "proc/cpu", ImmutableMap.of(
              "jvm/cpu/total", procCpu.getTotal(),
              "jvm/cpu/sys", procCpu.getSys(),
              "jvm/cpu/user", procCpu.getUser()
          )
      );
      if (procDiff != null) {
        for (Map.Entry<String, Long> entry : procDiff.entrySet()) {
          emitter.emit(builder.build(entry.getKey(), entry.getValue()));
        }
      }
      emitter.emit(builder.build("jvm/cpu/percent", procCpu.getPercent()));
    }
    catch (SigarException e) {
      log.error(e, "Failed to get ProcCpu");
    }
    return true;
  }
}
