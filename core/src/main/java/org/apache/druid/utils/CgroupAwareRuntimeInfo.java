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

package org.apache.druid.utils;

import org.apache.druid.java.util.metrics.ProcFsReader;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.Cpu;
import org.apache.druid.java.util.metrics.cgroups.CpuSet;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

/**
 * A container-aware utility class for finer details on CPU allocations.
 * <p>
 * NOTE: Since these values are essentially immutable after the process starts,
 * the constructor takes a snapshot of the values and stores it locally.
 */
public class CgroupAwareRuntimeInfo extends AbstractRuntimeInfo
{
  private static final String PROVIDER = "cgroupv1";
  private final Cpu.CpuAllocationMetric cpuAllocationMetric;
  private final CpuSet.CpuSetMetric cpuSetMetric;
  private final int processorCount;

  public CgroupAwareRuntimeInfo()
  {
    // If there's no proc dir, then there's no hope of finding /sys/fs either
    // so just emit default values.
    if (!ProcFsReader.DEFAULT_PROC_FS_ROOT.toFile().isDirectory()) {
      this.cpuAllocationMetric = Cpu.CpuAllocationMetric.DEFAULT;
      this.cpuSetMetric = CpuSet.CpuSetMetric.DEFAULT;
      this.processorCount = JvmUtils.getRuntimeInfo().getAvailableProcessors();
    } else {
      CgroupDiscoverer cgroupDiscoverer = new ProcSelfCgroupDiscoverer();
      this.cpuAllocationMetric = new Cpu(cgroupDiscoverer).snapshot();
      this.cpuSetMetric = new CpuSet(cgroupDiscoverer).snapshot();
      this.processorCount = Math.toIntExact(new ProcFsReader(ProcFsReader.DEFAULT_PROC_FS_ROOT).getProcessorCount());
    }
  }

  @Override
  public String getProvider()
  {
    return PROVIDER;
  }

  @Override
  public int getAvailableProcessors()
  {
    return this.processorCount;
  }

  @Override
  public long getCpuPeriod()
  {
    return this.cpuAllocationMetric.getPeriodUs();
  }

  @Override
  public long getCpuQuota()
  {
    return this.cpuAllocationMetric.getQuotaUs();
  }

  @Override
  public long getCpuShares()
  {
    return this.cpuAllocationMetric.getShares();
  }

  @Override
  public int[] getEffectiveCpuSetCpus()
  {
    return this.cpuSetMetric.getEffectiveCpuSetCpus();
  }
}
