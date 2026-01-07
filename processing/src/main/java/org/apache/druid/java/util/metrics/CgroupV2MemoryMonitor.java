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
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupV2Discoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcSelfCgroupDiscoverer;

/**
 * Monitor that reports memory usage stats by reading `memory.*` files reported by cgroupv2
 */
public class CgroupV2MemoryMonitor extends FeedDefiningMonitor
{

  private static final String MEMORY_USAGE_FILE = "memory.current";
  private static final String MEMORY_LIMIT_FILE = "memory.max";
  private final CgroupDiscoverer cgroupDiscoverer;

  @VisibleForTesting
  CgroupV2MemoryMonitor(CgroupDiscoverer cgroupDiscoverer, String feed)
  {
    super(feed);
    this.cgroupDiscoverer = cgroupDiscoverer;
  }


  public CgroupV2MemoryMonitor(String feed)
  {
    this(new ProcSelfCgroupDiscoverer(ProcCgroupV2Discoverer.class), feed);
  }

  public CgroupV2MemoryMonitor()
  {
    this(DEFAULT_METRICS_FEED);
  }

  @Override
  public boolean doMonitor(ServiceEmitter emitter)
  {
    return CgroupMemoryMonitor.parseAndEmit(
        emitter,
        cgroupDiscoverer,
        MEMORY_USAGE_FILE,
        MEMORY_LIMIT_FILE,
        this
    );
  }
}
