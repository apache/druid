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

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupV2Discoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class CgroupV2MemoryMonitorTest
{
  @TempDir
  Path tempDir;
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws IOException
  {
    cgroupDir = FileUtils.createTempDirInLocation(tempDir, "cgroupDir");
    procDir = FileUtils.createTempDirInLocation(tempDir, "procDir");
    discoverer = new ProcCgroupV2Discoverer(procDir.toPath());
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);


    TestUtils.copyResource("/memory.stat", new File(cgroupDir, "memory.stat"));
    TestUtils.copyResource("/memory.numa_stat", new File(cgroupDir, "memory.numa_stat"));
    TestUtils.copyResource("/memory.usage_in_bytes", new File(cgroupDir, "memory.current"));
    TestUtils.copyResource("/memory.limit_in_bytes", new File(cgroupDir, "memory.max"));
  }

  @Test
  public void testMonitor()
  {
    final CgroupV2MemoryMonitor monitor = new CgroupV2MemoryMonitor(
        discoverer,
        FeedDefiningMonitor.DEFAULT_METRICS_FEED
    );
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assertions.assertTrue(monitor.doMonitor(emitter));
    final List<Event> actualEvents = emitter.getEvents();
    Assertions.assertEquals(46, actualEvents.size());
  }
}
