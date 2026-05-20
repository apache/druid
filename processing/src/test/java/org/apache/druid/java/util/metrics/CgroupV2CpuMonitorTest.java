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

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.FileUtils;
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
import java.util.stream.Collectors;

public class CgroupV2CpuMonitorTest
{
  @TempDir
  Path tempDir;
  private File procDir;
  private File cgroupDir;
  private File statFile;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws IOException
  {
    cgroupDir = FileUtils.createTempDirInLocation(tempDir, "cgroupDir");
    procDir = FileUtils.createTempDirInLocation(tempDir, "procDir");
    discoverer = new ProcCgroupV2Discoverer(procDir.toPath());
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);

    statFile = new File(cgroupDir, "cpu.stat");
    TestUtils.copyOrReplaceResource("/cgroupv2/cpu.stat", statFile);
  }

  @Test
  public void testMonitor() throws IOException, InterruptedException
  {
    final CgroupV2CpuMonitor monitor = new CgroupV2CpuMonitor(discoverer);
    final StubServiceEmitter emitter = StubServiceEmitter.createStarted();
    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertEquals(2, emitter.getNumEmittedEvents());

    emitter.flush();

    TestUtils.copyOrReplaceResource("/cgroupv2/cpu.stat-2", statFile);
    // We need to pass atleast a second for the calculation to trigger
    // to avoid divide by zero.
    Thread.sleep(1000);

    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertTrue(
        emitter
            .getEvents()
            .stream()
            .map(e -> e.toMap().get("metric"))
            .collect(Collectors.toList())
            .containsAll(
                ImmutableSet.of(
                    CgroupUtil.CPU_TOTAL_USAGE_METRIC,
                    CgroupUtil.CPU_USER_USAGE_METRIC,
                    CgroupUtil.CPU_SYS_USAGE_METRIC
                )));
  }
}
