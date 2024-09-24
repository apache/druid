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
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupV2Discoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class CgroupV2CpuMonitorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File procDir;
  private File cgroupDir;
  private File statFile;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws IOException
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupV2Discoverer(procDir.toPath());
    TestUtils.setUpCgroupsV2(procDir, cgroupDir);

    statFile = new File(cgroupDir, "cpu.stat");
    TestUtils.copyOrReplaceResource("/cgroupv2/cpu.stat", statFile);
  }

  @Test
  public void testMonitor() throws IOException, InterruptedException
  {
    final CgroupV2CpuMonitor monitor = new CgroupV2CpuMonitor(discoverer);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assert.assertTrue(monitor.doMonitor(emitter));
    final List<Event> actualEvents = emitter.getEvents();
    Assert.assertEquals(0, actualEvents.size());

    emitter.flush();

    TestUtils.copyOrReplaceResource("/cgroupv2/cpu.stat-2", statFile);
    // We need to pass atleast a second for the calculation to trigger
    // to avoid divide by zero.
    Thread.sleep(1000);

    Assert.assertTrue(monitor.doMonitor(emitter));
    Assert.assertTrue(
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
