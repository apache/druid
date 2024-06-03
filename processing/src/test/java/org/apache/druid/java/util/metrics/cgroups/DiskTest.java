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

package org.apache.druid.java.util.metrics.cgroups;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

public class DiskTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @Before
  public void setUp() throws Exception
  {
    cgroupDir = temporaryFolder.newFolder();
    procDir = temporaryFolder.newFolder();
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File blkioDir = new File(
        cgroupDir,
        "blkio/system.slice/some.service"
    );

    FileUtils.mkdirp(blkioDir);
    TestUtils.copyResource("/blkio.throttle.io_serviced", new File(blkioDir, "blkio.throttle.io_serviced"));
    TestUtils.copyResource("/blkio.throttle.io_service_bytes", new File(blkioDir, "blkio.throttle.io_service_bytes"));
  }

  @Test
  public void testWontCrash()
  {
    final Disk disk = new Disk((cgroup) -> {
      throw new RuntimeException("shouldContinue");
    });
    final Map<String, Disk.Metrics> stats = disk.snapshot();
    Assert.assertEquals(ImmutableMap.of(), stats);
  }

  @Test
  public void testSimpleSnapshot()
  {
    final Map<String, Disk.Metrics> stats = new Disk(discoverer).snapshot();
    Assert.assertEquals(ImmutableSet.of("259:0", "259:7"), stats.keySet());

    Assert.assertEquals(stats.get("259:0").getReadCount(), 98L);
    Assert.assertEquals(stats.get("259:0").getWriteCount(), 756L);
    Assert.assertEquals(stats.get("259:0").getReadBytes(), 55000L);
    Assert.assertEquals(stats.get("259:0").getWriteBytes(), 6208512L);

    Assert.assertEquals(stats.get("259:7").getReadCount(), 26L);
    Assert.assertEquals(stats.get("259:7").getWriteCount(), 0L);
    Assert.assertEquals(stats.get("259:7").getReadBytes(), 1773568L);
    Assert.assertEquals(stats.get("259:7").getWriteBytes(), 0L);
  }
}
