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
import org.apache.druid.java.util.common.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Map;

public class DiskTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
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
    Assert.assertEquals(ImmutableMap.of(
        "259:0",
        new Disk.Metrics("259:0", 98L, 756L, 55000L, 6208512L),
        "259:7",
        new Disk.Metrics("259:7", 26L, 0L, 1773568L, 0L)
    ), stats);
  }
}
