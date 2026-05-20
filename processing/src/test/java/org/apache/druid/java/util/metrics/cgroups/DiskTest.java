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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;

public class DiskTest
{
  @TempDir
  Path tempDir;
  private File procDir;
  private File cgroupDir;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws Exception
  {
    cgroupDir = FileUtils.createTempDirInLocation(tempDir, "cgroupDir");
    procDir = FileUtils.createTempDirInLocation(tempDir, "procDir");
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
    final Disk disk = new Disk(TestUtils.exceptionThrowingDiscoverer());
    final Map<String, Disk.Metrics> stats = disk.snapshot();
    Assertions.assertEquals(ImmutableMap.of(), stats);
  }

  @Test
  public void testSimpleSnapshot()
  {
    final Map<String, Disk.Metrics> stats = new Disk(discoverer).snapshot();
    Assertions.assertEquals(ImmutableSet.of("259:0", "259:7"), stats.keySet());

    Assertions.assertEquals(stats.get("259:0").getReadCount(), 98L);
    Assertions.assertEquals(stats.get("259:0").getWriteCount(), 756L);
    Assertions.assertEquals(stats.get("259:0").getReadBytes(), 55000L);
    Assertions.assertEquals(stats.get("259:0").getWriteBytes(), 6208512L);

    Assertions.assertEquals(stats.get("259:7").getReadCount(), 26L);
    Assertions.assertEquals(stats.get("259:7").getWriteCount(), 0L);
    Assertions.assertEquals(stats.get("259:7").getReadBytes(), 1773568L);
    Assertions.assertEquals(stats.get("259:7").getWriteBytes(), 0L);
  }
}
