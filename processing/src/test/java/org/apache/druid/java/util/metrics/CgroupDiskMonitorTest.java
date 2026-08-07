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
import org.apache.druid.java.util.metrics.cgroups.CgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.ProcCgroupDiscoverer;
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class CgroupDiskMonitorTest
{
  @TempDir
  Path tempDir;
  private File procDir;
  private File cgroupDir;
  private File servicedFile;
  private File serviceBytesFile;
  private CgroupDiscoverer discoverer;

  @BeforeEach
  public void setUp() throws IOException
  {
    cgroupDir = FileUtils.createTempDirInLocation(tempDir, "cgroupDir");
    procDir = FileUtils.createTempDirInLocation(tempDir, "procDir");
    discoverer = new ProcCgroupDiscoverer(procDir.toPath());
    TestUtils.setUpCgroups(procDir, cgroupDir);
    final File blkioDir = new File(
        cgroupDir,
        "blkio/system.slice/some.service/"
    );

    FileUtils.mkdirp(blkioDir);
    servicedFile = new File(blkioDir, "blkio.throttle.io_serviced");
    serviceBytesFile = new File(blkioDir, "blkio.throttle.io_service_bytes");
    TestUtils.copyResource("/blkio.throttle.io_service_bytes", serviceBytesFile);
    TestUtils.copyResource("/blkio.throttle.io_serviced", servicedFile);
  }

  @Test
  public void testMonitor() throws IOException
  {
    final CgroupDiskMonitor monitor = new CgroupDiskMonitor(discoverer, "some_feed");
    final StubServiceEmitter emitter = StubServiceEmitter.createStarted();
    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertEquals(0, emitter.getNumEmittedEvents());

    TestUtils.copyOrReplaceResource("/blkio.throttle.io_service_bytes-2", serviceBytesFile);
    TestUtils.copyOrReplaceResource("/blkio.throttle.io_serviced-2", servicedFile);

    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertEquals(8, emitter.getNumEmittedEvents());
    Assertions.assertTrue(
        emitter
            .getEvents()
            .stream()
            .map(e -> e.toMap().get("value"))
            .allMatch(val -> Long.valueOf(10).equals(val)));
  }
}
