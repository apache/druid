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
import org.apache.druid.java.util.metrics.cgroups.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ProcFsReaderTest
{
  @TempDir
  Path tempDir;
  private File procDir;

  @BeforeEach
  public void setUp() throws IOException
  {
    procDir = FileUtils.createTempDirInLocation(tempDir, "procDir");
    File kernelDir = new File(
        procDir,
        "sys/kernel/random"
    );

    FileUtils.mkdirp(kernelDir);
    TestUtils.copyResource("/cpuinfo", new File(procDir, "cpuinfo"));
    TestUtils.copyResource("/boot_id", new File(kernelDir, "boot_id"));
  }

  @Test
  public void testUtilThrowsOnBadDir()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new ProcFsReader(Paths.get(procDir + "_dummy"))
    );
  }

  @Test
  public void testBootId()
  {
    final ProcFsReader fetcher = new ProcFsReader(procDir.toPath());
    Assertions.assertEquals("ad1f0a5c-55ea-4a49-9db8-bbb0f22e2ba6", fetcher.getBootId().toString());
  }

  @Test
  public void testProcessorCount()
  {
    final ProcFsReader fetcher = new ProcFsReader(procDir.toPath());
    Assertions.assertEquals(8, fetcher.getProcessorCount());
  }
}
