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

import org.apache.druid.java.util.common.StringUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TestUtils
{
  public static void setUpCgroups(
      File procDir,
      File cgroupDir
  ) throws IOException
  {
    final File procMountsTemplate = new File(procDir, "mounts.template");
    final File procMounts = new File(procDir, "mounts");
    copyResource("/proc.mounts", procMountsTemplate);

    final String procMountsString = StringUtils.fromUtf8(Files.readAllBytes(procMountsTemplate.toPath()));
    Files.write(
        procMounts.toPath(),
        StringUtils.toUtf8(StringUtils.replace(procMountsString, "/sys/fs/cgroup", cgroupDir.getAbsolutePath()))
    );

    Assert.assertTrue(new File(
        cgroupDir,
        "cpu,cpuacct/system.slice/some.service/f12ba7e0-fa16-462e-bb9d-652ccc27f0ee"
    ).mkdirs());
    copyResource("/proc.pid.cgroup", new File(procDir, "cgroup"));
  }

  public static void copyResource(String resource, File out) throws IOException
  {
    Files.copy(TestUtils.class.getResourceAsStream(resource), out.toPath());
    Assert.assertTrue(out.exists());
    Assert.assertNotEquals(0, out.length());
  }
}
