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

import com.google.common.io.Files;
import org.apache.druid.java.util.common.RE;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

public class ProcCgroupV2Discoverer extends ProcCgroupDiscoverer
{
  private static final String CGROUP_TYPE = "cgroup2";

  /**
   * Create a proc discovery mechanism based on a `/proc` directory.
   *
   * @param procDir The directory under proc. This is usually `/proc/self` or `/proc/#pid`
   */
  public ProcCgroupV2Discoverer(Path procDir)
  {
    super(procDir);
  }

  @Override
  public Path discover(String cgroup)
  {
    try {
      for (final String line : Files.readLines(new File(procDir, "mounts"), StandardCharsets.UTF_8)) {
        final ProcMountsEntry entry = ProcMountsEntry.parse(line);
        if (CGROUP_TYPE.equals(entry.type)) {
          return entry.path;
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    throw new RE("Cgroup location not found");
  }
}
