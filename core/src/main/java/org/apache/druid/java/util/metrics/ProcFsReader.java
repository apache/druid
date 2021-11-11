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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * Fetches data from top-level procfs files for metrics.
 */
public class ProcFsReader
{
  private final File procDir;

  private static final Logger LOG = new Logger(ProcFsReader.class);
  public static final Path DEFAULT_PROC_FS_ROOT = Paths.get("/proc/");
  private static final String BOOT_ID_PATH = "sys/kernel/random/boot_id";
  private static final String CPUINFO_PATH = "cpuinfo";

  public ProcFsReader(Path procFsRoot)
  {
    this.procDir = Preconditions.checkNotNull(procFsRoot, "procFsRoot").toFile();
    Preconditions.checkArgument(this.procDir.isDirectory(), "Not a directory: [%s]", procFsRoot);
  }

  /**
   * Reads the boot ID from the boot_id path, which is just a UUID.
   *
   * @return The boot UUID.
   */
  public UUID getBootId()
  {
    Path path = Paths.get(this.procDir.toString(), BOOT_ID_PATH);
    try {
      List<String> lines = Files.readAllLines(path);
      return lines.stream().map(UUID::fromString).findFirst().orElse(new UUID(0, 0));
    }
    catch (IOException ex) {
      LOG.error(ex, "Unable to read %s", path);
      return new UUID(0, 0);
    }
  }

  /**
   * Reads the cpuinfo path (example in src/test/resources/cpuinfo) and
   * counts the number of processors.
   *
   * @return the number of processors.
   */
  public long getProcessorCount()
  {
    Path path = Paths.get(this.procDir.toString(), CPUINFO_PATH);
    try {
      List<String> lines = Files.readAllLines(path);
      return lines.stream().filter(l -> l.startsWith("processor")).count();
    }
    catch (IOException ex) {
      LOG.error(ex, "Unable to read %s", path);
      return -1L;
    }
  }
}
