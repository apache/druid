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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.metrics.CgroupUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class ProcCgroupDiscoverer implements CgroupDiscoverer
{
  private static final String CGROUP_TYPE = "cgroup";

  private final File procDir;

  /**
   * Create a proc discovery mechanism based on a `/proc` directory.
   *
   * @param procDir The directory under proc. This is usually `/proc/self` or `/proc/#pid`
   */
  public ProcCgroupDiscoverer(Path procDir)
  {
    this.procDir = Preconditions.checkNotNull(procDir, "procDir").toFile();
    Preconditions.checkArgument(this.procDir.isDirectory(), "Not a directory: [%s]", procDir);
  }

  @Override
  public Path discover(final String cgroup)
  {
    Preconditions.checkNotNull(cgroup, "cgroup required");
    final File procMounts = new File(procDir, "mounts");
    final File pidCgroups = new File(procDir, "cgroup");
    final PidCgroupEntry pidCgroupsEntry = getCgroupEntry(pidCgroups, cgroup);
    final ProcMountsEntry procMountsEntry = getMountEntry(procMounts, cgroup);
    final File cgroupDir = new File(
        procMountsEntry.path.toString(),
        pidCgroupsEntry.path.toString()
    );
    if (cgroupDir.exists() && cgroupDir.isDirectory()) {
      return cgroupDir.toPath();
    }
    throw new RE("Invalid cgroup directory [%s]", cgroupDir);
  }

  private PidCgroupEntry getCgroupEntry(final File procCgroup, final String cgroup)
  {
    final List<String> lines;
    try {
      lines = Files.readLines(procCgroup, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (final String line : lines) {
      if (line.startsWith("#")) {
        continue;
      }
      final PidCgroupEntry entry = PidCgroupEntry.parse(line);
      if (entry.controllers.contains(cgroup)) {
        return entry;
      }
    }
    throw new RE("Hierarchy for [%s] not found", cgroup);
  }

  private ProcMountsEntry getMountEntry(final File procMounts, final String cgroup)
  {
    final List<String> lines;
    try {
      lines = Files.readLines(procMounts, StandardCharsets.UTF_8);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    for (final String line : lines) {
      final ProcMountsEntry entry = ProcMountsEntry.parse(line);
      if (CGROUP_TYPE.equals(entry.type) && entry.options.contains(cgroup)) {
        return entry;
      }
    }
    throw new RE("Cgroup [%s] not found", cgroup);
  }

  /**
   * Doesn't use the last two mount entries for priority/boot stuff
   */
  static class ProcMountsEntry
  {
    // Example: cgroup /sys/fs/cgroup/cpu,cpuacct cgroup rw,nosuid,nodev,noexec,relatime,cpu,cpuacct 0 0
    static ProcMountsEntry parse(String entry)
    {
      final String[] splits = entry.split(CgroupUtil.SPACE_MATCH, 6);
      Preconditions.checkArgument(splits.length == 6, "Invalid entry: [%s]", entry);
      return new ProcMountsEntry(
          splits[0],
          Paths.get(splits[1]),
          splits[2],
          ImmutableSet.copyOf(splits[3].split(CgroupUtil.COMMA_MATCH))
      );
    }

    final String dev;
    final Path path;
    final String type;
    final Set<String> options;

    ProcMountsEntry(String dev, Path path, String type, Collection<String> options)
    {
      this.dev = dev;
      this.path = path;
      this.type = type;
      this.options = ImmutableSet.copyOf(options);
    }
  }

  // See man CGROUPS(7)
  static class PidCgroupEntry
  {
    static PidCgroupEntry parse(String entry)
    {
      // For example, entries with a port number will have an extra `:` in it somewhere, or ipv6 addresses.
      final String[] parts = entry.split(Pattern.quote(":"), 3);
      if (parts.length != 3) {
        throw new RE("Bad entry [%s]", entry);
      }
      final Set<String> controllers = new HashSet<>(Arrays.asList(parts[1].split(Pattern.quote(","))));
      final Path path = Paths.get(parts[2]);
      return new PidCgroupEntry(controllers, path);
    }

    final Set<String> controllers;
    final Path path;

    private PidCgroupEntry(Set<String> controllers, Path path)
    {
      this.controllers = controllers;
      this.path = path;
    }
  }
}
