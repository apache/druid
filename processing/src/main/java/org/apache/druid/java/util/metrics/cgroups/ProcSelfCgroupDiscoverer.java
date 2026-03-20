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
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ProcSelfCgroupDiscoverer implements CgroupDiscoverer
{
  private static final Logger LOG = new Logger(ProcSelfCgroupDiscoverer.class);
  private final CgroupDiscoverer delegate;

  public ProcSelfCgroupDiscoverer()
  {
    this(ProcCgroupDiscoverer.class);
  }


  public static CgroupDiscoverer autoCgroupDiscoverer()
  {
    return autoCgroupDiscoverer(Paths.get("/proc/self"));
  }

  public ProcSelfCgroupDiscoverer(Class<? extends CgroupDiscoverer> discoverer)
  {
    try {
      delegate = discoverer.getDeclaredConstructor(Path.class).newInstance(Paths.get("/proc/self"));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Path discover(String cgroup)
  {
    return delegate.discover(cgroup);
  }

  @Override
  public Cpu.CpuMetrics getCpuMetrics()
  {
    return delegate.getCpuMetrics();
  }

  @Override
  public CpuSet.CpuSetMetric getCpuSetMetrics()
  {
    return delegate.getCpuSetMetrics();
  }

  @Override
  public CgroupVersion getCgroupVersion()
  {
    return delegate.getCgroupVersion();
  }

  /**
   * Creates the appropriate CgroupDiscoverer based on the cgroups version detected in the system.
   */
  public static CgroupDiscoverer autoCgroupDiscoverer(Path procPidDir)
  {
    CgroupVersion version = detectCgroupVersion(procPidDir);
    
    switch (version) {
      case V2:
        LOG.info("Detected cgroups v2, using ProcCgroupV2Discoverer");
        return new ProcCgroupV2Discoverer(procPidDir);
      
      case V1:
        LOG.info("Detected cgroups v1, using ProcCgroupDiscoverer");
        return new ProcCgroupDiscoverer(procPidDir);
        
      case UNKNOWN:
      default:
        LOG.warn("Could not detect cgroups version, falling back to cgroups v1 discoverer");
        return new ProcCgroupDiscoverer(procPidDir);
    }
  }

  /**
   * Detects the cgroups version by examining the /proc/mounts file.
   */
  private static CgroupVersion detectCgroupVersion(Path procPidDir)
  {
    File mountsFile = new File(procPidDir.toFile(), "mounts");
    
    if (!mountsFile.exists() || !mountsFile.canRead()) {
      LOG.warn("Cannot read mounts file at [%s], unable to detect cgroups version", mountsFile);
      return CgroupVersion.UNKNOWN;
    }

    try {
      boolean hasV1 = false;
      boolean hasV2 = false;
      
      for (String line : Files.readLines(mountsFile, StandardCharsets.UTF_8)) {
        String[] parts = line.split("\\s+");
        if (parts.length >= 3) {
          String fsType = parts[2];
          if ("cgroup".equals(fsType)) {
            hasV1 = true;
          } else if ("cgroup2".equals(fsType)) {
            hasV2 = true;
          }
        }
      }
      
      // Prefer v2 if both are present (hybrid mode)
      if (hasV2) {
        return CgroupVersion.V2;
      } else if (hasV1) {
        return CgroupVersion.V1;
      } else {
        return CgroupVersion.UNKNOWN;
      }
    }
    catch (IOException e) {
      LOG.warn(e, "Error reading mounts file [%s], unable to detect cgroups version", mountsFile);
      return CgroupVersion.UNKNOWN;
    }
  }

}
