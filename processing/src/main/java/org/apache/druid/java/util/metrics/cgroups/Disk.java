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

import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class Disk
{
  private static final Logger LOG = new Logger(Disk.class);
  private static final String CGROUP = "blkio";
  private static final String IO_SERVICED_FILE = "blkio.throttle.io_serviced";
  private static final String IO_SERVICE_BYTES_FILE = "blkio.throttle.io_service_bytes";
  private static final String READ = "Read";
  private static final String WRITE = "Write";
  private final CgroupDiscoverer cgroupDiscoverer;

  public Disk(CgroupDiscoverer cgroupDiscoverer)
  {
    this.cgroupDiscoverer = cgroupDiscoverer;
  }

  /**
   * Take a snapshot of cpu cgroup data
   *
   * @return A snapshot with the data populated.
   */
  public Map<String, Metrics> snapshot()
  {
    Map<String, Metrics> statsByDisk = new HashMap<>();

    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), IO_SERVICED_FILE))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        final String[] parts = line.split(Pattern.quote(" "));
        if (parts.length != 3) {
          // ignore
          continue;
        }
        Metrics metrics = statsByDisk.computeIfAbsent(parts[0], majorMinor -> new Metrics(majorMinor));
        switch (parts[1]) {
          case WRITE:
            metrics.writeCount = Longs.tryParse(parts[2]);
            break;
          case READ:
            metrics.readCount = Longs.tryParse(parts[2]);
            break;
        }
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.error(ex, "Unable to fetch disk snapshot");
    }

    try (final BufferedReader reader = Files.newBufferedReader(
        Paths.get(cgroupDiscoverer.discover(CGROUP).toString(), IO_SERVICE_BYTES_FILE))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        final String[] parts = line.split(Pattern.quote(" "));
        if (parts.length != 3) {
          // ignore
          continue;
        }
        Metrics metrics = statsByDisk.computeIfAbsent(parts[0], majorMinor -> new Metrics(majorMinor));
        switch (parts[1]) {
          case WRITE:
            metrics.writeBytes = Longs.tryParse(parts[2]);
            break;
          case READ:
            metrics.readBytes = Longs.tryParse(parts[2]);
            break;
        }
      }
    }
    catch (IOException | RuntimeException ex) {
      LOG.error(ex, "Unable to fetch memory snapshot");
    }

    return statsByDisk;
  }

  public static class Metrics
  {
    String diskName;
    long readCount;
    long writeCount;
    long readBytes;
    long writeBytes;

    public Metrics(String majorMinor)
    {
      try {
        File deviceFile = new File("/sys/dev/block/" + majorMinor);
        if (deviceFile.exists()) {
          diskName = deviceFile.getCanonicalPath();
        }
      }
      catch (IOException e) {
        LOG.warn("Unable to get disk name for " + majorMinor);
      }
      finally {
        if (diskName == null) {
          diskName = majorMinor;
        }
      }
    }

    public long getReadCount()
    {
      return readCount;
    }
    public long getWriteCount()
    {
      return writeCount;
    }

    public long getReadBytes()
    {
      return readBytes;
    }
    public long getWriteBytes()
    {
      return writeBytes;
    }
    public String getDiskName()
    {
      return diskName;
    }

    public void setReadCount(long readCount)
    {
      this.readCount = readCount;
    }

    public void setWriteCount(long writeCount)
    {
      this.writeCount = writeCount;
    }

    public void setReadBytes(long readBytes)
    {
      this.readBytes = readBytes;
    }

    public void setWriteBytes(long writeBytes)
    {
      this.writeBytes = writeBytes;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Metrics metrics = (Metrics) o;
      return readCount == metrics.readCount
             && writeCount == metrics.writeCount
             && readBytes == metrics.readBytes
             && writeBytes == metrics.writeBytes
             && Objects.equals(diskName, metrics.diskName);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(diskName, readCount, writeCount, readBytes, writeBytes);
    }
  }
}
