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

package org.apache.druid.segment.file;

import org.apache.druid.segment.loading.DirectoryBackedRangeReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link DirectoryBackedRangeReader} that tracks range-read activity across the partial-segment test suite. Records
 * total reads, header-only reads (offset == 0, which corresponds to V10 header preamble fetches), and the set of
 * filenames that have been read. Each call site reads only the metric(s) it cares about.
 */
public class CountingRangeReader extends DirectoryBackedRangeReader
{
  private final AtomicInteger readCount = new AtomicInteger(0);
  private final AtomicInteger headerReadCount = new AtomicInteger(0);
  private final AtomicLong readBytes = new AtomicLong(0);
  private final Set<String> readFilenames = ConcurrentHashMap.newKeySet();

  public CountingRangeReader(File directory)
  {
    super(directory);
  }

  public int getReadCount()
  {
    return readCount.get();
  }

  public int getHeaderReadCount()
  {
    return headerReadCount.get();
  }

  public long getReadBytes()
  {
    return readBytes.get();
  }

  public Set<String> getReadFilenames()
  {
    return Set.copyOf(readFilenames);
  }

  public void resetCount()
  {
    readCount.set(0);
    headerReadCount.set(0);
    readBytes.set(0);
    readFilenames.clear();
  }

  @Override
  public InputStream readRange(String filename, long offset, long length) throws IOException
  {
    readCount.incrementAndGet();
    if (offset == 0) {
      headerReadCount.incrementAndGet();
    }
    readBytes.addAndGet(length);
    readFilenames.add(filename);
    return super.readRange(filename, offset, length);
  }
}
