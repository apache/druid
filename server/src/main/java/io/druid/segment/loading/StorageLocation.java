/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.loading;

import com.google.common.collect.Sets;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.util.Set;

/**
*/
class StorageLocation
{
  private final File path;
  private final long maxSize;
  private final Set<DataSegment> segments;

  private volatile long currSize = 0;

  StorageLocation(File path, long maxSize)
  {
    this.path = path;
    this.maxSize = maxSize;

    this.segments = Sets.newHashSet();
  }

  File getPath()
  {
    return path;
  }

  long getMaxSize()
  {
    return maxSize;
  }

  synchronized void addSegment(DataSegment segment)
  {
    if (segments.add(segment)) {
      currSize += segment.getSize();
    }
  }

  synchronized void removeSegment(DataSegment segment)
  {
    if (segments.remove(segment)) {
      currSize -= segment.getSize();
    }
  }

  boolean canHandle(long size)
  {
    return available() >= size;
  }

  synchronized long available()
  {
    return maxSize - currSize;
  }

  StorageLocation mostEmpty(StorageLocation other)
  {
    return available() > other.available() ? this : other;
  }
}
