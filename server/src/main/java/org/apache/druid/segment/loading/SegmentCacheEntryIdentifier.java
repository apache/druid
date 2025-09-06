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

package org.apache.druid.segment.loading;

import org.apache.druid.timeline.SegmentId;

import java.util.Objects;

/**
 * Use a {@link SegmentId} as a {@link CacheEntryIdentifier}
 */
public final class SegmentCacheEntryIdentifier implements CacheEntryIdentifier
{
  private final SegmentId segmentId;

  public SegmentCacheEntryIdentifier(SegmentId segmentId)
  {
    this.segmentId = segmentId;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentCacheEntryIdentifier that = (SegmentCacheEntryIdentifier) o;
    return Objects.equals(segmentId, that.segmentId);
  }

  @Override
  public int hashCode()
  {
    return segmentId.hashCode();
  }

  @Override
  public String toString()
  {
    return segmentId.toString();
  }
}
