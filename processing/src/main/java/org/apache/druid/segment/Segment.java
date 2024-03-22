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

package org.apache.druid.segment;

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * The difference between this class and {@link org.apache.druid.timeline.DataSegment} is that {@link
 * org.apache.druid.timeline.DataSegment} contains the segment metadata only, while this class represents the actual
 * body of segment data, queryable.
 */
@PublicApi
public interface Segment extends Closeable
{
  SegmentId getId();
  Interval getDataInterval();
  @Nullable
  QueryableIndex asQueryableIndex();
  StorageAdapter asStorageAdapter();
  
  /**
   * Request an implementation of a particular interface.
   *
   * If the passed-in interface is {@link QueryableIndex} or {@link StorageAdapter}, then this method behaves
   * identically to {@link #asQueryableIndex()} or {@link #asStorageAdapter()}. Other interfaces are only
   * expected to be requested by callers that have specific knowledge of extra features provided by specific
   * segment types. For example, an extension might provide a custom Segment type that can offer both
   * StorageAdapter and some new interface. That extension can also offer a Query that uses that new interface.
   * 
   * Implementations which accept classes other than {@link QueryableIndex} or {@link StorageAdapter} are limited 
   * to using those classes within the extension. This means that one extension cannot rely on the `Segment.as` 
   * behavior of another extension.
   *
   * @param clazz desired interface
   * @param <T> desired interface
   * @return instance of clazz, or null if the interface is not supported by this segment
   */
  @SuppressWarnings({"unused", "unchecked"})
  @Nullable
  default <T> T as(@Nonnull Class<T> clazz)
  {
    if (clazz.equals(QueryableIndex.class)) {
      return (T) asQueryableIndex();
    } else if (clazz.equals(StorageAdapter.class)) {
      return (T) asStorageAdapter();
    }
    return null;
  }

  default String asString()
  {
    return getClass().toString();
  }
}
