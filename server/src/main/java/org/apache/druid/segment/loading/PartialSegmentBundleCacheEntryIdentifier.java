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

import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.timeline.SegmentId;

/**
 * Identifier for a {@link PartialSegmentBundleCacheEntry}; a named group of containers within a partial-loaded V10
 * segment that gets mounted and evicted as a unit. The {@code bundleName} is the group name declared at write time
 * via {@link SegmentFileBuilder#startFileBundle}.
 * <p>
 * Each partial segment is split across multiple {@link CacheEntry}, with one {@link SegmentCacheEntryIdentifier}-keyed
 * metadata entry plus one of these per bundle.
 */
public record PartialSegmentBundleCacheEntryIdentifier(SegmentId segmentId, String bundleName)
    implements CacheEntryIdentifier
{
  @Override
  public String toString()
  {
    return segmentId + ":" + bundleName;
  }
}
