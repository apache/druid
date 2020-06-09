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

package org.apache.druid.server.coordinator.duty;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.timeline.DataSegment;

import java.util.Iterator;
import java.util.List;

/**
 * Segments in the lists which are the elements of this iterator are sorted according to the natural segment order
 * (see {@link DataSegment#compareTo}).
 */
public interface CompactionSegmentIterator extends Iterator<List<DataSegment>>
{
  long UNKNOWN_TOTAL_REMAINING_SEGMENTS_SIZE = -1L;
  /**
   * Return a map of (dataSource, total size of remaining segments) for all dataSources.
   * This method should consider all segments except the segments returned by {@link #next()}.
   */
  Object2LongOpenHashMap<String> totalRemainingSegmentsSizeBytes();
}
