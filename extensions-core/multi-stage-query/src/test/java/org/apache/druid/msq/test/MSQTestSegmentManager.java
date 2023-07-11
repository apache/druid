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

package org.apache.druid.msq.test;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Segment manager for tests to retrieve the generated segments in case of an insert query
 */
public class MSQTestSegmentManager
{
  private final ConcurrentMap<SegmentId, DataSegment> dataSegments = new ConcurrentHashMap<>();
  private final ConcurrentMap<SegmentId, Segment> segments = new ConcurrentHashMap<>();
  private final SegmentCacheManager segmentCacheManager;
  private final IndexIO indexIO;

  final Object lock = new Object();


  public MSQTestSegmentManager(SegmentCacheManager segmentCacheManager, IndexIO indexIO)
  {
    this.segmentCacheManager = segmentCacheManager;
    this.indexIO = indexIO;
  }

  public void addDataSegment(DataSegment dataSegment)
  {
    synchronized (lock) {
      dataSegments.put(dataSegment.getId(), dataSegment);

      try {
        segmentCacheManager.getSegmentFiles(dataSegment);
      }
      catch (SegmentLoadingException e) {
        throw new ISE(e, "Unable to load segment [%s]", dataSegment.getId());
      }
    }
  }

  public Collection<DataSegment> getAllDataSegments()
  {
    return dataSegments.values();
  }

  public void addSegment(Segment segment)
  {
    segments.put(segment.getId(), segment);
  }

  @Nullable
  public Segment getSegment(SegmentId segmentId)
  {
    return segments.get(segmentId);
  }

}
