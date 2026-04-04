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

package org.apache.druid.test.utils;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Wrapper around {@link SegmentManager} using {@link TestSegmentCacheManager} that helps with adding in-memory
 * test segments.
 */
public class TestSegmentManager
{
  private final SegmentManager segmentManager;
  private final TestSegmentCacheManager cacheManager;
  private final ConcurrentMap<SegmentId, DataSegment> testGeneratedDataSegments = new ConcurrentHashMap<>();
  private final ConcurrentMap<SegmentId, DataSegment> addedSegments = new ConcurrentHashMap<>();

  public TestSegmentManager()
  {
    this.cacheManager = new TestSegmentCacheManager();
    this.segmentManager = new SegmentManager(cacheManager);
  }

  /**
   * Creates a minimal DataSegment for a test Segment.
   */
  public static DataSegment createDataSegmentForTest(final SegmentId segmentId)
  {
    return DataSegment.builder()
                      .dataSource(segmentId.getDataSource())
                      .interval(segmentId.getInterval())
                      .version(segmentId.getVersion())
                      .shardSpec(new NumberedShardSpec(segmentId.getPartitionNum(), 0))
                      .size(0)
                      .build();
  }

  /**
   * Returns the underlying {@link SegmentManager} for use in injection or direct usage.
   * The returned manager has its timeline properly populated with any segments added via
   * {@link #addSegment(DataSegment, Segment)}.
   */
  public SegmentManager getSegmentManager()
  {
    return segmentManager;
  }

  /**
   * Adds a segment to both the cache and the timeline. After calling this method, the segment
   * will be available via {@link SegmentManager#getTimeline(org.apache.druid.query.TableDataSource)},
   * {@link SegmentManager#acquireCachedSegment(DataSegment)}, and
   * {@link SegmentManager#acquireSegment(DataSegment)}.
   */
  public void addSegment(final DataSegment dataSegment, final Segment segment)
  {
    cacheManager.registerSegment(dataSegment, segment);
    addedSegments.put(dataSegment.getId(), dataSegment);

    // Call loadSegment to populate the timeline.
    try {
      segmentManager.loadSegment(dataSegment);
    }
    catch (SegmentLoadingException | IOException e) {
      throw new ISE(e, "Failed to load test segment[%s]", dataSegment.getId());
    }
  }

  /**
   * Returns the Segment for a given {@link SegmentId}, if it has been added.
   */
  @Nullable
  public Segment getSegment(final SegmentId segmentId)
  {
    DataSegment dataSegment = addedSegments.get(segmentId);
    if (dataSegment == null) {
      return null;
    }
    return segmentManager.acquireCachedSegment(dataSegment).orElse(null);
  }

  /**
   * Records a generated DataSegment (e.g., from an INSERT query) for later retrieval.
   * This is separate from {@link #addSegment} which adds segments that can be queried.
   */
  public void recordGeneratedSegment(final DataSegment dataSegment)
  {
    testGeneratedDataSegments.put(dataSegment.getId(), dataSegment);
  }

  /**
   * Returns all generated segments recorded via {@link #recordGeneratedSegment}.
   */
  public Collection<DataSegment> getGeneratedSegments()
  {
    return testGeneratedDataSegments.values();
  }
}
