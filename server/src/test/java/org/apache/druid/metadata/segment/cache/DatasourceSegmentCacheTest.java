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

package org.apache.druid.metadata.segment.cache;

import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DatasourceSegmentCacheTest
{
  private static final String WIKI = "wiki";

  private DatasourceSegmentCache cache;

  @Before
  public void setup()
  {
    cache = new DatasourceSegmentCache(WIKI);
  }

  @Test
  public void testEmptyCache()
  {
    Assert.assertNull(cache.findUsedSegment("abc"));
    Assert.assertNull(cache.findHighestUnusedSegmentId(Intervals.ETERNITY, "v1"));

    Assert.assertTrue(cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()).isEmpty());
    Assert.assertTrue(cache.findPendingSegmentsOverlapping(Intervals.ETERNITY).isEmpty());
  }

  @Test
  public void testFindSegmentIsUnsupported()
  {
    verifyUnsupported(() -> cache.findSegment("abc"));
  }

  @Test
  public void testFindUnusedSegmentsIsUnsupported()
  {
    verifyUnsupported(() -> cache.findUnusedSegments(null, null, null, null));
  }

  @Test
  public void testFindSegmentsIsUnsupported()
  {
    verifyUnsupported(() -> cache.findSegments(Set.of()));
  }

  @Test
  public void testFindSegmentsWithSchemaIsUnsupported()
  {
    verifyUnsupported(() -> cache.findSegmentsWithSchema(Set.of()));
  }

  private void verifyUnsupported(ThrowingRunnable runnable)
  {
    DruidException exception = Assert.assertThrows(DruidException.class, runnable);
    DruidExceptionMatcher.defensive()
                         .expectMessageIs("Unsupported: Unused segments are not cached")
                         .matches(exception);
  }

  @Test
  public void testAddUsedSegment()
  {
    final DataSegmentPlus segmentPlus = createUsedSegment().ofSizeInMb(100);
    final DataSegment segment = segmentPlus.getDataSegment();

    cache.addSegment(segmentPlus);

    final SegmentId segmentId = segment.getId();
    final Interval interval = segmentId.getInterval();

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId.toString()));
    Assert.assertEquals(List.of(segment), cache.findUsedSegments(Set.of(segmentId.toString())));

    Assert.assertEquals(Set.of(segmentId.toString()), cache.findExistingSegmentIds(Set.of(segment)));

    Assert.assertEquals(Set.of(segmentId), cache.findUsedSegmentIdsOverlapping(interval));
    Assert.assertEquals(Set.of(segmentId), cache.findUsedSegmentIdsOverlapping(Intervals.ETERNITY));

    Assert.assertEquals(Set.of(segment), cache.findUsedSegmentsOverlappingAnyOf(List.of()));
    Assert.assertEquals(Set.of(segment), cache.findUsedSegmentsOverlappingAnyOf(List.of(interval)));
    Assert.assertEquals(Set.of(segment), cache.findUsedSegmentsOverlappingAnyOf(List.of(Intervals.ETERNITY)));

    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(interval)));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(Intervals.ETERNITY)));

    Assert.assertNull(cache.findHighestUnusedSegmentId(interval, segment.getVersion()));
  }

  @Test
  public void testUpdateUsedSegment()
  {
    final DataSegmentPlus segmentPlus = createUsedSegment().updatedNow().ofSizeInMb(100);
    final DataSegment segment = segmentPlus.getDataSegment();

    cache.addSegment(segmentPlus);
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));

    // Update the segment and verify that the fields have been updated
    final DataSegmentPlus updatedSegmentPlus
        = new DataSegmentPlus(segment, null, DateTimes.EPOCH, true, null, 100L, null);
    cache.addSegment(updatedSegmentPlus);
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));
  }

  @Test
  public void testAddUnusedSegment()
  {
    final DataSegmentPlus segmentPlus = createUnusedSegment().ofSizeInMb(100);
    final DataSegment segment = segmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(segmentPlus);

    // Verify that the segment is not returned in any of the used segment methods
    Assert.assertNull(cache.findUsedSegment(segmentId.toString()));
    Assert.assertTrue(cache.findUsedSegments(Set.of(segmentId.toString())).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentIdsOverlapping(segment.getInterval()).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentsOverlappingAnyOf(List.of()).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()).isEmpty());

    Assert.assertEquals(Set.of(segmentId.toString()), cache.findExistingSegmentIds(Set.of(segment)));

    // Verify unused segment methods
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
  }

  @Test
  public void testShouldRefreshUsedSegment()
  {
    final DataSegmentPlus segmentPlus = createUsedSegment().updatedNow().ofSizeInMb(100);
    final DataSegment segment = segmentPlus.getDataSegment();
    final String segmentId = segment.getId().toString();

    Assert.assertTrue(cache.shouldRefreshUsedSegment(segmentId, segmentPlus.getUsedStatusLastUpdatedDate()));

    cache.addSegment(segmentPlus);
    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));

    // Verify that segment refresh is required only if updated time has increased
    final DateTime updatedTime = segmentPlus.getUsedStatusLastUpdatedDate();
    Assert.assertNotNull(updatedTime);

    Assert.assertFalse(cache.shouldRefreshUsedSegment(segmentId, updatedTime));
    Assert.assertFalse(cache.shouldRefreshUsedSegment(segmentId, updatedTime.minus(1)));
    Assert.assertTrue(cache.shouldRefreshUsedSegment(segmentId, updatedTime.plus(1)));
  }

  @Test
  public void testUpdateUsedSegmentToUnused()
  {
    final DataSegmentPlus usedSegmentPlus = createUsedSegment().ofSizeInMb(100);
    final DataSegment segment = usedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(usedSegmentPlus);

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId.toString()));
    Assert.assertNull(cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));

    final DataSegmentPlus unusedSegmentPlus = new DataSegmentPlus(
        segment,
        null,
        DateTimes.EPOCH,
        false,
        null,
        null,
        null
    );

    cache.addSegment(unusedSegmentPlus);

    Assert.assertNull(cache.findUsedSegment(segmentId.toString()));
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
  }

  @Test
  public void testUpdateUnusedSegmentToUsed()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().ofSizeInMb(100);
    final DataSegment segment = unusedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(unusedSegmentPlus);

    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
    Assert.assertNull(cache.findUsedSegment(segmentId.toString()));

    final DataSegmentPlus usedSegmentPlus = new DataSegmentPlus(
        segment,
        null,
        DateTimes.EPOCH,
        true,
        null,
        null,
        null
    );

    cache.addSegment(usedSegmentPlus);

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId.toString()));
  }

  @Test
  public void testOnlyResetUpdatesHighestId()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().ofSizeInMb(100);
    final DataSegment segment = unusedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(unusedSegmentPlus);
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));

    // Verify that marking the segment as used does not update the highest ID
    final DataSegmentPlus usedSegmentPlus = new DataSegmentPlus(
        segment,
        null,
        DateTimes.EPOCH,
        true,
        null,
        null,
        null
    );
    cache.addSegment(usedSegmentPlus);
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));

    // Verify that removing segment does not update the highest ID
    cache.removeSegmentIds(Set.of(segmentId.toString()));
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));

    // Verify that only reset updates the highest ID
    cache.resetMaxUnusedIds(Map.of());
    Assert.assertNull(cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
  }

  private static CreateDataSegments createUsedSegment()
  {
    return CreateDataSegments.ofDatasource(WIKI).markUsed();
  }

  private static CreateDataSegments createUnusedSegment()
  {
    return CreateDataSegments.ofDatasource(WIKI).markUnused();
  }
}
