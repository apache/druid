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

import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class HeapMemoryDatasourceSegmentCacheTest
{
  private static final String WIKI = "wiki";
  private static final Interval FIRST_WEEK_OF_JAN = Intervals.of("2024-01-01/P1W");
  private static final Interval FIRST_DAY_OF_JAN
      = FIRST_WEEK_OF_JAN.withDurationAfterStart(Duration.standardDays(1));
  private static final DataSegmentPlus JAN_1_SEGMENT
      = createUsedSegment().startingAt(FIRST_DAY_OF_JAN.getStart()).withVersion("v1").asPlus();
  private static final DataSegmentPlus JAN_2_SEGMENT
      = createUsedSegment().startingAt(FIRST_DAY_OF_JAN.getEnd()).withVersion("v2").asPlus();
  private static final DataSegmentPlus JAN_3_SEGMENT
      = createUsedSegment().startingAt(FIRST_DAY_OF_JAN.getEnd().plusDays(1)).withVersion("v3").asPlus();

  private HeapMemoryDatasourceSegmentCache cache;

  @Before
  public void setup()
  {
    cache = new HeapMemoryDatasourceSegmentCache(WIKI);
  }

  @Test
  public void testEmptyCache()
  {
    Assert.assertNull(cache.findUsedSegment(SegmentId.dummy(WIKI)));
    Assert.assertTrue(cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()).isEmpty());
    Assert.assertTrue(cache.findPendingSegmentsOverlapping(Intervals.ETERNITY).isEmpty());
  }

  @Test
  public void testInsertSegments_withUsedSegment()
  {
    final DataSegmentPlus segmentPlus = createUsedSegment().asPlus();
    final DataSegment segment = segmentPlus.getDataSegment();

    cache.insertSegments(Set.of(segmentPlus));

    final SegmentId segmentId = segment.getId();
    final Interval interval = segmentId.getInterval();

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));
    Assert.assertEquals(List.of(segmentPlus), cache.findUsedSegments(Set.of(segmentId)));

    Assert.assertEquals(Set.of(segmentId), cache.findUsedSegmentIdsOverlapping(interval));
    Assert.assertEquals(Set.of(segmentId), cache.findUsedSegmentIdsOverlapping(Intervals.ETERNITY));

    Assert.assertEquals(Set.of(segment), cache.findUsedSegmentsOverlappingAnyOf(List.of()));
    Assert.assertEquals(Set.of(segment), cache.findUsedSegmentsOverlappingAnyOf(List.of(interval)));
    Assert.assertEquals(Set.of(segment), cache.findUsedSegmentsOverlappingAnyOf(List.of(Intervals.ETERNITY)));

    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(interval)));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(Intervals.ETERNITY)));
  }

  @Test
  public void testInsertSegments_updatesCacheWithNewerEntry()
  {
    final DateTime now = DateTimes.nowUtc();
    final DataSegmentPlus segmentPlus = createUsedSegment().lastUpdatedOn(now).asPlus();

    Assert.assertEquals(1, cache.insertSegments(Set.of(segmentPlus)));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));

    // Verify that a segment with older updated time does not update cache
    final DataSegmentPlus oldSegmentPlus = updateSegment(segmentPlus, now.minus(1));
    Assert.assertEquals(0, cache.insertSegments(Set.of(oldSegmentPlus)));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));

    // Verify that a segment with newer updated time updates the cache
    final DataSegmentPlus newSegmentPlus = updateSegment(segmentPlus, now.plus(1));
    Assert.assertEquals(1, cache.insertSegments(Set.of(newSegmentPlus)));
    Assert.assertEquals(Set.of(newSegmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));
  }

  @Test
  public void testInsertSegments_withUnusedSegment()
  {
    final DataSegmentPlus segmentPlus = createUnusedSegment().asPlus();
    final DataSegment segment = segmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.insertSegments(Set.of(segmentPlus));

    // Verify that the segment is not returned in any of the used segment methods
    Assert.assertNull(cache.findUsedSegment(segmentId));
    Assert.assertTrue(cache.findUsedSegments(Set.of(segmentId)).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentIdsOverlapping(segment.getInterval()).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentsOverlappingAnyOf(List.of()).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()).isEmpty());

    final CacheStats cacheStats = cache.markCacheSynced();
    Assert.assertEquals(0, cacheStats.getNumUsedSegments());
    Assert.assertEquals(1, cacheStats.getNumUnusedSegments());
  }

  @Test
  public void testInsertSegments_addsToCache_ifUpdatedTimeIsNull()
  {
    final DataSegmentPlus usedSegmentPlus = createUsedSegment().lastUpdatedOn(null).asPlus();
    Assert.assertEquals(1, cache.insertSegments(Set.of(usedSegmentPlus)));
    Assert.assertEquals(Set.of(usedSegmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));
  }

  @Test
  public void testInsertSegments_doesNotUpdateCache_ifNewUpdatedTimeIsNull()
  {
    final DataSegmentPlus usedSegment = createUsedSegment().lastUpdatedOn(null).asPlus();
    Assert.assertEquals(1, cache.insertSegments(Set.of(usedSegment)));
    Assert.assertEquals(0, cache.insertSegments(Set.of(updateSegment(usedSegment, null))));
  }

  @Test
  public void testInsertSegments_canMarkItAsUnused()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().asPlus();
    final DataSegment segment = unusedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.insertSegments(Set.of(unusedSegmentPlus));

    Assert.assertNull(cache.findUsedSegment(segmentId));

    final DataSegmentPlus usedSegmentPlus = new DataSegmentPlus(
        segment,
        null,
        DateTimes.EPOCH,
        true,
        null,
        null,
        null
    );

    cache.insertSegments(Set.of(usedSegmentPlus));

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));
  }

  @Test
  public void testSyncSegmentIds_identifiesExpiredUsedSegmentIds()
  {
    final DataSegmentPlus usedSegmentPlus = createUsedSegment().updatedNow().asPlus();
    final SegmentRecord usedRecord = asRecord(usedSegmentPlus);

    final DateTime now = DateTimes.nowUtc();
    SegmentSyncResult result = cache.syncSegmentIds(List.of(usedRecord), now);
    Assert.assertEquals(Set.of(usedRecord.getSegmentId()), result.getExpiredIds());
  }

  @Test
  public void testSyncSegmentIds_ignoresUnusedSegment()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().updatedNow().asPlus();
    final SegmentRecord unusedRecord = asRecord(unusedSegmentPlus);

    final DateTime now = DateTimes.nowUtc();
    SegmentSyncResult result = cache.syncSegmentIds(List.of(unusedRecord), now);
    Assert.assertEquals(0, result.getUpdated());
    Assert.assertEquals(0, result.getDeleted());
    Assert.assertTrue(result.getExpiredIds().isEmpty());
  }

  @Test
  public void testInsertPendingSegment()
  {
    final PendingSegmentRecord pendingSegment = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(0, 1)),
        "sequenceName",
        null,
        null,
        "allocatorId"
    );
    Assert.assertTrue(cache.insertPendingSegment(pendingSegment, false));

    Assert.assertEquals(List.of(pendingSegment), cache.findPendingSegments("allocatorId"));
    Assert.assertEquals(
        List.of(pendingSegment.getId()),
        cache.findPendingSegmentIds("sequenceName", "")
    );
    Assert.assertEquals(
        List.of(pendingSegment),
        cache.findPendingSegmentsOverlapping(FIRST_WEEK_OF_JAN.withDurationAfterStart(Duration.standardHours(1)))
    );
    Assert.assertEquals(
        List.of(pendingSegment),
        cache.findPendingSegmentsWithExactInterval(FIRST_WEEK_OF_JAN)
    );
    Assert.assertEquals(
        List.of(pendingSegment.getId()),
        cache.findPendingSegmentIdsWithExactInterval("sequenceName", FIRST_WEEK_OF_JAN)
    );
  }

  @Test
  public void testInsertPendingSegment_doesNotUpdateEntry()
  {
    final DateTime now = DateTimes.nowUtc();
    final PendingSegmentRecord pendingSegment = new PendingSegmentRecord(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(0, 1)),
        "sequenceName",
        null,
        null,
        "allocatorId",
        now
    );
    Assert.assertTrue(cache.insertPendingSegment(pendingSegment, false));
    Assert.assertEquals(
        List.of(pendingSegment),
        cache.findPendingSegments(pendingSegment.getTaskAllocatorId())
    );

    // Verify that the pending segment does not get updated even with a newer created date
    final PendingSegmentRecord updatedPendingSegment = new PendingSegmentRecord(
        pendingSegment.getId(),
        pendingSegment.getSequenceName(),
        pendingSegment.getSequencePrevId(),
        pendingSegment.getUpgradedFromSegmentId(),
        pendingSegment.getTaskAllocatorId(),
        now.plusDays(1)
    );
    Assert.assertFalse(cache.insertPendingSegment(updatedPendingSegment, false));
    Assert.assertEquals(
        List.of(pendingSegment),
        cache.findPendingSegments(pendingSegment.getTaskAllocatorId())
    );
  }

  @Test
  public void testInsertPendingSegments()
  {
    final PendingSegmentRecord segment1 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(0, 2)),
        "sequenceName",
        null,
        null,
        "group1"
    );
    final PendingSegmentRecord segment2 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(1, 2)),
        "sequenceName",
        null,
        null,
        "group1"
    );
    final PendingSegmentRecord segment3 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v2", new NumberedShardSpec(0, 1)),
        "sequenceName",
        null,
        null,
        "group2"
    );

    Assert.assertEquals(
        3,
        cache.insertPendingSegments(List.of(segment1, segment2, segment3), false)
    );
  }

  @Test
  public void testInsertPendingSegment_isIdempotent()
  {
    final PendingSegmentRecord pendingSegment = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, Intervals.ETERNITY, "v1", new NumberedShardSpec(0, 1)),
        "s1",
        null,
        null,
        null
    );
    Assert.assertTrue(cache.insertPendingSegment(pendingSegment, true));
    Assert.assertFalse(cache.insertPendingSegment(pendingSegment, true));
    Assert.assertEquals(List.of(pendingSegment), cache.findPendingSegmentsOverlapping(Intervals.ETERNITY));
  }

  @Test
  public void testSyncSegmentIds_removesUsedSegmentUpdatedBeforeSyncStart()
  {
    final DateTime syncTime = DateTimes.nowUtc();

    final DataSegmentPlus persistedSegment = createUsedSegment().asPlus();
    final DataSegmentPlus unpersistedSegmentUpdatedBeforeSync =
        createUsedSegment().lastUpdatedOn(syncTime.minus(1)).asPlus();
    final DataSegmentPlus unpersistedSegmentUpdatedAfterSync =
        createUsedSegment().lastUpdatedOn(syncTime.plus(1)).asPlus();

    // Add segments to the cache and verify that they have been added
    final Set<DataSegmentPlus> allSegments = Set.of(
        persistedSegment,
        unpersistedSegmentUpdatedBeforeSync,
        unpersistedSegmentUpdatedAfterSync
    );
    cache.insertSegments(allSegments);
    Assert.assertEquals(
        allSegments,
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
    );

    // Remove unpersisted segments and verify that only unpersisted segments
    // last updated before the sync time are removed
    cache.syncSegmentIds(
        List.of(asRecord(persistedSegment)),
        syncTime
    );
    Assert.assertEquals(
        Set.of(persistedSegment, unpersistedSegmentUpdatedAfterSync),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
    );
  }

  @Test
  public void testSyncSegmentIds_removesUnusedSegmentUpdatedBeforeSyncStart()
  {
    final DateTime syncTime = DateTimes.nowUtc();

    final DataSegmentPlus usedSegment = createUsedSegment().asPlus();
    final DataSegmentPlus unusedSegmentUpdatedBeforeSync =
        createUnusedSegment().lastUpdatedOn(syncTime.minus(1)).asPlus();
    final DataSegmentPlus unusedSegmentUpdatedAfterSync =
        createUnusedSegment().lastUpdatedOn(syncTime.plus(1)).asPlus();

    // Add unused segments to the cache and verify that they have been added
    cache.insertSegments(
        Set.of(
            usedSegment,
            unusedSegmentUpdatedBeforeSync,
            unusedSegmentUpdatedAfterSync
        )
    );

    CacheStats cacheStats = cache.markCacheSynced();
    Assert.assertEquals(1, cacheStats.getNumUsedSegments());
    Assert.assertEquals(2, cacheStats.getNumUnusedSegments());

    // Remove unpersisted segments and verify that only unpersisted segments
    // last updated before the sync time are removed
    cache.syncSegmentIds(
        List.of(asRecord(usedSegment)),
        syncTime
    );

    cacheStats = cache.markCacheSynced();
    Assert.assertEquals(1, cacheStats.getNumUsedSegments());
    Assert.assertEquals(1, cacheStats.getNumUnusedSegments());
  }

  @Test
  public void testSyncPendingSegments_removesPendingSegmentCreatedbeforeSyncStart()
  {
    final DateTime syncTime = DateTimes.nowUtc();
    final String taskAllocator = "allocator1";
    final PendingSegmentRecord persistedSegment = new PendingSegmentRecord(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(0, 2)),
        "sequenceName",
        null,
        null,
        taskAllocator,
        null
    );
    final PendingSegmentRecord unpersistedSegmentCreatedBeforeSync = new PendingSegmentRecord(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(1, 2)),
        "sequenceName",
        null,
        null,
        taskAllocator,
        syncTime.minus(1)
    );
    final PendingSegmentRecord unpersistedSegmentCreatedAfterSync = new PendingSegmentRecord(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v2", new NumberedShardSpec(0, 1)),
        "sequenceName",
        null,
        null,
        taskAllocator,
        syncTime.plus(1)
    );

    // Add pending segments to the cache and verify that they have been added
    final List<PendingSegmentRecord> allSegments = List.of(
        persistedSegment,
        unpersistedSegmentCreatedBeforeSync,
        unpersistedSegmentCreatedAfterSync
    );
    cache.insertPendingSegments(allSegments, false);
    Assert.assertEquals(
        Set.copyOf(allSegments),
        Set.copyOf(cache.findPendingSegments(taskAllocator))
    );

    // Remove unpersisted segments and verify that only segments which are not
    // present in the metadata store and were created before the sync are removed
    cache.syncPendingSegments(
        List.of(persistedSegment),
        syncTime
    );
    Assert.assertEquals(
        Set.of(persistedSegment, unpersistedSegmentCreatedAfterSync),
        Set.copyOf(cache.findPendingSegments(taskAllocator))
    );
  }

  @Test
  public void testDeleteSegments()
  {
    final DataSegmentPlus usedSegmentPlus = createUsedSegment().asPlus();
    final SegmentId usedSegmentId = usedSegmentPlus.getDataSegment().getId();

    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().withVersion("v1").asPlus();
    final SegmentId unusedSegmentId = unusedSegmentPlus.getDataSegment().getId();

    cache.insertSegments(Set.of(usedSegmentPlus, unusedSegmentPlus));
    Assert.assertEquals(
        List.of(usedSegmentPlus),
        cache.findUsedSegments(Set.of(usedSegmentId))
    );

    CacheStats cacheStats = cache.markCacheSynced();
    Assert.assertEquals(1, cacheStats.getNumUsedSegments());
    Assert.assertEquals(1, cacheStats.getNumUnusedSegments());
    Assert.assertFalse(cache.isEmpty());

    Assert.assertEquals(
        2,
        cache.deleteSegments(Set.of(usedSegmentId, unusedSegmentId))
    );

    cacheStats = cache.markCacheSynced();
    Assert.assertEquals(0, cacheStats.getNumUsedSegments());
    Assert.assertEquals(0, cacheStats.getNumUnusedSegments());
    Assert.assertTrue(cache.isEmpty());
  }

  @Test
  public void testDeleteSegments_forEmptyOrAbsentIdsReturnsZero()
  {
    Assert.assertEquals(0, cache.deleteSegments(Set.of()));
    Assert.assertEquals(0, cache.deleteSegments(Set.of(SegmentId.dummy(WIKI))));
  }

  @Test
  public void testDeletePendingSegments_byTaskAllocatorId()
  {
    final PendingSegmentRecord group1PendingSegment1 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(0, 2)),
        "sequenceName",
        null,
        null,
        "group1"
    );
    final PendingSegmentRecord group1PendingSegment2 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(1, 2)),
        "sequenceName",
        null,
        null,
        "group1"
    );
    final PendingSegmentRecord group2PendingSegment1 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v2", new NumberedShardSpec(0, 1)),
        "sequenceName",
        null,
        null,
        "group2"
    );

    cache.insertPendingSegments(
        List.of(group1PendingSegment1, group1PendingSegment2, group2PendingSegment1),
        false
    );

    // Delete the segments for group1 and verify contents
    Assert.assertEquals(2, cache.deletePendingSegments("group1"));
    Assert.assertTrue(cache.findPendingSegments("group1").isEmpty());
    Assert.assertEquals(List.of(group2PendingSegment1), cache.findPendingSegments("group2"));
  }

  @Test
  public void testDeletePendingSegments_bySegmentIds()
  {
    final PendingSegmentRecord segment1 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(0, 2)),
        "sequenceName",
        null,
        null,
        "group1"
    );
    final PendingSegmentRecord segment2 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(1, 2)),
        "sequenceName",
        null,
        null,
        "group1"
    );
    final PendingSegmentRecord segment3 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v2", new NumberedShardSpec(0, 1)),
        "sequenceName",
        null,
        null,
        "group1"
    );

    cache.insertPendingSegments(List.of(segment1, segment2, segment3), false);

    Assert.assertEquals(
        2,
        cache.deletePendingSegments(
            Set.of(segment1.getId().toString(), segment2.getId().toString())
        )
    );
    Assert.assertEquals(List.of(segment3), cache.findPendingSegments("group1"));
  }

  @Test
  public void testDeleteAllPendingSegments()
  {
    final PendingSegmentRecord segment1 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v1", new NumberedShardSpec(0, 1)),
        "sequence1",
        null,
        null,
        "group1"
    );
    final PendingSegmentRecord segment2 = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, FIRST_WEEK_OF_JAN, "v2", new NumberedShardSpec(0, 1)),
        "sequence2",
        null,
        null,
        "group2"
    );

    cache.insertPendingSegments(List.of(segment1, segment2), true);

    Assert.assertEquals(2, cache.deleteAllPendingSegments());
    Assert.assertTrue(cache.findPendingSegmentsOverlapping(FIRST_WEEK_OF_JAN).isEmpty());
  }

  @Test
  public void testDeletePendingSegmentsCreatedIn()
  {
    final Interval firstWeekOfJan = Intervals.of("2024-01-01/P1W");

    final PendingSegmentRecord segment1 = new PendingSegmentRecord(
        new SegmentIdWithShardSpec(WIKI, firstWeekOfJan, "v1", new NumberedShardSpec(0, 2)),
        "sequenceName",
        null,
        null,
        "group1",
        firstWeekOfJan.getStart().plusDays(2)
    );
    final PendingSegmentRecord segment2 = new PendingSegmentRecord(
        new SegmentIdWithShardSpec(WIKI, firstWeekOfJan, "v1", new NumberedShardSpec(1, 2)),
        "sequenceName",
        null,
        null,
        "group1",
        firstWeekOfJan.getEnd().plusDays(10)
    );
    final PendingSegmentRecord segment3 = new PendingSegmentRecord(
        new SegmentIdWithShardSpec(WIKI, firstWeekOfJan, "v2", new NumberedShardSpec(0, 1)),
        "sequenceName",
        null,
        null,
        "group1",
        firstWeekOfJan.getStart()
    );

    cache.insertPendingSegments(List.of(segment1, segment2, segment3), false);


    Assert.assertEquals(2, cache.deletePendingSegmentsCreatedIn(firstWeekOfJan));
    Assert.assertEquals(
        List.of(segment2),
        cache.findPendingSegmentsOverlapping(firstWeekOfJan)
    );
  }

  @Test
  public void testMarkSegmentsWithinIntervalAsUnused()
  {
    cache.insertSegments(Set.of(JAN_1_SEGMENT, JAN_2_SEGMENT, JAN_3_SEGMENT));
    Assert.assertEquals(
        Set.of(JAN_1_SEGMENT, JAN_2_SEGMENT, JAN_3_SEGMENT),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(FIRST_WEEK_OF_JAN))
    );

    // Mark segments as unused by interval
    Assert.assertEquals(
        1,
        cache.markSegmentsWithinIntervalAsUnused(
            FIRST_DAY_OF_JAN.withDurationAfterStart(Duration.standardDays(1)),
            null,
            DateTimes.nowUtc()
        )
    );

    // Mark segment as unused by version
    Assert.assertEquals(
        1,
        cache.markSegmentsWithinIntervalAsUnused(
            Intervals.ETERNITY,
            List.of(JAN_2_SEGMENT.getDataSegment().getVersion()),
            DateTimes.nowUtc()
        )
    );

    // Verify that all the segment IDs are still present in cache but 2 have
    // been marked as unused
    Assert.assertEquals(
        Set.of(JAN_3_SEGMENT),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(FIRST_WEEK_OF_JAN))
    );

    final CacheStats cacheStats = cache.markCacheSynced();
    Assert.assertEquals(1, cacheStats.getNumUsedSegments());
    Assert.assertEquals(2, cacheStats.getNumUnusedSegments());
    Assert.assertEquals(3, cacheStats.getNumIntervals());
  }

  @Test
  public void testMarkSegmentAsUnused()
  {
    cache.insertSegments(Set.of(JAN_1_SEGMENT, JAN_2_SEGMENT, JAN_3_SEGMENT));

    cache.markSegmentAsUnused(JAN_1_SEGMENT.getDataSegment().getId(), DateTimes.nowUtc());
    Assert.assertEquals(
        Set.of(JAN_2_SEGMENT, JAN_3_SEGMENT),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(FIRST_WEEK_OF_JAN))
    );
  }

  @Test
  public void testMarkSegmentsAsUnused()
  {
    cache.insertSegments(Set.of(JAN_1_SEGMENT, JAN_2_SEGMENT, JAN_3_SEGMENT));

    cache.markSegmentsAsUnused(
        Set.of(JAN_1_SEGMENT.getDataSegment().getId(), JAN_2_SEGMENT.getDataSegment().getId()),
        DateTimes.nowUtc()
    );
    Assert.assertEquals(
        Set.of(JAN_3_SEGMENT),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(FIRST_WEEK_OF_JAN))
    );
  }

  @Test
  public void testMarkAllSegmentsAsUnused()
  {
    cache.insertSegments(Set.of(JAN_1_SEGMENT, JAN_2_SEGMENT, JAN_3_SEGMENT));
    cache.markAllSegmentsAsUnused(DateTimes.nowUtc());
    Assert.assertTrue(
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(FIRST_WEEK_OF_JAN))
             .isEmpty()
    );
  }

  @Test
  public void testStop_disablesFurtherActions()
  {
    cache.stop();

    DruidExceptionMatcher.internalServerError().expectMessageIs(
        "Cannot perform operation on cache as it is already stopped"
    ).assertThrowsAndMatches(
        () -> cache.deleteAllPendingSegments()
    );

    DruidExceptionMatcher.internalServerError().expectMessageIs(
        "Cannot perform operation on cache as it is already stopped"
    ).assertThrowsAndMatches(
        () -> cache.findPendingSegments("alloc1")
    );
  }

  private static CreateDataSegments createUsedSegment()
  {
    return CreateDataSegments.ofDatasource(WIKI).markUsed();
  }

  private static CreateDataSegments createUnusedSegment()
  {
    return CreateDataSegments.ofDatasource(WIKI).markUnused();
  }

  private static DataSegmentPlus updateSegment(DataSegmentPlus segment, DateTime newUpdatedTime)
  {
    return new DataSegmentPlus(
        segment.getDataSegment(),
        segment.getCreatedDate(),
        newUpdatedTime,
        segment.getUsed(),
        segment.getSchemaFingerprint(),
        segment.getNumRows(),
        segment.getUpgradedFromSegmentId()
    );
  }

  private static SegmentRecord asRecord(DataSegmentPlus segment)
  {
    return new SegmentRecord(
        segment.getDataSegment().getId(),
        Boolean.TRUE.equals(segment.getUsed()),
        segment.getUsedStatusLastUpdatedDate()
    );
  }
}
