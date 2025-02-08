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
    Assert.assertNull(cache.findHighestUnusedSegmentId(Intervals.ETERNITY, "v1"));

    Assert.assertTrue(cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()).isEmpty());
    Assert.assertTrue(cache.findPendingSegmentsOverlapping(Intervals.ETERNITY).isEmpty());
  }

  @Test
  public void testFindSegment_throwsUnsupported()
  {
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Unsupported: Unused segments are not cached"
    ).assertThrowsAndMatches(
        () -> cache.findSegment(SegmentId.dummy(WIKI))
    );
  }

  @Test
  public void testFindUnusedSegments_throwsUnsupported()
  {
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Unsupported: Unused segments are not cached"
    ).assertThrowsAndMatches(
        () -> cache.findUnusedSegments(null, null, null, null)
    );
  }

  @Test
  public void testFindSegments_throwsUnsupported()
  {
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Unsupported: Unused segments are not cached"
    ).assertThrowsAndMatches(
        () -> cache.findSegments(Set.of())
    );
  }

  @Test
  public void testFindSegmentsWithSchema_throwsUnsupported()
  {
    DruidExceptionMatcher.defensive().expectMessageIs(
        "Unsupported: Unused segments are not cached"
    ).assertThrowsAndMatches(
        () -> cache.findSegmentsWithSchema(Set.of())
    );
  }

  @Test
  public void testAddSegment_withUsedSegment()
  {
    final DataSegmentPlus segmentPlus = createUsedSegment().asPlus();
    final DataSegment segment = segmentPlus.getDataSegment();

    cache.addSegment(segmentPlus);

    final SegmentId segmentId = segment.getId();
    final Interval interval = segmentId.getInterval();

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));
    Assert.assertEquals(List.of(segment), cache.findUsedSegments(Set.of(segmentId)));

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
  public void testAddSegment_updatesCacheWithNewerEntry()
  {
    final DateTime now = DateTimes.nowUtc();
    final DataSegmentPlus segmentPlus = createUsedSegment().lastUpdatedOn(now).asPlus();
    final DataSegment segment = segmentPlus.getDataSegment();

    Assert.assertTrue(cache.addSegment(segmentPlus));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));

    // Verify that a segment with older updated time does not update cache
    final DataSegmentPlus oldSegmentPlus
        = new DataSegmentPlus(segment, null, now.minus(1), true, null, 100L, null);
    Assert.assertFalse(cache.addSegment(oldSegmentPlus));
    Assert.assertEquals(Set.of(segmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));

    // Verify that a segment with newer updated time updates the cache
    final DataSegmentPlus newSegmentPlus
        = new DataSegmentPlus(segment, null, now.plus(1), true, null, 100L, null);
    Assert.assertTrue(cache.addSegment(newSegmentPlus));
    Assert.assertEquals(Set.of(newSegmentPlus), cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()));
  }

  @Test
  public void testAddSegment_withUnusedSegment()
  {
    final DataSegmentPlus segmentPlus = createUnusedSegment().asPlus();
    final DataSegment segment = segmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(segmentPlus);

    // Verify that the segment is not returned in any of the used segment methods
    Assert.assertNull(cache.findUsedSegment(segmentId));
    Assert.assertTrue(cache.findUsedSegments(Set.of(segmentId)).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentIdsOverlapping(segment.getInterval()).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentsOverlappingAnyOf(List.of()).isEmpty());
    Assert.assertTrue(cache.findUsedSegmentsPlusOverlappingAnyOf(List.of()).isEmpty());

    Assert.assertEquals(Set.of(segmentId.toString()), cache.findExistingSegmentIds(Set.of(segment)));

    // Verify unused segment methods
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
  }

  @Test
  public void testShouldRefreshUsedSegment_returnsTrueIfCacheHasNoEntry()
  {
    final SegmentId segmentId = SegmentId.dummy(WIKI);
    Assert.assertTrue(cache.shouldRefreshUsedSegment(segmentId, null));
    Assert.assertTrue(cache.shouldRefreshUsedSegment(segmentId, DateTimes.EPOCH));
    Assert.assertTrue(cache.shouldRefreshUsedSegment(segmentId, DateTimes.nowUtc()));
  }

  @Test
  public void testShouldRefreshUsedSegment_returnsTrueIfCacheHasOutdatedEntry()
  {
    final DataSegmentPlus segmentPlus = createUsedSegment().updatedNow().asPlus();
    final DataSegment segment = segmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(segmentPlus);
    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));

    final DateTime cachedUpdatedTime = segmentPlus.getUsedStatusLastUpdatedDate();
    Assert.assertNotNull(cachedUpdatedTime);

    // Verify that segment refresh is required only if updated time has increased
    Assert.assertFalse(cache.shouldRefreshUsedSegment(segmentId, null));
    Assert.assertFalse(cache.shouldRefreshUsedSegment(segmentId, cachedUpdatedTime));
    Assert.assertFalse(cache.shouldRefreshUsedSegment(segmentId, cachedUpdatedTime.minus(1)));

    Assert.assertTrue(cache.shouldRefreshUsedSegment(segmentId, cachedUpdatedTime.plus(1)));
  }

  @Test
  public void testAddSegment_canMarkItAsUsed()
  {
    final DataSegmentPlus usedSegmentPlus = createUsedSegment().asPlus();
    final DataSegment segment = usedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(usedSegmentPlus);

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));
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

    Assert.assertNull(cache.findUsedSegment(segmentId));
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
  }

  @Test
  public void testAddSegment_canMarkItAsUnused()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().asPlus();
    final DataSegment segment = unusedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(unusedSegmentPlus);

    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
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

    cache.addSegment(usedSegmentPlus);

    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));
  }

  @Test
  public void testAddUnusedSegmentId()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().asPlus();
    final DataSegment segment = unusedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    Assert.assertTrue(cache.addUnusedSegmentId(segmentId, null));

    Assert.assertEquals(Set.of(segmentId.toString()), cache.findExistingSegmentIds(Set.of(segment)));
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
  }

  @Test
  public void testAddUnusedSegmentId_updatesCacheWithNewerEntry()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().asPlus();
    final DataSegment segment = unusedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    Assert.assertTrue(cache.addUnusedSegmentId(segmentId, null));

    Assert.assertFalse(cache.addUnusedSegmentId(segmentId, null));

    final DateTime now = DateTimes.nowUtc();
    Assert.assertTrue(cache.addUnusedSegmentId(segmentId, now));

    Assert.assertFalse(cache.addUnusedSegmentId(segmentId, null));
    Assert.assertFalse(cache.addUnusedSegmentId(segmentId, now.minus(1)));

    Assert.assertTrue(cache.addUnusedSegmentId(segmentId, now.plus(1)));
  }

  @Test
  public void testAddUnusedSegmenId_marksUsedSegmentAsUnused()
  {
    final DataSegmentPlus usedSegmentPlus = createUsedSegment().asPlus();
    final DataSegment segment = usedSegmentPlus.getDataSegment();
    final SegmentId segmentId = segment.getId();

    cache.addSegment(usedSegmentPlus);
    Assert.assertEquals(segment, cache.findUsedSegment(segmentId));
    Assert.assertNull(cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));

    cache.addUnusedSegmentId(segmentId, null);
    Assert.assertNull(cache.findUsedSegment(segmentId));
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
  }

  @Test
  public void testMarkCacheSynced_isNeededAfterUpdateOrRemove()
  {
    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().asPlus();
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
    cache.removeSegmentsForIds(Set.of(segmentId));
    Assert.assertEquals(segmentId, cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));

    // Verify that only reset updates the highest ID
    cache.markCacheSynced();
    Assert.assertNull(cache.findHighestUnusedSegmentId(segment.getInterval(), segment.getVersion()));
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
  public void testShouldRefreshPendingSegment_returnsTrueWhenNotPresentInCache()
  {
    final PendingSegmentRecord pendingSegment = PendingSegmentRecord.create(
        new SegmentIdWithShardSpec(WIKI, Intervals.ETERNITY, "v1", new NumberedShardSpec(0, 1)),
        "s1",
        null,
        null,
        null
    );
    Assert.assertTrue(cache.shouldRefreshPendingSegment(pendingSegment));
    Assert.assertTrue(cache.insertPendingSegment(pendingSegment, true));
    Assert.assertFalse(cache.shouldRefreshPendingSegment(pendingSegment));
  }

  @Test
  public void testRemoveUnpersistedSegments_removesUsedSegmentUpdatedBeforeSyncStart()
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
    // last updated before the sync time are remove
    cache.removeUnpersistedSegments(
        Set.of(persistedSegment.getDataSegment().getId()),
        syncTime
    );
    Assert.assertEquals(
        Set.of(persistedSegment, unpersistedSegmentUpdatedAfterSync),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of())
    );
  }

  @Test
  public void testRemoveUnpersistedSegments_removesUnusedSegmentUpdatedBeforeSyncStart()
  {
    final DateTime syncTime = DateTimes.nowUtc();

    final DataSegmentPlus persistedSegment = createUnusedSegment().asPlus();
    final DataSegmentPlus unpersistedSegmentUpdatedBeforeSync =
        createUnusedSegment().lastUpdatedOn(syncTime.minus(1)).asPlus();
    final DataSegmentPlus unpersistedSegmentUpdatedAfterSync =
        createUnusedSegment().lastUpdatedOn(syncTime.plus(1)).asPlus();

    // Add unused segments to the cache and verify that they have been added
    cache.insertSegments(
        Set.of(
            persistedSegment,
            unpersistedSegmentUpdatedBeforeSync,
            unpersistedSegmentUpdatedAfterSync
        )
    );
    Assert.assertEquals(
        Set.of(
            persistedSegment.getDataSegment().getId().toString(),
            unpersistedSegmentUpdatedBeforeSync.getDataSegment().getId().toString(),
            unpersistedSegmentUpdatedAfterSync.getDataSegment().getId().toString()
        ),
        cache.findExistingSegmentIds(
            Set.of(
                persistedSegment.getDataSegment(),
                unpersistedSegmentUpdatedBeforeSync.getDataSegment(),
                unpersistedSegmentUpdatedAfterSync.getDataSegment()
            )
        )
    );

    // Remove unpersisted segments and verify that only unpersisted segments
    // last updated before the sync time are remove
    cache.removeUnpersistedSegments(
        Set.of(persistedSegment.getDataSegment().getId()),
        syncTime
    );
    Assert.assertEquals(
        Set.of(
            persistedSegment.getDataSegment().getId().toString(),
            unpersistedSegmentUpdatedAfterSync.getDataSegment().getId().toString()
        ),
        cache.findExistingSegmentIds(
            Set.of(
                persistedSegment.getDataSegment(),
                unpersistedSegmentUpdatedBeforeSync.getDataSegment(),
                unpersistedSegmentUpdatedAfterSync.getDataSegment()
            )
        )
    );
  }

  @Test
  public void testRemoveUnpersistedPendingSegments_removesPendingSegmentCreatedbeforeSyncStart()
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
    cache.removeUnpersistedPendingSegments(
        Set.of(persistedSegment.getId().toString()),
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
    final String usedSegmentId = usedSegmentPlus.getDataSegment().getId().toString();

    final DataSegmentPlus unusedSegmentPlus = createUnusedSegment().withVersion("v1").asPlus();
    final String unusedSegmentId = unusedSegmentPlus.getDataSegment().getId().toString();

    cache.addSegment(usedSegmentPlus);
    cache.addSegment(unusedSegmentPlus);
    Assert.assertEquals(
        Set.of(usedSegmentId, unusedSegmentId),
        cache.findExistingSegmentIds(Set.of(usedSegmentPlus.getDataSegment(), unusedSegmentPlus.getDataSegment()))
    );

    Assert.assertEquals(
        2,
        cache.deleteSegments(
            Set.of(usedSegmentPlus.getDataSegment().getId(), unusedSegmentPlus.getDataSegment().getId())
        )
    );
    Assert.assertTrue(
        cache.findExistingSegmentIds(Set.of(usedSegmentPlus.getDataSegment(), unusedSegmentPlus.getDataSegment()))
             .isEmpty()
    );
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
    final Interval firstDayOfJan = FIRST_WEEK_OF_JAN.withDurationAfterStart(Duration.standardDays(1));
    final DataSegmentPlus jan1Segment
        = createUsedSegment().startingAt(firstDayOfJan.getStart()).asPlus();
    final DataSegmentPlus jan2Segment
        = createUsedSegment().startingAt(firstDayOfJan.getEnd()).asPlus();
    final DataSegmentPlus jan3Segment
        = createUsedSegment().startingAt(firstDayOfJan.getEnd().plusDays(1)).asPlus();

    cache.insertSegments(Set.of(jan1Segment, jan2Segment, jan3Segment));
    Assert.assertEquals(
        Set.of(jan1Segment, jan2Segment, jan3Segment),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(FIRST_WEEK_OF_JAN))
    );

    // Mark segments as unused
    Assert.assertEquals(
        2,
        cache.markSegmentsWithinIntervalAsUnused(
            firstDayOfJan.withDurationAfterStart(Duration.standardDays(2)),
            DateTimes.nowUtc()
        )
    );

    // Verify that all the segment IDs are still present in cache but 2 have
    // been marked as unused
    Assert.assertEquals(
        Set.of(jan3Segment),
        cache.findUsedSegmentsPlusOverlappingAnyOf(List.of(FIRST_WEEK_OF_JAN))
    );
    Assert.assertEquals(
        Set.of(
            jan1Segment.getDataSegment().getId().toString(),
            jan2Segment.getDataSegment().getId().toString(),
            jan3Segment.getDataSegment().getId().toString()
        ),
        cache.findExistingSegmentIds(
            Set.of(
                jan1Segment.getDataSegment(),
                jan2Segment.getDataSegment(),
                jan3Segment.getDataSegment()
            )
        )
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
}
