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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.AvailableSegmentMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.SegmentStatusInCluster;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * Tests {@link SegmentsRollup#poll()}: that it reads both the published and available sources, joins
 * available metadata into published segments, does not double-count an available-only segment already
 * seen in the published set, and groups the result per datasource (sorted). This is the "served
 * semantics" guard for the accelerated {@code GROUP BY datasource} path - the rollup must reflect the
 * same segments a scan of sys.segments would.
 */
public class SegmentsRollupTest
{
  private static final boolean[] ALL = allCells();

  private MetadataSegmentView metadataView;
  private BrokerSegmentMetadataCache availableCache;
  private SegmentsRollup rollup;

  @Before
  public void setUp()
  {
    metadataView = EasyMock.createMock(MetadataSegmentView.class);
    availableCache = EasyMock.createMock(BrokerSegmentMetadataCache.class);
    rollup = new SegmentsRollup(
        metadataView,
        availableCache,
        BrokerSegmentMetadataCacheConfig.create(),
        EasyMock.createNiceMock(ServiceEmitter.class)
    );
  }

  @Test
  public void testStatsNullBeforeFirstPoll()
  {
    Assert.assertNull(rollup.getStatsIfReady());
  }

  @Test
  public void testPollJoinsDedupsAndGroupsByDatasource()
  {
    // Published: two "foo" segments. P1 has available metadata; P2 (realtime) has none.
    final DataSegment fooP1 = segment("foo", 0, 100L);
    final DataSegment fooP2 = segment("foo", 1, 50L);
    final SegmentStatusInCluster statusP1 = new SegmentStatusInCluster(fooP1, false, 2, 10L, false);
    final SegmentStatusInCluster statusP2 = new SegmentStatusInCluster(fooP2, false, null, 5L, true);

    EasyMock.expect(metadataView.getSegments((Set<String>) null))
            .andReturn(ImmutableList.of(statusP1, statusP2).iterator());

    EasyMock.expect(availableCache.getAvailableSegmentMetadata("foo", fooP1.getId()))
            .andReturn(available(fooP1, 0L /* historical */, 10L));
    EasyMock.expect(availableCache.getAvailableSegmentMetadata("foo", fooP2.getId()))
            .andReturn(null);

    // Available-only iterator: a duplicate of the already-seen fooP1 (must be skipped), plus a new
    // "bar" segment that only the available side knows about.
    final DataSegment barA1 = segment("bar", 0, 30L);
    EasyMock.expect(availableCache.iterateSegmentMetadata((Set<String>) null))
            .andReturn(iterator(available(fooP1, 0L, 10L), available(barA1, 0L, 3L)));

    EasyMock.replay(metadataView, availableCache);

    rollup.poll();

    final ImmutableSortedMap<String, DatasourceSegmentStats> stats = rollup.getStatsIfReady();
    Assert.assertNotNull(stats);
    // Sorted by datasource, and the available-only duplicate of fooP1 did not create a phantom entry.
    Assert.assertEquals(Arrays.asList("bar", "foo"), ImmutableList.copyOf(stats.keySet()));

    // foo: both published segments counted once; sizes summed.
    final DatasourceSegmentStats foo = stats.get("foo");
    Assert.assertEquals(2, foo.count(ALL));
    Assert.assertEquals(150L, foo.sumSize(ALL));
    Assert.assertEquals(15L, foo.sumNumRows(ALL)); // 10 (P1) + 5 (P2 from SegmentStatusInCluster)

    // bar: only the available-only segment; the fooP1 duplicate was skipped (not counted here).
    final DatasourceSegmentStats bar = stats.get("bar");
    Assert.assertEquals(1, bar.count(ALL));
    Assert.assertEquals(30L, bar.sumSize(ALL));

    EasyMock.verify(metadataView, availableCache);
  }

  private static DataSegment segment(final String dataSource, final int day, final long size)
  {
    final SegmentId id = SegmentId.of(
        dataSource,
        Intervals.utc(day * 86_400_000L, (day + 1) * 86_400_000L),
        "1",
        new LinearShardSpec(0)
    );
    return DataSegment.builder(id).size(size).build();
  }

  private static AvailableSegmentMetadata available(final DataSegment segment, final long isRealtime, final long numRows)
  {
    return AvailableSegmentMetadata
        .builder(segment, isRealtime, Collections.emptySet(), RowSignature.empty(), numRows)
        .build();
  }

  private static Iterator<AvailableSegmentMetadata> iterator(final AvailableSegmentMetadata... metas)
  {
    return Arrays.asList(metas).iterator();
  }

  private static boolean[] allCells()
  {
    final boolean[] mask = new boolean[DatasourceSegmentStats.NUM_CELLS];
    Arrays.fill(mask, true);
    return mask;
  }
}
