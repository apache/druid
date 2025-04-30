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

package org.apache.druid.metadata;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataQueryTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private static final DateTime JAN_1 = DateTimes.of("2025-01-01");
  private static final String V1 = JAN_1.toString();
  private static final String V2 = JAN_1.plusDays(1).toString();

  private static final List<DataSegment> WIKI_SEGMENTS_2X5D
      = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(5, Granularities.DAY)
                          .withNumPartitions(2)
                          .startingAt(JAN_1)
                          .withVersion(V1)
                          .eachOfSizeInMb(500);

  @Before
  public void setUp()
  {
    derbyConnectorRule.getConnector().createSegmentTable();
    insertSegments(WIKI_SEGMENTS_2X5D.toArray(new DataSegment[0]));
  }

  @Test
  public void test_markSegmentsAsUnused()
  {
    // Check segments currently present in the metadata store
    Assert.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
    Assert.assertTrue(retrieveAllUnusedSegments().isEmpty());

    // Mark segments as unused and verify the results
    final Set<DataSegment> segmentsToUpdate = Set.of(WIKI_SEGMENTS_2X5D.get(0), WIKI_SEGMENTS_2X5D.get(1));
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUnused(getIds(segmentsToUpdate), DateTimes.nowUtc())
    );
    Assert.assertEquals(2, numUpdatedSegments);
    Assert.assertEquals(segmentsToUpdate, retrieveAllUnusedSegments());

    // Verify that these segments are not present in used segments set
    Set<DataSegment> usedSegments = retrieveAllUsedSegments();
    Assert.assertEquals(8, usedSegments.size());

    segmentsToUpdate.forEach(
        updatedSegment -> Assert.assertFalse(usedSegments.contains(updatedSegment))
    );
  }

  @Test
  public void test_markSegmentsAsUsed()
  {
    // Mark segments as unused and verify the results
    final Set<DataSegment> segmentsToUpdate = Set.of(WIKI_SEGMENTS_2X5D.get(0), WIKI_SEGMENTS_2X5D.get(1));
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUnused(getIds(segmentsToUpdate), DateTimes.nowUtc())
    );
    Assert.assertEquals(2, numUpdatedSegments);
    Assert.assertEquals(segmentsToUpdate, retrieveAllUnusedSegments());

    // Mark segments as used again and verify the results
    numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUsed(getIds(segmentsToUpdate), DateTimes.nowUtc())
    );
    Assert.assertEquals(2, numUpdatedSegments);
    Assert.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
    Assert.assertTrue(retrieveAllUnusedSegments().isEmpty());
  }

  @Test
  public void test_markSegmentsAsUnused_forEmptySegmentIds_isNoop()
  {
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUnused(Set.of(), DateTimes.nowUtc())
    );
    Assert.assertEquals(0, numUpdatedSegments);
    Assert.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
  }

  @Test
  public void test_markSegmentsUnused_forEternityInterval()
  {
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsUnused(TestDataSource.WIKI, Intervals.ETERNITY, null, DateTimes.nowUtc())
    );
    Assert.assertEquals(WIKI_SEGMENTS_2X5D.size(), numUpdatedSegments);
    Assert.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUnusedSegments());
    Assert.assertTrue(retrieveAllUsedSegments().isEmpty());
  }

  @Test
  public void test_markSegmentsUnused_forSingleVersion()
  {
    // Insert v2 segments
    insertSegments(
        WIKI_SEGMENTS_2X5D.stream().map(
            segment -> DataSegment.builder(segment).version(V2).build()
        ).toArray(DataSegment[]::new)
    );

    // Update segments for 2 days
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsUnused(
            TestDataSource.WIKI,
            new Interval(JAN_1, Period.days(2)),
            List.of(V1),
            DateTimes.nowUtc()
        )
    );
    Assert.assertEquals(4, numUpdatedSegments);
    Assert.assertEquals(4, retrieveAllUnusedSegments().size());
    Assert.assertEquals(16, retrieveAllUsedSegments().size());
  }

  @Test
  public void test_markSegmentsUnused_forMultipleVersions()
  {
    // Insert v2 segments
    insertSegments(
        WIKI_SEGMENTS_2X5D.stream().map(
            segment -> DataSegment.builder(segment).version(V2).build()
        ).toArray(DataSegment[]::new)
    );

    // Update segments for 2 days
    final List<String> versionsToUpdate = List.of(V1, V2);
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsUnused(
            TestDataSource.WIKI,
            new Interval(JAN_1, Period.days(2)),
            versionsToUpdate,
            DateTimes.nowUtc()
        )
    );
    Assert.assertEquals(8, numUpdatedSegments);
    Assert.assertEquals(8, retrieveAllUnusedSegments().size());
    Assert.assertEquals(12, retrieveAllUsedSegments().size());
  }

  @Test
  public void test_markSegmentsUnused_forAllVersions()
  {
    // Insert v2 segments
    insertSegments(
        WIKI_SEGMENTS_2X5D.stream().map(
            segment -> DataSegment.builder(segment).version(V2).build()
        ).toArray(DataSegment[]::new)
    );

    // Update segments for 2 days
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsUnused(
            TestDataSource.WIKI,
            new Interval(JAN_1, Period.days(2)),
            null,
            DateTimes.nowUtc()
        )
    );
    Assert.assertEquals(8, numUpdatedSegments);
    Assert.assertEquals(8, retrieveAllUnusedSegments().size());
    Assert.assertEquals(12, retrieveAllUsedSegments().size());
  }

  @Test
  public void test_markSegmentsUnused_forEmptyVersions_isNoop()
  {
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsUnused(TestDataSource.WIKI, Intervals.ETERNITY, List.of(), DateTimes.nowUtc())
    );
    Assert.assertEquals(0, numUpdatedSegments);
    Assert.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
    Assert.assertTrue(retrieveAllUnusedSegments().isEmpty());
  }

  @Test
  public void test_retrieveSegmentForId()
  {
    final DataSegment segmentJan1 = WIKI_SEGMENTS_2X5D.get(0);
    Assert.assertEquals(
        segmentJan1,
        read(sql -> sql.retrieveSegmentForId(segmentJan1.getId()))
    );
  }

  @Test
  public void test_retrieveSegmentForId_returnsNull_forUnknownId()
  {
    Assert.assertNull(
        read(
            sql -> sql.retrieveSegmentForId(SegmentId.dummy(TestDataSource.WIKI))
        )
    );
  }

  /**
   * Reads segments from the metadata store using a
   * {@link SqlSegmentsMetadataQuery} object.
   */
  private <T> T read(Function<SqlSegmentsMetadataQuery, T> function)
  {
    final DerbyConnector connector = derbyConnectorRule.getConnector();
    final MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    return connector.inReadOnlyTransaction(
        (handle, status) -> function.apply(
            SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, TestHelper.JSON_MAPPER)
        )
    );
  }

  /**
   * Reads a set of segments from the metadata store using a
   * {@link SqlSegmentsMetadataQuery} object.
   */
  private <T> Set<T> readAsSet(Function<SqlSegmentsMetadataQuery, CloseableIterator<T>> iterableReader)
  {
    final DerbyConnector connector = derbyConnectorRule.getConnector();
    final MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    return connector.inReadOnlyTransaction((handle, status) -> {
      final SqlSegmentsMetadataQuery query =
          SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, TestHelper.JSON_MAPPER);

      try (CloseableIterator<T> iterator = iterableReader.apply(query)) {
        return ImmutableSet.copyOf(iterator);
      }
    });
  }

  /**
   * Executes an update using a {@link SqlSegmentsMetadataQuery} object.
   */
  private <T> T update(Function<SqlSegmentsMetadataQuery, T> function)
  {
    final DerbyConnector connector = derbyConnectorRule.getConnector();
    final MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    return connector.retryWithHandle(
        handle -> function.apply(
            SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, TestHelper.JSON_MAPPER)
        )
    );
  }

  private Set<DataSegment> retrieveAllUsedSegments()
  {
    return readAsSet(
        sql -> sql.retrieveUsedSegments(TestDataSource.WIKI, List.of())
    );
  }

  private Set<DataSegment> retrieveAllUnusedSegments()
  {
    return readAsSet(
        sql -> sql.retrieveUnusedSegments(TestDataSource.WIKI, List.of(), null, null, null, null, null)
    );
  }

  private void insertSegments(DataSegment... segments)
  {
    IndexerSqlMetadataStorageCoordinatorTestBase.insertUsedSegments(
        Set.of(segments),
        Map.of(),
        derbyConnectorRule,
        TestHelper.JSON_MAPPER
    );
  }

  private static Set<SegmentId> getIds(Set<DataSegment> segments)
  {
    return segments.stream().map(DataSegment::getId).collect(Collectors.toSet());
  }
}
