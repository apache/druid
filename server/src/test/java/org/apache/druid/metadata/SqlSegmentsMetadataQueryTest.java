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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.segment.cache.IndexingStateRecord;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.SqlIndexingStateStorage;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataQueryTest
{
  @RegisterExtension
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

  @BeforeEach
  public void setUp()
  {
    derbyConnectorRule.getConnector().createSegmentTable();
    insertSegments(WIKI_SEGMENTS_2X5D.toArray(new DataSegment[0]));
  }

  @Test
  public void test_markSegmentsAsUnused()
  {
    // Check segments currently present in the metadata store
    Assertions.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
    Assertions.assertTrue(retrieveAllUnusedSegments().isEmpty());

    // Mark segments as unused and verify the results
    final Set<DataSegment> segmentsToUpdate = Set.of(WIKI_SEGMENTS_2X5D.get(0), WIKI_SEGMENTS_2X5D.get(1));
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUnused(getIds(segmentsToUpdate), DateTimes.nowUtc())
    );
    Assertions.assertEquals(2, numUpdatedSegments);
    Assertions.assertEquals(segmentsToUpdate, retrieveAllUnusedSegments());

    // Verify that these segments are not present in used segments set
    Set<DataSegment> usedSegments = retrieveAllUsedSegments();
    Assertions.assertEquals(8, usedSegments.size());

    segmentsToUpdate.forEach(
        updatedSegment -> Assertions.assertFalse(usedSegments.contains(updatedSegment))
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
    Assertions.assertEquals(2, numUpdatedSegments);
    Assertions.assertEquals(segmentsToUpdate, retrieveAllUnusedSegments());

    // Mark segments as used again and verify the results
    numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUsed(getIds(segmentsToUpdate), DateTimes.nowUtc())
    );
    Assertions.assertEquals(2, numUpdatedSegments);
    Assertions.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
    Assertions.assertTrue(retrieveAllUnusedSegments().isEmpty());
  }

  @Test
  public void test_markSegmentsAsUnused_forEmptySegmentIds_isNoop()
  {
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUnused(Set.of(), DateTimes.nowUtc())
    );
    Assertions.assertEquals(0, numUpdatedSegments);
    Assertions.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
  }

  @Test
  public void test_markSegmentsUnused_forEternityInterval()
  {
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsUnused(TestDataSource.WIKI, Intervals.ETERNITY, null, DateTimes.nowUtc())
    );
    Assertions.assertEquals(WIKI_SEGMENTS_2X5D.size(), numUpdatedSegments);
    Assertions.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUnusedSegments());
    Assertions.assertTrue(retrieveAllUsedSegments().isEmpty());
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
    Assertions.assertEquals(4, numUpdatedSegments);
    Assertions.assertEquals(4, retrieveAllUnusedSegments().size());
    Assertions.assertEquals(16, retrieveAllUsedSegments().size());
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
    Assertions.assertEquals(8, numUpdatedSegments);
    Assertions.assertEquals(8, retrieveAllUnusedSegments().size());
    Assertions.assertEquals(12, retrieveAllUsedSegments().size());
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
    Assertions.assertEquals(8, numUpdatedSegments);
    Assertions.assertEquals(8, retrieveAllUnusedSegments().size());
    Assertions.assertEquals(12, retrieveAllUsedSegments().size());
  }

  @Test
  public void test_markSegmentsUnused_forEmptyVersions_isNoop()
  {
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsUnused(TestDataSource.WIKI, Intervals.ETERNITY, List.of(), DateTimes.nowUtc())
    );
    Assertions.assertEquals(0, numUpdatedSegments);
    Assertions.assertEquals(Set.copyOf(WIKI_SEGMENTS_2X5D), retrieveAllUsedSegments());
    Assertions.assertTrue(retrieveAllUnusedSegments().isEmpty());
  }

  @Test
  public void test_retrieveSegmentForId()
  {
    final DataSegment segmentJan1 = WIKI_SEGMENTS_2X5D.get(0);
    Assertions.assertEquals(
        segmentJan1,
        read(sql -> sql.retrieveSegmentForId(segmentJan1.getId()))
    );
  }

  @Test
  public void test_retrieveSegmentForId_returnsNull_forUnknownId()
  {
    Assertions.assertNull(
        read(
            sql -> sql.retrieveSegmentForId(SegmentId.dummy(TestDataSource.WIKI))
        )
    );
  }

  @Test
  public void test_retrieveUsedSegments_withOverlapsCondition()
  {
    Interval queryInterval = new Interval(JAN_1.plusDays(2), JAN_1.plusDays(4));

    Set<DataSegment> result = readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval)));

    Assertions.assertEquals(4, result.size());
    assertSegmentsOverlapInterval(result, queryInterval);
  }

  @Test
  public void test_retrieveUsedSegments_withOverlapsCondition_andUnusedSegments()
  {
    final Set<DataSegment> segmentsToUpdate = Set.of(WIKI_SEGMENTS_2X5D.get(2));
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUnused(getIds(segmentsToUpdate), DateTimes.nowUtc())
    );
    Assertions.assertEquals(1, numUpdatedSegments);

    final Interval queryInterval = new Interval(JAN_1, JAN_1.plusDays(2));

    Set<DataSegment> result = readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval)));

    Assertions.assertEquals(3, result.size());
    assertSegmentsOverlapInterval(result, queryInterval);
  }

  @Test
  public void test_retrieveUsedSegments_withOverlapsCondition_nearEndDate()
  {
    Interval queryInterval = new Interval(JAN_1.plusDays(4), JAN_1.plusDays(5));

    Set<DataSegment> result = readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval)));
    Assertions.assertEquals(2, result.size());
    assertSegmentsOverlapInterval(result, queryInterval);
  }

  private void assertSegmentsOverlapInterval(
      Set<DataSegment> segments,
      Interval interval
  )
  {
    for (DataSegment segment : segments) {
      Assertions.assertTrue(
          segment.getInterval().overlaps(interval),
          "Segment " + segment.getId() + " should be in interval " + interval
      );
    }
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

  // ==================== Indexing State Tests ====================

  @Test
  public void test_retrieveAllUsedIndexingStateFingerprints_emptyDatabase()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Set<String> fingerprints = read(SqlSegmentsMetadataQuery::retrieveAllUsedIndexingStateFingerprints);

    Assertions.assertTrue(fingerprints.isEmpty(), "Should return empty set when no segments have indexing states");
  }

  @Test
  public void test_retrieveAllUsedIndexingStateFingerprints()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    indexingStates.put("fp2", createTestIndexingState());
    indexingStates.put("fp3", createTestIndexingState());
    insertIndexingStates(indexingStates);

    insertSegmentWithIndexingState("seg1", "fp1", true);
    insertSegmentWithIndexingState("seg2", "fp2", true);
    insertSegmentWithIndexingState("seg3", "fp1", true);  // Duplicate fingerprint
    insertSegmentWithIndexingState("seg4", "fp3", false); // Unused segment

    Set<String> fingerprints = read(SqlSegmentsMetadataQuery::retrieveAllUsedIndexingStateFingerprints);

    Assertions.assertEquals(Set.of("fp1", "fp2", "fp3"), fingerprints, "Should return all fingerprints in the cache");
  }

  @Test
  public void test_retrieveAllUsedIndexingStateFingerprints_ignoresNullFingerprints()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    insertIndexingStates(indexingStates);

    insertSegmentWithIndexingState("seg1", "fp1", true);
    insertSegmentWithIndexingState("seg2", null, true); // No indexing state

    Set<String> fingerprints = read(SqlSegmentsMetadataQuery::retrieveAllUsedIndexingStateFingerprints);

    Assertions.assertEquals(Set.of("fp1"), fingerprints, "Should ignore segments without indexing states");
  }

  @Test
  public void test_retrieveAllUsedIndexingStates_emptyDatabase()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    List<IndexingStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedIndexingStates);

    Assertions.assertTrue(records.isEmpty(), "Should return empty list when no indexing states exist");
  }

  @Test
  public void test_retrieveAllUsedIndexingStates_fullSync()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    CompactionState state1 = createTestIndexingState();
    CompactionState state2 = CompactionState.builder()
                                            .partitionsSpec(new DynamicPartitionsSpec(200, null))
                                            .dimensionsSpec(DimensionsSpec.EMPTY)
                                            .indexSpec(IndexSpec.getDefault())
                                            .build();
    CompactionState state3 = createTestIndexingState();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", state1);
    indexingStates.put("fp2", state2);
    indexingStates.put("fp3", state3); // Unreferenced state
    insertIndexingStates(indexingStates);

    // Only reference fp1 and fp2
    insertSegmentWithIndexingState("seg1", "fp1", true);
    insertSegmentWithIndexingState("seg2", "fp2", true);

    List<IndexingStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedIndexingStates);

    Assertions.assertEquals(3, records.size(), "Should return all indexing states");

    Set<String> retrievedFingerprints = records.stream()
                                                .map(IndexingStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assertions.assertEquals(Set.of("fp1", "fp2", "fp3"), retrievedFingerprints, "Should contain all fps");

    // Verify payloads
    Map<String, CompactionState> retrievedStates = records.stream()
        .collect(Collectors.toMap(
            IndexingStateRecord::getFingerprint,
            IndexingStateRecord::getState
        ));
    Assertions.assertEquals(state1, retrievedStates.get("fp1"), "fp1 state should match");
    Assertions.assertEquals(state2, retrievedStates.get("fp2"), "fp2 state should match");
    Assertions.assertEquals(state3, retrievedStates.get("fp3"), "fp3 state should match");
  }

  @Test
  public void test_retrieveAllUsedIndexingStates_onlyFromUsedSegments()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    indexingStates.put("fp2", createTestIndexingState());
    insertIndexingStates(indexingStates);

    insertSegmentWithIndexingState("seg1", "fp1", true);  // Used
    insertSegmentWithIndexingState("seg2", "fp2", false); // Unused

    List<IndexingStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedIndexingStates);

    Assertions.assertEquals(2, records.size(), "Should only return all indexing states");
  }

  @Test
  public void test_retrieveAllUsedIndexingStates_ignoresUnusedIndexingStates()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    insertIndexingStates(indexingStates);

    insertSegmentWithIndexingState("seg1", "fp1", true);

    markIndexingStateAsUnused("fp1");

    List<IndexingStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedIndexingStates);

    Assertions.assertTrue(records.isEmpty(), "Should not return unused indexing states");
  }

  @Test
  public void test_retrieveIndexingStatesForFingerprints_emptyInput()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    List<IndexingStateRecord> records = read(
        sql -> sql.retrieveIndexingStatesForFingerprints(Set.of())
    );

    Assertions.assertTrue(records.isEmpty(), "Should return empty list for empty input");
  }

  @Test
  public void test_retrieveIndexingStatesForFingerprints_deltaSync()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    indexingStates.put("fp2", createTestIndexingState());
    indexingStates.put("fp3", createTestIndexingState());
    insertIndexingStates(indexingStates);

    // Request specific fingerprints (delta sync scenario)
    List<IndexingStateRecord> records = read(
        sql -> sql.retrieveIndexingStatesForFingerprints(Set.of("fp1", "fp3"))
    );

    Assertions.assertEquals(2, records.size(), "Should return requested fingerprints");

    Set<String> retrievedFingerprints = records.stream()
                                                .map(IndexingStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assertions.assertEquals(Set.of("fp1", "fp3"), retrievedFingerprints, "Should contain only requested fingerprints");
  }

  @Test
  public void test_retrieveIndexingStatesForFingerprints_largeBatch()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    // Insert 150 indexing states (exceeds batching threshold of 100)
    Map<String, CompactionState> indexingStates = new HashMap<>();
    Set<String> expectedFingerprints = new HashSet<>();
    for (int i = 0; i < 150; i++) {
      String fingerprint = "fp" + i;
      indexingStates.put(fingerprint, createTestIndexingState());
      expectedFingerprints.add(fingerprint);
    }
    insertIndexingStates(indexingStates);

    // Request all fingerprints
    List<IndexingStateRecord> records = read(
        sql -> sql.retrieveIndexingStatesForFingerprints(expectedFingerprints)
    );

    Assertions.assertEquals(150, records.size(), "Should return all fingerprints across multiple batches");

    Set<String> retrievedFingerprints = records.stream()
                                                .map(IndexingStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assertions.assertEquals(expectedFingerprints, retrievedFingerprints, "Should contain all requested fingerprints");
  }

  @Test
  public void test_retrieveIndexingStatesForFingerprints_nonexistentFingerprints()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    insertIndexingStates(indexingStates);

    // Request fingerprints that don't exist
    List<IndexingStateRecord> records = read(
        sql -> sql.retrieveIndexingStatesForFingerprints(Set.of("fp999", "fp888"))
    );

    Assertions.assertTrue(records.isEmpty(), "Should return empty list when fingerprints don't exist");
  }

  @Test
  public void test_retrieveIndexingStatesForFingerprints_mixedExistingAndNonexistent()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    indexingStates.put("fp2", createTestIndexingState());
    insertIndexingStates(indexingStates);

    // Mix existing and non-existing fingerprints
    List<IndexingStateRecord> records = read(
        sql -> sql.retrieveIndexingStatesForFingerprints(Set.of("fp1", "fp999", "fp2", "fp888"))
    );

    Assertions.assertEquals(2, records.size(), "Should return only existing fingerprints");

    Set<String> retrievedFingerprints = records.stream()
                                                .map(IndexingStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assertions.assertEquals(Set.of("fp1", "fp2"), retrievedFingerprints, "Should contain only existing fingerprints");
  }

  @Test
  public void test_retrieveIndexingStatesForFingerprints_onlyReturnsUsedStates()
  {
    derbyConnectorRule.getConnector().createIndexingStatesTable();

    Map<String, CompactionState> indexingStates = new HashMap<>();
    indexingStates.put("fp1", createTestIndexingState());
    indexingStates.put("fp2", createTestIndexingState());
    insertIndexingStates(indexingStates);

    // Mark fp2 as unused
    markIndexingStateAsUnused("fp2");

    List<IndexingStateRecord> records = read(
        sql -> sql.retrieveIndexingStatesForFingerprints(Set.of("fp1", "fp2"))
    );

    Assertions.assertEquals(1, records.size(), "Should only return used indexing states");
    Assertions.assertEquals("fp1", records.get(0).getFingerprint(), "Should return fp1");
  }

  // ==================== Helper Methods for Indexing State Tests ====================

  private CompactionState createTestIndexingState()
  {
    return CompactionState.builder()
                          .partitionsSpec(new DynamicPartitionsSpec(100, null))
                          .dimensionsSpec(DimensionsSpec.EMPTY)
                          .indexSpec(IndexSpec.getDefault())
                          .build();
  }

  private void insertIndexingStates(Map<String, CompactionState> indexingStates)
  {
    ObjectMapper mapper = TestHelper.JSON_MAPPER;
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    SqlIndexingStateStorage manager = new SqlIndexingStateStorage(
        tablesConfig,
        mapper,
        derbyConnectorRule.getConnector()
    );

    derbyConnectorRule.getConnector().retryWithHandle(handle -> {
      for (Map.Entry<String, CompactionState> entry : indexingStates.entrySet()) {
        manager.upsertIndexingState(TestDataSource.WIKI, entry.getKey(), entry.getValue(), DateTimes.nowUtc());
      }
      return null;
    });
  }

  private void insertSegmentWithIndexingState(
      String segmentId,
      String indexingStateFingerprint,
      boolean used
  )
  {
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    DerbyConnector connector = derbyConnectorRule.getConnector();

    connector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getSegmentsTable() + " "
                + "(id, dataSource, created_date, start, \"end\", partitioned, version, used, payload, "
                + "used_status_last_updated, indexing_state_fingerprint) "
                + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, "
                + ":used_status_last_updated, :indexing_state_fingerprint)"
            )
            .bind("id", segmentId)
            .bind("dataSource", TestDataSource.WIKI)
            .bind("created_date", DateTimes.nowUtc().toString())
            .bind("start", JAN_1.toString())
            .bind("end", JAN_1.plusDays(1).toString())
            .bind("partitioned", false)
            .bind("version", V1)
            .bind("used", used)
            .bind("payload", TestHelper.JSON_MAPPER.writeValueAsBytes(WIKI_SEGMENTS_2X5D.get(0)))
            .bind("used_status_last_updated", DateTimes.nowUtc().toString())
            .bind("indexing_state_fingerprint", indexingStateFingerprint)
            .execute();
      return null;
    });
  }

  private void markIndexingStateAsUnused(String fingerprint)
  {
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    DerbyConnector connector = derbyConnectorRule.getConnector();

    connector.retryWithHandle(handle -> {
      handle.createStatement(
                "UPDATE " + tablesConfig.getIndexingStatesTable() + " "
                + "SET used = false "
                + "WHERE fingerprint = :fingerprint"
            )
            .bind("fingerprint", fingerprint)
            .execute();
      return null;
    });
  }
}
