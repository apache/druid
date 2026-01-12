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
import org.apache.druid.metadata.segment.cache.CompactionStateRecord;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.metadata.SqlCompactionStateStorage;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.CompactionState;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
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

  @Test
  public void test_retrieveUsedSegments_withOverlapsCondition()
  {
    Interval queryInterval = new Interval(JAN_1.plusDays(2), JAN_1.plusDays(4));

    Set<DataSegment> result = readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval)));

    Assert.assertEquals(4, result.size());
    assertSegmentsOverlapInterval(result, queryInterval);
  }

  @Test
  public void test_retrieveUsedSegments_withOverlapsCondition_andUnusedSegments()
  {
    final Set<DataSegment> segmentsToUpdate = Set.of(WIKI_SEGMENTS_2X5D.get(2));
    int numUpdatedSegments = update(
        sql -> sql.markSegmentsAsUnused(getIds(segmentsToUpdate), DateTimes.nowUtc())
    );
    Assert.assertEquals(1, numUpdatedSegments);

    final Interval queryInterval = new Interval(JAN_1, JAN_1.plusDays(2));

    Set<DataSegment> result = readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval)));

    Assert.assertEquals(3, result.size());
    assertSegmentsOverlapInterval(result, queryInterval);
  }

  @Test
  public void test_retrieveUsedSegments_withOverlapsCondition_nearEndDate()
  {
    Interval queryInterval = new Interval(JAN_1.plusDays(4), JAN_1.plusDays(5));

    Set<DataSegment> result = readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval)));
    Assert.assertEquals(2, result.size());
    assertSegmentsOverlapInterval(result, queryInterval);
  }

  private void assertSegmentsOverlapInterval(
      Set<DataSegment> segments,
      Interval interval
  )
  {
    for (DataSegment segment : segments) {
      Assert.assertTrue(
          "Segment " + segment.getId() + " should be in interval " + interval,
          segment.getInterval().overlaps(interval)
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

  // ==================== Compaction State Tests ====================

  @Test
  public void test_retrieveAllUsedCompactionStateFingerprints_emptyDatabase()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    Set<String> fingerprints = read(SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStateFingerprints);

    Assert.assertTrue("Should return empty set when no segments have compaction states", fingerprints.isEmpty());
  }

  @Test
  public void test_retrieveAllUsedCompactionStateFingerprints()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    // Insert compaction states
    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    compactionStates.put("fp2", createTestCompactionState());
    compactionStates.put("fp3", createTestCompactionState());
    insertCompactionStates(compactionStates);

    // Insert segments referencing compaction states
    insertSegmentWithCompactionState("seg1", "fp1", true);
    insertSegmentWithCompactionState("seg2", "fp2", true);
    insertSegmentWithCompactionState("seg3", "fp1", true);  // Duplicate fingerprint
    insertSegmentWithCompactionState("seg4", "fp3", false); // Unused segment

    Set<String> fingerprints = read(SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStateFingerprints);

    Assert.assertEquals("Should return all fingerprints in the cache", Set.of("fp1", "fp2", "fp3"), fingerprints);
  }

  @Test
  public void test_retrieveAllUsedCompactionStateFingerprints_ignoresNullFingerprints()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    insertCompactionStates(compactionStates);

    insertSegmentWithCompactionState("seg1", "fp1", true);
    insertSegmentWithCompactionState("seg2", null, true); // No compaction state

    Set<String> fingerprints = read(SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStateFingerprints);

    Assert.assertEquals("Should ignore segments without compaction states", Set.of("fp1"), fingerprints);
  }

  @Test
  public void test_retrieveAllUsedCompactionStates_emptyDatabase()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    List<CompactionStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStates);

    Assert.assertTrue("Should return empty list when no compaction states exist", records.isEmpty());
  }

  @Test
  public void test_retrieveAllUsedCompactionStates_fullSync()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    // Create distinct compaction states
    CompactionState state1 = createTestCompactionState();
    CompactionState state2 = new CompactionState(
        new DynamicPartitionsSpec(200, null),
        DimensionsSpec.EMPTY,
        null,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
    CompactionState state3 = createTestCompactionState();

    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", state1);
    compactionStates.put("fp2", state2);
    compactionStates.put("fp3", state3); // Unreferenced state
    insertCompactionStates(compactionStates);

    // Only reference fp1 and fp2
    insertSegmentWithCompactionState("seg1", "fp1", true);
    insertSegmentWithCompactionState("seg2", "fp2", true);

    List<CompactionStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStates);

    Assert.assertEquals("Should return all compaction states", 3, records.size());

    Set<String> retrievedFingerprints = records.stream()
                                                .map(CompactionStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assert.assertEquals("Should contain all fps", Set.of("fp1", "fp2", "fp3"), retrievedFingerprints);

    // Verify payloads
    Map<String, CompactionState> retrievedStates = records.stream()
        .collect(Collectors.toMap(
            CompactionStateRecord::getFingerprint,
            CompactionStateRecord::getState
        ));
    Assert.assertEquals("fp1 state should match", state1, retrievedStates.get("fp1"));
    Assert.assertEquals("fp2 state should match", state2, retrievedStates.get("fp2"));
    Assert.assertEquals("fp3 state should match", state3, retrievedStates.get("fp3"));
  }

  @Test
  public void test_retrieveAllUsedCompactionStates_onlyFromUsedSegments()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    compactionStates.put("fp2", createTestCompactionState());
    insertCompactionStates(compactionStates);

    insertSegmentWithCompactionState("seg1", "fp1", true);  // Used
    insertSegmentWithCompactionState("seg2", "fp2", false); // Unused

    List<CompactionStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStates);

    Assert.assertEquals("Should only return all compaction states", 2, records.size());
  }

  @Test
  public void test_retrieveAllUsedCompactionStates_ignoresUnusedCompactionStates()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    insertCompactionStates(compactionStates);

    insertSegmentWithCompactionState("seg1", "fp1", true);

    // Mark compaction state as unused
    markCompactionStateAsUnused("fp1");

    List<CompactionStateRecord> records = read(SqlSegmentsMetadataQuery::retrieveAllUsedCompactionStates);

    Assert.assertTrue("Should not return unused compaction states", records.isEmpty());
  }

  @Test
  public void test_retrieveCompactionStatesForFingerprints_emptyInput()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    List<CompactionStateRecord> records = read(
        sql -> sql.retrieveCompactionStatesForFingerprints(Set.of())
    );

    Assert.assertTrue("Should return empty list for empty input", records.isEmpty());
  }

  @Test
  public void test_retrieveCompactionStatesForFingerprints_deltaSync()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    // Insert multiple compaction states
    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    compactionStates.put("fp2", createTestCompactionState());
    compactionStates.put("fp3", createTestCompactionState());
    insertCompactionStates(compactionStates);

    // Request specific fingerprints (delta sync scenario)
    List<CompactionStateRecord> records = read(
        sql -> sql.retrieveCompactionStatesForFingerprints(Set.of("fp1", "fp3"))
    );

    Assert.assertEquals("Should return requested fingerprints", 2, records.size());

    Set<String> retrievedFingerprints = records.stream()
                                                .map(CompactionStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assert.assertEquals("Should contain only requested fingerprints", Set.of("fp1", "fp3"), retrievedFingerprints);
  }

  @Test
  public void test_retrieveCompactionStatesForFingerprints_largeBatch()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    // Insert 150 compaction states (exceeds batching threshold of 100)
    Map<String, CompactionState> compactionStates = new HashMap<>();
    Set<String> expectedFingerprints = new HashSet<>();
    for (int i = 0; i < 150; i++) {
      String fingerprint = "fp" + i;
      compactionStates.put(fingerprint, createTestCompactionState());
      expectedFingerprints.add(fingerprint);
    }
    insertCompactionStates(compactionStates);

    // Request all fingerprints
    List<CompactionStateRecord> records = read(
        sql -> sql.retrieveCompactionStatesForFingerprints(expectedFingerprints)
    );

    Assert.assertEquals("Should return all fingerprints across multiple batches", 150, records.size());

    Set<String> retrievedFingerprints = records.stream()
                                                .map(CompactionStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assert.assertEquals("Should contain all requested fingerprints", expectedFingerprints, retrievedFingerprints);
  }

  @Test
  public void test_retrieveCompactionStatesForFingerprints_nonexistentFingerprints()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    insertCompactionStates(compactionStates);

    // Request fingerprints that don't exist
    List<CompactionStateRecord> records = read(
        sql -> sql.retrieveCompactionStatesForFingerprints(Set.of("fp999", "fp888"))
    );

    Assert.assertTrue("Should return empty list when fingerprints don't exist", records.isEmpty());
  }

  @Test
  public void test_retrieveCompactionStatesForFingerprints_mixedExistingAndNonexistent()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    compactionStates.put("fp2", createTestCompactionState());
    insertCompactionStates(compactionStates);

    // Mix existing and non-existing fingerprints
    List<CompactionStateRecord> records = read(
        sql -> sql.retrieveCompactionStatesForFingerprints(Set.of("fp1", "fp999", "fp2", "fp888"))
    );

    Assert.assertEquals("Should return only existing fingerprints", 2, records.size());

    Set<String> retrievedFingerprints = records.stream()
                                                .map(CompactionStateRecord::getFingerprint)
                                                .collect(Collectors.toSet());
    Assert.assertEquals("Should contain only existing fingerprints", Set.of("fp1", "fp2"), retrievedFingerprints);
  }

  @Test
  public void test_retrieveCompactionStatesForFingerprints_onlyReturnsUsedStates()
  {
    derbyConnectorRule.getConnector().createCompactionStatesTable();

    Map<String, CompactionState> compactionStates = new HashMap<>();
    compactionStates.put("fp1", createTestCompactionState());
    compactionStates.put("fp2", createTestCompactionState());
    insertCompactionStates(compactionStates);

    // Mark fp2 as unused
    markCompactionStateAsUnused("fp2");

    List<CompactionStateRecord> records = read(
        sql -> sql.retrieveCompactionStatesForFingerprints(Set.of("fp1", "fp2"))
    );

    Assert.assertEquals("Should only return used compaction states", 1, records.size());
    Assert.assertEquals("Should return fp1", "fp1", records.get(0).getFingerprint());
  }

  // ==================== Helper Methods for Compaction State Tests ====================

  private CompactionState createTestCompactionState()
  {
    return new CompactionState(
        new DynamicPartitionsSpec(100, null),
        DimensionsSpec.EMPTY,
        null,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
  }

  private void insertCompactionStates(Map<String, CompactionState> compactionStates)
  {
    ObjectMapper mapper = TestHelper.JSON_MAPPER;
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    SqlCompactionStateStorage manager = new SqlCompactionStateStorage(
        tablesConfig,
        mapper,
        derbyConnectorRule.getConnector()
    );

    derbyConnectorRule.getConnector().retryWithHandle(handle -> {
      for (Map.Entry<String, CompactionState> entry : compactionStates.entrySet()) {
        manager.upsertCompactionState(TestDataSource.WIKI, entry.getKey(), entry.getValue(), DateTimes.nowUtc());
      }
      return null;
    });
  }

  private void insertSegmentWithCompactionState(
      String segmentId,
      String compactionStateFingerprint,
      boolean used
  )
  {
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    DerbyConnector connector = derbyConnectorRule.getConnector();

    connector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getSegmentsTable() + " "
                + "(id, dataSource, created_date, start, \"end\", partitioned, version, used, payload, "
                + "used_status_last_updated, compaction_state_fingerprint) "
                + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, "
                + ":used_status_last_updated, :compaction_state_fingerprint)"
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
            .bind("compaction_state_fingerprint", compactionStateFingerprint)
            .execute();
      return null;
    });
  }

  private void markCompactionStateAsUnused(String fingerprint)
  {
    MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    DerbyConnector connector = derbyConnectorRule.getConnector();

    connector.retryWithHandle(handle -> {
      handle.createStatement(
                "UPDATE " + tablesConfig.getCompactionStatesTable() + " "
                + "SET used = false "
                + "WHERE fingerprint = :fingerprint"
            )
            .bind("fingerprint", fingerprint)
            .execute();
      return null;
    });
  }
}
