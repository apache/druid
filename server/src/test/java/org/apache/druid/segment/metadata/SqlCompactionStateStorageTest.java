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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.timeline.CompactionState;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlCompactionStateStorageTest
{
  @RegisterExtension
  public static final TestDerbyConnector.DerbyConnectorRule5 DERBY_CONNECTOR_RULE =
      new TestDerbyConnector.DerbyConnectorRule5();
  private static final ObjectMapper DETERMINISTIC_MAPPER = CompactionTestUtils.createDeterministicMapper();

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private static TestDerbyConnector derbyConnector;
  private static MetadataStorageTablesConfig tablesConfig;
  private SqlCompactionStateStorage manager;

  private static DefaultCompactionFingerprintMapper fingerprintMapper;

  @BeforeAll
  public static void setUpClass()
  {
    derbyConnector = DERBY_CONNECTOR_RULE.getConnector();
    tablesConfig = DERBY_CONNECTOR_RULE.metadataTablesConfigSupplier().get();
    derbyConnector.createCompactionStatesTable();
    derbyConnector.createSegmentTable();
    fingerprintMapper = new DefaultCompactionFingerprintMapper(
        new NoopCompactionStateCache(),
        DETERMINISTIC_MAPPER
    );
  }

  @BeforeEach
  public void setUp()
  {
    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement("DELETE FROM " + tablesConfig.getSegmentsTable()).execute();
      handle.createStatement("DELETE FROM " + tablesConfig.getCompactionStatesTable()).execute();
      return null;
    });

    manager = new SqlCompactionStateStorage(tablesConfig, jsonMapper, derbyConnector);
  }

  @Test
  public void test_upsertCompactionState_successfullyInsertsIntoDatabase()
  {
    CompactionState state1 = createTestCompactionState();
    String fingerprint = "fingerprint_abc123";

    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState(
          "testDatasource",
          fingerprint,
          state1,
          DateTimes.nowUtc()
      );
      return null;
    });

    // Verify the state was inserted into database by checking count
    Integer count = derbyConnector.retryWithHandle(handle ->
        handle.createQuery(
            "SELECT COUNT(*) FROM " + tablesConfig.getCompactionStatesTable()
            + " WHERE fingerprint = :fp"
        ).bind("fp", fingerprint)
         .map((i, r, ctx) -> r.getInt(1))
         .first()
    );
    assertEquals(1, count);
  }

  @Test
  public void test_upsertCompactionState_andThen_markUnreferencedCompactionStateAsUnused_andThen_markCompactionStatesAsUsed()
  {
    CompactionState state1 = createTestCompactionState();
    String fingerprint = "fingerprint_abc123";

    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState(
          "testDatasource",
          fingerprint,
          state1,
          DateTimes.nowUtc()
      );
      return null;
    });
    assertEquals(1, manager.markUnreferencedCompactionStatesAsUnused());
    assertEquals(1, manager.markCompactionStatesAsUsed(List.of(fingerprint)));
  }

  @Test
  public void test_findReferencedCompactionStateMarkedAsUnused()
  {
    CompactionState state1 = createTestCompactionState();
    String fingerprint = "fingerprint_abc123";

    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState(
          "testDatasource",
          fingerprint,
          state1,
          DateTimes.nowUtc()
      );
      return null;
    });
    manager.markUnreferencedCompactionStatesAsUnused();
    assertEquals(0, manager.findReferencedCompactionStateMarkedAsUnused().size());

    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getSegmentsTable() + " "
                + "(id, dataSource, created_date, start, \"end\", partitioned, version, used, payload, "
                + "used_status_last_updated, compaction_state_fingerprint) "
                + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, "
                + ":used_status_last_updated, :compaction_state_fingerprint)"
            )
            .bind("id", "testSegment_2024-01-01_2024-01-02_v1_0")
            .bind("dataSource", "testDatasource")
            .bind("created_date", DateTimes.nowUtc().toString())
            .bind("start", "2024-01-01T00:00:00.000Z")
            .bind("end", "2024-01-02T00:00:00.000Z")
            .bind("partitioned", 0)
            .bind("version", "v1")
            .bind("used", true)
            .bind("payload", new byte[]{})  // Empty payload is fine for this test
            .bind("used_status_last_updated", DateTimes.nowUtc().toString())
            .bind("compaction_state_fingerprint", fingerprint)
            .execute();
      return null;
    });

    List<String> referenced = manager.findReferencedCompactionStateMarkedAsUnused();
    assertEquals(1, referenced.size());
    assertEquals(fingerprint, referenced.get(0));
  }

  @Test
  public void test_deleteCompactionStatesOlderThan_deletesOnlyOldUnusedStates()
  {
    DateTime now = DateTimes.nowUtc();
    DateTime oldTime = now.minusDays(60);
    DateTime recentTime = now.minusDays(15);
    DateTime cutoffTime = now.minusDays(30);

    String oldFingerprint = "old_fp_should_delete";
    String recentFingerprint = "recent_fp_should_keep";

    CompactionState oldState = createTestCompactionState();
    CompactionState recentState = createTestCompactionState();

    // Insert old unused state (60 days old)
    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getCompactionStatesTable() + " "
                + "(created_date, dataSource, fingerprint, payload, used, pending, used_status_last_updated) "
                + "VALUES (:cd, :ds, :fp, :pl, :used, :pending, :updated)"
            )
            .bind("cd", oldTime.toString())
            .bind("ds", "testDatasource")
            .bind("fp", oldFingerprint)
            .bind("pl", jsonMapper.writeValueAsBytes(oldState))
            .bind("used", false)
            .bind("pending", false)
            .bind("updated", oldTime.toString())
            .execute();
      return null;
    });

    // Insert recent unused state (15 days old)
    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getCompactionStatesTable() + " "
                + "(created_date, dataSource, fingerprint, payload, used, pending, used_status_last_updated) "
                + "VALUES (:cd, :ds, :fp, :pl, :used, :pending, :updated)"
            )
            .bind("cd", recentTime.toString())
            .bind("ds", "testDatasource")
            .bind("fp", recentFingerprint)
            .bind("pl", jsonMapper.writeValueAsBytes(recentState))
            .bind("used", false)
            .bind("pending", false)
            .bind("updated", recentTime.toString())
            .execute();
      return null;
    });

    // Delete states older than 30 days
    int deleted = manager.deleteUnusedCompactionStatesOlderThan(cutoffTime.getMillis());
    assertEquals(1, deleted);

    // Verify only 1 state remains in the table
    Integer count = derbyConnector.retryWithHandle(handle ->
                                                       handle.createQuery("SELECT COUNT(*) FROM " + tablesConfig.getCompactionStatesTable())
                                                             .map((i, r, ctx) -> r.getInt(1))
                                                             .first()
    );
    assertEquals(1, count);
  }

  @Test
  public void test_upsertCompactionState_withNullState_throwsException()
  {
    Exception exception = assertThrows(
        Exception.class,
        () -> derbyConnector.retryWithHandle(handle -> {
          manager.upsertCompactionState("ds", "somePrint", null, DateTimes.nowUtc());
          return null;
        })
    );

    assertTrue(
        exception.getMessage().contains("compactionState cannot be null"),
        "Exception message should contain 'compactionState cannot be null'"
    );
  }

  @Test
  public void test_upsertCompactionState_withEmptyFingerprint_throwsException()
  {
    // The exception ends up wrapped in a sql exception doe to the retryWithHandle so we will just check the message
    Exception exception = assertThrows(
        Exception.class,
        () -> derbyConnector.retryWithHandle(handle -> {
          manager.upsertCompactionState("ds", "", createBasicCompactionState(), DateTimes.nowUtc());
          return null;
        })
    );

    assertTrue(
        exception.getMessage().contains("fingerprint cannot be empty"),
        "Exception message should contain 'fingerprint cannot be empty'"
    );
  }

  @Test
  public void test_upsertCompactionState_verifyExistingFingerprintMarkedUsed()
  {
    String fingerprint = "existing_fingerprint";
    CompactionState state = createTestCompactionState();

    // Persist initially
    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState("ds1", fingerprint, state, DateTimes.nowUtc());
      return null;
    });

    // Verify it's marked as used
    Boolean usedBefore = derbyConnector.retryWithHandle(handle ->
                                                            handle.createQuery(
                                                                      "SELECT used FROM " + tablesConfig.getCompactionStatesTable()
                                                                      + " WHERE fingerprint = :fp"
                                                                  ).bind("fp", fingerprint)
                                                                  .map((i, r, ctx) -> r.getBoolean("used"))
                                                                  .first()
    );
    assertTrue(usedBefore);

    // Manually mark it as unused
    derbyConnector.retryWithHandle(handle ->
                                       handle.createStatement(
                                           "UPDATE " + tablesConfig.getCompactionStatesTable()
                                           + " SET used = false WHERE fingerprint = :fp"
                                       ).bind("fp", fingerprint).execute()
    );

    // Persist again with the same fingerprint (should UPDATE, not INSERT)
    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState("ds1", fingerprint, state, DateTimes.nowUtc());
      return null;
    });

    // Verify it's marked as used again
    Boolean usedAfter = derbyConnector.retryWithHandle(handle ->
                                                           handle.createQuery(
                                                                     "SELECT used FROM " + tablesConfig.getCompactionStatesTable()
                                                                     + " WHERE fingerprint = :fp"
                                                                 ).bind("fp", fingerprint)
                                                                 .map((i, r, ctx) -> r.getBoolean("used"))
                                                                 .first()
    );
    assertTrue(usedAfter);

    // Verify only 1 row exists (no duplicate insert)
    Integer count = derbyConnector.retryWithHandle(handle ->
                                                       handle.createQuery("SELECT COUNT(*) FROM " + tablesConfig.getCompactionStatesTable())
                                                             .map((i, r, ctx) -> r.getInt(1))
                                                             .first()
    );
    assertEquals(1, count);
  }

  @Test
  public void test_upsertCompactionState_whenAlreadyUsed_skipsUpdate()
  {
    String fingerprint = "already_used_fingerprint";
    CompactionState state = createTestCompactionState();
    DateTime initialTime = DateTimes.of("2024-01-01T00:00:00.000Z");

    // Insert fingerprint as used initially
    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState("ds1", fingerprint, state, initialTime);
      return null;
    });

    // Verify it's marked as used with the initial timestamp
    DateTime usedStatusBeforeUpdate = derbyConnector.retryWithHandle(handle ->
                                                                         handle.createQuery(
                                                                                   "SELECT used_status_last_updated FROM " + tablesConfig.getCompactionStatesTable()
                                                                                   + " WHERE fingerprint = :fp"
                                                                               ).bind("fp", fingerprint)
                                                                               .map((i, r, ctx) -> DateTimes.of(r.getString("used_status_last_updated")))
                                                                               .first()
    );
    assertEquals(initialTime, usedStatusBeforeUpdate);

    // Call upsert again with a different timestamp
    // Since the fingerprint is already used, this should skip the UPDATE
    DateTime laterTime = DateTimes.of("2024-01-02T00:00:00.000Z");
    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState("ds1", fingerprint, state, laterTime);
      return null;
    });

    // Verify the used_status_last_updated timestamp DID NOT change
    DateTime usedStatusAfterUpdate = derbyConnector.retryWithHandle(handle ->
                                                                        handle.createQuery(
                                                                                  "SELECT used_status_last_updated FROM " + tablesConfig.getCompactionStatesTable()
                                                                                  + " WHERE fingerprint = :fp"
                                                                              ).bind("fp", fingerprint)
                                                                              .map((i, r, ctx) -> DateTimes.of(r.getString("used_status_last_updated")))
                                                                              .first()
    );

    assertEquals(
        initialTime,
        usedStatusAfterUpdate,
        "used_status_last_updated should not change when upserting an already-used fingerprint"
    );

    // Verify still only 1 row
    Integer count = derbyConnector.retryWithHandle(handle ->
                                                       handle.createQuery("SELECT COUNT(*) FROM " + tablesConfig.getCompactionStatesTable())
                                                             .map((i, r, ctx) -> r.getInt(1))
                                                             .first()
    );
    assertEquals(1, count);
  }

  @Test
  public void test_markCompactionStateAsUsed_withEmptyList_returnsZero()
  {
    assertEquals(0, manager.markCompactionStatesAsUsed(List.of()));
  }

  @Test
  public void test_markCompactionStatesAsActive_marksPendingStateAsActive()
  {
    String fingerprint = "pending_fingerprint";
    CompactionState state = createTestCompactionState();

    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState("ds1", fingerprint, state, DateTimes.nowUtc());
      return null;
    });

    Boolean pendingBefore = derbyConnector.retryWithHandle(handle ->
        handle.createQuery("SELECT pending FROM " + tablesConfig.getCompactionStatesTable() + " WHERE fingerprint = :fp")
              .bind("fp", fingerprint)
              .map((i, r, ctx) -> r.getBoolean("pending"))
              .first()
    );
    assertTrue(pendingBefore);

    int rowsUpdated = manager.markCompactionStatesAsActive(fingerprint);
    assertEquals(1, rowsUpdated);

    Boolean pendingAfter = derbyConnector.retryWithHandle(handle ->
        handle.createQuery("SELECT pending FROM " + tablesConfig.getCompactionStatesTable() + " WHERE fingerprint = :fp")
              .bind("fp", fingerprint)
              .map((i, r, ctx) -> r.getBoolean("pending"))
              .first()
    );
    assertEquals(false, pendingAfter);
  }

  @Test
  public void test_markCompactionStatesAsActive_idempotent_returnsZeroWhenAlreadyActive()
  {
    String fingerprint = "already_active_fingerprint";
    CompactionState state = createTestCompactionState();

    derbyConnector.retryWithHandle(handle -> {
      manager.upsertCompactionState("ds1", fingerprint, state, DateTimes.nowUtc());
      return null;
    });
    int firstUpdate = manager.markCompactionStatesAsActive(fingerprint);
    assertEquals(1, firstUpdate);

    int secondUpdate = manager.markCompactionStatesAsActive(fingerprint);
    assertEquals(0, secondUpdate);

    Boolean pending = derbyConnector.retryWithHandle(handle ->
        handle.createQuery("SELECT pending FROM " + tablesConfig.getCompactionStatesTable() + " WHERE fingerprint = :fp")
              .bind("fp", fingerprint)
              .map((i, r, ctx) -> r.getBoolean("pending"))
              .first()
    );
    assertEquals(false, pending);
  }

  @Test
  public void test_markCompactionStatesAsActive_nonExistentFingerprint_returnsZero()
  {
    int rowsUpdated = manager.markCompactionStatesAsActive("does_not_exist");
    assertEquals(0, rowsUpdated);
  }

  // ===== Fingerprint Generation Tests =====

  @Test
  public void test_generateCompactionStateFingerprint_deterministicFingerprinting()
  {
    CompactionState compactionState1 = createBasicCompactionState();
    CompactionState compactionState2 = createBasicCompactionState();

    String fingerprint1 = fingerprintMapper.generateFingerprint("test-ds", compactionState1);
    String fingerprint2 = fingerprintMapper.generateFingerprint("test-ds", compactionState2);

    assertEquals(
        fingerprint1,
        fingerprint2,
        "Same CompactionState should produce identical fingerprints when datasource is same"
    );
  }

  @Test
  public void test_generateCompactionStateFingerprint_differentDatasourcesWithSameState_differentFingerprints()
  {
    CompactionState compactionState = createBasicCompactionState();

    String fingerprint1 = fingerprintMapper.generateFingerprint("ds1", compactionState);
    String fingerprint2 = fingerprintMapper.generateFingerprint("ds2", compactionState);

    assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Different datasources should produce different fingerprints despite same state"
    );
  }

  @Test
  public void test_generateCompactionStateFingerprint_metricsListOrderDifferenceResultsInNewFingerprint()
  {
    List<AggregatorFactory> metrics1 = Arrays.asList(
        new CountAggregatorFactory("count"),
        new LongSumAggregatorFactory("sum", "value")
    );

    List<AggregatorFactory> metrics2 = Arrays.asList(
        new LongSumAggregatorFactory("sum", "value"),
        new CountAggregatorFactory("count")
    );

    CompactionState state1 = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        DimensionsSpec.EMPTY,
        metrics1,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    CompactionState state2 = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        DimensionsSpec.EMPTY,
        metrics2,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    String fingerprint1 = fingerprintMapper.generateFingerprint("test-ds", state1);
    String fingerprint2 = fingerprintMapper.generateFingerprint("test-ds", state2);

    assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Metrics order currently matters (arrays preserve order in JSON)"
    );
  }

  @Test
  public void test_generateCompactionStateFingerprint_dimensionsListOrderDifferenceResultsInNewFingerprint()
  {
    DimensionsSpec dimensions1 = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim1", "dim2", "dim3"))
    );

    DimensionsSpec dimensions2 = new DimensionsSpec(
        DimensionsSpec.getDefaultSchemas(ImmutableList.of("dim3", "dim2", "dim1"))
    );

    CompactionState state1 = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        dimensions1,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    CompactionState state2 = new CompactionState(
        new DynamicPartitionsSpec(null, null),
        dimensions2,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    String fingerprint1 = fingerprintMapper.generateFingerprint("test-ds", state1);
    String fingerprint2 = fingerprintMapper.generateFingerprint("test-ds", state2);

    assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Dimensions order currently matters (arrays preserve order in JSON)"
    );
  }

  @Test
  public void testGenerateCompactionStateFingerprint_differentPartitionsSpec()
  {
    CompactionState state1 = new CompactionState(
        new DynamicPartitionsSpec(5000000, null),
        DimensionsSpec.EMPTY,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    CompactionState state2 = new CompactionState(
        new HashedPartitionsSpec(null, 2, Collections.singletonList("dim1")),
        DimensionsSpec.EMPTY,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );

    String fingerprint1 = fingerprintMapper.generateFingerprint("test-ds", state1);
    String fingerprint2 = fingerprintMapper.generateFingerprint("test-ds", state2);

    assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Different PartitionsSpec should produce different fingerprints"
    );
  }

  private CompactionState createBasicCompactionState()
  {
    return new CompactionState(
        new DynamicPartitionsSpec(5000000, null),
        DimensionsSpec.EMPTY,
        Collections.singletonList(new CountAggregatorFactory("count")),
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
  }

  private CompactionState createTestCompactionState()
  {
    return new CompactionState(
        new DynamicPartitionsSpec(100, null),
        null,
        null,
        null,
        IndexSpec.getDefault(),
        null,
        null
    );
  }
}
