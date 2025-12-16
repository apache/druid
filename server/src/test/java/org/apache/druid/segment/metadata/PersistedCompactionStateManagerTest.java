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

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PersistedCompactionStateManagerTest
{
  @RegisterExtension
  public static final TestDerbyConnector.DerbyConnectorRule5 DERBY_CONNECTOR_RULE =
      new TestDerbyConnector.DerbyConnectorRule5();

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final ObjectMapper deterministicMapper = createDeterministicMapper();

  private static TestDerbyConnector derbyConnector;
  private static MetadataStorageTablesConfig tablesConfig;
  private PersistedCompactionStateManager manager;

  @BeforeAll
  public static void setUpClass()
  {
    derbyConnector = DERBY_CONNECTOR_RULE.getConnector();
    tablesConfig = DERBY_CONNECTOR_RULE.metadataTablesConfigSupplier().get();
    derbyConnector.createCompactionStatesTable();
    derbyConnector.createSegmentTable();
  }

  @BeforeEach
  public void setUp()
  {
    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement("DELETE FROM " + tablesConfig.getSegmentsTable()).execute();
      handle.createStatement("DELETE FROM " + tablesConfig.getCompactionStatesTable()).execute();
      return null;
    });

    manager = new PersistedCompactionStateManager(tablesConfig, jsonMapper, deterministicMapper, derbyConnector, new CompactionStateManagerConfig());
  }

  @Test
  public void test_persistCompactionState_andThen_getCompactionStateByFingerprint()
  {
    CompactionState state1 = createTestCompactionState();
    String fingerprint = "fingerprint_abc123";

    Map<String, CompactionState> fingerprintMap = new HashMap<>();
    fingerprintMap.put(fingerprint, state1);

    derbyConnector.retryWithHandle(handle -> {
      manager.persistCompactionState(
          "testDatasource",
          fingerprintMap,
          DateTimes.nowUtc()
      );
      return null;
    });

    assertTrue(manager.isCached(fingerprint));
    CompactionState retrieved = manager.getCompactionStateByFingerprint(fingerprint);
    assertNotNull(retrieved);
    assertEquals(state1, retrieved);
  }

  @Test
  public void test_persistCompactionState_andThen_confirmCached_andThen_invalidateCache_andThen_confirmNotCached()
  {
    String fingerprint = "cachemiss_fingerprint";
    CompactionState state = createTestCompactionState();

    derbyConnector.retryWithHandle(handle -> {
      Map<String, CompactionState> map = new HashMap<>();
      map.put(fingerprint, state);
      manager.persistCompactionState("ds1", map, DateTimes.nowUtc());
      return null;
    });

    assertTrue(manager.isCached(fingerprint));
    manager.invalidateFingerprint(fingerprint);
    assertFalse(manager.isCached(fingerprint));
    CompactionState result = manager.getCompactionStateByFingerprint(fingerprint);
    assertNotNull(result);
    assertEquals(state, result);
  }

  @Test
  public void test_persistCompactionState_andThen_markUnreferencedCompactionStateAsUnused_andThen_markCompactionStatesAsUsed()
  {
    CompactionState state1 = createTestCompactionState();
    String fingerprint = "fingerprint_abc123";

    Map<String, CompactionState> fingerprintMap = new HashMap<>();
    fingerprintMap.put(fingerprint, state1);

    derbyConnector.retryWithHandle(handle -> {
      manager.persistCompactionState(
          "testDatasource",
          fingerprintMap,
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

    Map<String, CompactionState> fingerprintMap = new HashMap<>();
    fingerprintMap.put(fingerprint, state1);

    derbyConnector.retryWithHandle(handle -> {
      manager.persistCompactionState(
          "testDatasource",
          fingerprintMap,
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
  public void test_deleteCompactionStatesOlderThan_deletesOnlyOldUnusedStates() throws Exception
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
                + "(created_date, datasource, fingerprint, payload, used, used_status_last_updated) "
                + "VALUES (:cd, :ds, :fp, :pl, :used, :updated)"
            )
            .bind("cd", oldTime.toString())
            .bind("ds", "testDatasource")
            .bind("fp", oldFingerprint)
            .bind("pl", jsonMapper.writeValueAsBytes(oldState))
            .bind("used", false)
            .bind("updated", oldTime.toString())
            .execute();
      return null;
    });

    // Insert recent unused state (15 days old)
    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getCompactionStatesTable() + " "
                + "(created_date, datasource, fingerprint, payload, used, used_status_last_updated) "
                + "VALUES (:cd, :ds, :fp, :pl, :used, :updated)"
            )
            .bind("cd", recentTime.toString())
            .bind("ds", "testDatasource")
            .bind("fp", recentFingerprint)
            .bind("pl", jsonMapper.writeValueAsBytes(recentState))
            .bind("used", false)
            .bind("updated", recentTime.toString())
            .execute();
      return null;
    });

    // Delete states older than 30 days
    int deleted = manager.deleteUnusedCompactionStatesOlderThan(cutoffTime.getMillis());
    assertEquals(1, deleted);

    // Verify the old one is gone
    CompactionState oldResult = manager.getCompactionStateByFingerprint(oldFingerprint);
    assertNull(oldResult);

    // Verify only 1 state remains in the table
    Integer count = derbyConnector.retryWithHandle(handle ->
                                                       handle.createQuery("SELECT COUNT(*) FROM " + tablesConfig.getCompactionStatesTable())
                                                             .map((i, r, ctx) -> r.getInt(1))
                                                             .first()
    );
    assertEquals(1, count);
  }

  @Test
  public void test_prewarmCache_onModuleLifecycleStart() throws Exception
  {
    String fingerprint = "prewarm_fingerprint";
    CompactionState state = createTestCompactionState();

    // Insert a used compaction state directly into the database
    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getCompactionStatesTable() + " "
                + "(created_date, datasource, fingerprint, payload, used, used_status_last_updated) "
                + "VALUES (:cd, :ds, :fp, :pl, :used, :updated)"
            )
            .bind("cd", DateTimes.nowUtc().toString())
            .bind("ds", "testDatasource")
            .bind("fp", fingerprint)
            .bind("pl", jsonMapper.writeValueAsBytes(state))
            .bind("used", true)  // Mark as used so it gets prewarmed
            .bind("updated", DateTimes.nowUtc().toString())
            .execute();
      return null;
    });

    // Create a NEW manager (not the shared one) - should prewarm cache in constructor
    PersistedCompactionStateManager newManager = new PersistedCompactionStateManager(
        tablesConfig,
        jsonMapper,
        deterministicMapper,
        derbyConnector,
        new CompactionStateManagerConfig()
    );
    newManager.start(); // normally handled by Guice during startup

    // Verify the state was prewarmed into cache
    assertTrue(newManager.isCached(fingerprint));

    // Verify we can retrieve it
    CompactionState retrieved = newManager.getCompactionStateByFingerprint(fingerprint);
    assertNotNull(retrieved);
    assertEquals(state, retrieved);
  }

  @Test
  public void test_persistCompactionState_withEmptyMap_doesNothing()
  {
    // Get initial count
    Integer beforeCount = derbyConnector.retryWithHandle(handle ->
                                                             handle.createQuery("SELECT COUNT(*) FROM " + tablesConfig.getCompactionStatesTable())
                                                                   .map((i, r, ctx) -> r.getInt(1))
                                                                   .first()
    );

    // Persist empty map
    derbyConnector.retryWithHandle(handle -> {
      manager.persistCompactionState("ds", new HashMap<>(), DateTimes.nowUtc());
      return null;
    });

    // Verify count unchanged
    Integer afterCount = derbyConnector.retryWithHandle(handle ->
                                                            handle.createQuery("SELECT COUNT(*) FROM " + tablesConfig.getCompactionStatesTable())
                                                                  .map((i, r, ctx) -> r.getInt(1))
                                                                  .first()
    );

    assertEquals(beforeCount, afterCount);
  }

  @Test
  public void test_getCompactionStateByFingerprint_notFound_returnsNull()
  {
    // Try to get a fingerprint that doesn't exist
    CompactionState result = manager.getCompactionStateByFingerprint("nonexistent_fingerprint");

    assertNull(result);

    // Verify it's not cached (shouldn't cache nulls)
    assertFalse(manager.isCached("nonexistent_fingerprint"));
  }

  @Test
  public void test_persistCompactionState_verifyExistingFingerprintMarkedUsed() throws Exception
  {
    String fingerprint = "existing_fingerprint";
    CompactionState state = createTestCompactionState();

    // Persist initially
    derbyConnector.retryWithHandle(handle -> {
      Map<String, CompactionState> map = new HashMap<>();
      map.put(fingerprint, state);
      manager.persistCompactionState("ds1", map, DateTimes.nowUtc());
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
      Map<String, CompactionState> map = new HashMap<>();
      map.put(fingerprint, state);
      manager.persistCompactionState("ds1", map, DateTimes.nowUtc());
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
  public void test_markCompactionStateAsUsed_withEmptyList_returnsZero()
  {
    assertEquals(0, manager.markCompactionStatesAsUsed(List.of()));
  }

  // ===== Fingerprint Generation Tests =====

  @Test
  public void test_generateCompactionStateFingerprint_deterministicFingerprinting()
  {
    CompactionState compactionState1 = createBasicCompactionState();
    CompactionState compactionState2 = createBasicCompactionState();

    String fingerprint1 = manager.generateCompactionStateFingerprint(compactionState1, "test-ds");
    String fingerprint2 = manager.generateCompactionStateFingerprint(compactionState2, "test-ds");

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

    String fingerprint1 = manager.generateCompactionStateFingerprint(compactionState, "ds1");
    String fingerprint2 = manager.generateCompactionStateFingerprint(compactionState, "ds2");

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

    String fingerprint1 = manager.generateCompactionStateFingerprint(state1, "test-ds");
    String fingerprint2 = manager.generateCompactionStateFingerprint(state2, "test-ds");

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

    String fingerprint1 = manager.generateCompactionStateFingerprint(state1, "test-ds");
    String fingerprint2 = manager.generateCompactionStateFingerprint(state2, "test-ds");

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

    String fingerprint1 = manager.generateCompactionStateFingerprint(state1, "test-ds");
    String fingerprint2 = manager.generateCompactionStateFingerprint(state2, "test-ds");

    assertNotEquals(
        fingerprint1,
        fingerprint2,
        "Different PartitionsSpec should produce different fingerprints"
    );
  }

  private static ObjectMapper createDeterministicMapper()
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    return mapper;
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
