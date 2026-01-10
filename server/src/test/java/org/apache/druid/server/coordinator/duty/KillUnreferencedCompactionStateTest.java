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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.metadata.CompactionStateStorage;
import org.apache.druid.segment.metadata.SqlCompactionStateStorage;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.CompactionState;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class KillUnreferencedCompactionStateTest
{
  @RegisterExtension
  public static final TestDerbyConnector.DerbyConnectorRule5 DERBY_CONNECTOR_RULE =
      new TestDerbyConnector.DerbyConnectorRule5();

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private TestDerbyConnector derbyConnector;
  private MetadataStorageTablesConfig tablesConfig;
  private CompactionStateStorage compactionStateStorage;
  private DruidCoordinatorRuntimeParams mockParams;

  @BeforeEach
  public void setUp()
  {
    derbyConnector = DERBY_CONNECTOR_RULE.getConnector();
    tablesConfig = DERBY_CONNECTOR_RULE.metadataTablesConfigSupplier().get();

    derbyConnector.createCompactionStatesTable();
    derbyConnector.createSegmentTable();

    compactionStateStorage = new SqlCompactionStateStorage(tablesConfig, jsonMapper, createDeterministicMapper(), derbyConnector);

    mockParams = EasyMock.createMock(DruidCoordinatorRuntimeParams.class);
    CoordinatorRunStats runStats = new CoordinatorRunStats();
    EasyMock.expect(mockParams.getCoordinatorStats()).andReturn(runStats).anyTimes();
    EasyMock.replay(mockParams);
  }

  @Test
  public void testKillUnreferencedCompactionState_lifecycle()
  {
    // Setup time progression: now, +1hr, +7hrs (past cleanup period and retention)
    List<DateTime> dateTimes = new ArrayList<>();
    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);                         // Run 1: Mark as unused
    dateTimes.add(now.plusMinutes(61));         // Run 2: Still in retention period
    dateTimes.add(now.plusMinutes(6 * 60 + 1)); // Run 3: Past retention, delete

    MetadataCleanupConfig cleanupConfig = new MetadataCleanupConfig(
        true,
        Period.parse("PT1H").toStandardDuration(),  // cleanup period
        Period.parse("PT6H").toStandardDuration()   // retention duration
    );

    KillUnreferencedCompactionState duty =
        new TestKillUnreferencedCompactionState(cleanupConfig, compactionStateStorage, dateTimes);

    // Insert a compaction state (initially marked as used)
    String fingerprint = "test_fingerprint";
    CompactionState state = createTestCompactionState();

    derbyConnector.retryWithHandle(handle -> {
      compactionStateStorage.upsertCompactionState("test-ds", fingerprint, state, DateTimes.nowUtc());
      return null;
    });

    assertEquals(Boolean.TRUE, getCompactionStateUsedStatus(fingerprint));

    // Run 1: Should mark as unused (no segments reference it)
    duty.run(mockParams);
    assertEquals(Boolean.FALSE, getCompactionStateUsedStatus(fingerprint));

    // Run 2: Still unused, but within retention period - should not delete
    duty.run(mockParams);
    assertNotNull(getCompactionStateUsedStatus(fingerprint));

    // Run 3: Past retention period - should delete
    duty.run(mockParams);
    assertNull(getCompactionStateUsedStatus(fingerprint));
  }

  @Test
  public void testKillUnreferencedCompactionState_repair()
  {
    List<DateTime> dateTimes = new ArrayList<>();
    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));

    MetadataCleanupConfig cleanupConfig = new MetadataCleanupConfig(
        true,
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("PT6H").toStandardDuration()
    );

    KillUnreferencedCompactionState duty =
        new TestKillUnreferencedCompactionState(cleanupConfig, compactionStateStorage, dateTimes);

    // Insert compaction state
    String fingerprint = "repair_fingerprint";
    CompactionState state = createTestCompactionState();

    derbyConnector.retryWithHandle(handle -> {
      compactionStateStorage.upsertCompactionState("test-ds", fingerprint, state, DateTimes.nowUtc());
      return null;
    });

    // Run 1: Mark as unused
    duty.run(mockParams);
    assertEquals(Boolean.FALSE, getCompactionStateUsedStatus(fingerprint));

    // Now insert a used segment that references this fingerprint
    derbyConnector.retryWithHandle(handle -> {
      handle.createStatement(
                "INSERT INTO " + tablesConfig.getSegmentsTable() + " "
                + "(id, dataSource, created_date, start, \"end\", partitioned, version, used, payload, "
                + "used_status_last_updated, compaction_state_fingerprint) "
                + "VALUES (:id, :dataSource, :created_date, :start, :end, :partitioned, :version, :used, :payload, "
                + ":used_status_last_updated, :compaction_state_fingerprint)"
            )
            .bind("id", "testSegment_2024-01-01_2024-01-02_v1_0")
            .bind("dataSource", "test-ds")
            .bind("created_date", DateTimes.nowUtc().toString())
            .bind("start", "2024-01-01T00:00:00.000Z")
            .bind("end", "2024-01-02T00:00:00.000Z")
            .bind("partitioned", 0)
            .bind("version", "v1")
            .bind("used", true)
            .bind("payload", new byte[]{})
            .bind("used_status_last_updated", DateTimes.nowUtc().toString())
            .bind("compaction_state_fingerprint", fingerprint)
            .execute();
      return null;
    });

    // Run 2: Repair - should mark it back as used
    duty.run(mockParams);
    assertEquals(Boolean.TRUE, getCompactionStateUsedStatus(fingerprint));
  }

  @Test
  public void testKillUnreferencedCompactionState_disabled()
  {
    MetadataCleanupConfig cleanupConfig = new MetadataCleanupConfig(
        false, // disabled
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("PT6H").toStandardDuration()
    );

    KillUnreferencedCompactionState duty =
        new KillUnreferencedCompactionState(cleanupConfig, compactionStateStorage);

    // Insert compaction state
    String fingerprint = "disabled_fingerprint";
    derbyConnector.retryWithHandle(handle -> {
      compactionStateStorage.upsertCompactionState("test-ds", fingerprint, createTestCompactionState(), DateTimes.nowUtc());
      return null;
    });

    // Run duty - should do nothing
    duty.run(mockParams);

    // Should still be used (not marked as unused)
    assertEquals(Boolean.TRUE, getCompactionStateUsedStatus(fingerprint));
  }

  private static class TestKillUnreferencedCompactionState extends KillUnreferencedCompactionState
  {
    private final List<DateTime> dateTimes;
    private int index = -1;

    public TestKillUnreferencedCompactionState(
        MetadataCleanupConfig config,
        CompactionStateStorage compactionStateStorage,
        List<DateTime> dateTimes
    )
    {
      super(config, compactionStateStorage);
      this.dateTimes = dateTimes;
    }

    @Override
    protected DateTime getCurrentTime()
    {
      index++;
      return dateTimes.get(index);
    }
  }

  private CompactionState createTestCompactionState()
  {
    return new CompactionState(
        new DynamicPartitionsSpec(100, null),
        null, null, null,
        IndexSpec.getDefault(),
        null, null
    );
  }

  private Boolean getCompactionStateUsedStatus(String fingerprint)
  {
    List<Boolean> usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
                            "SELECT used FROM " + tablesConfig.getCompactionStatesTable()
                            + " WHERE fingerprint = :fp"
                        )
                        .bind("fp", fingerprint)
                        .mapTo(Boolean.class)
                        .list()
    );

    return usedStatus.isEmpty() ? null : usedStatus.get(0);
  }

  private static ObjectMapper createDeterministicMapper()
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    return mapper;
  }
}
