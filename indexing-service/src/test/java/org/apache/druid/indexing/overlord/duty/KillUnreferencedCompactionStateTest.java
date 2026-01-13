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

package org.apache.druid.indexing.overlord.duty;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.overlord.config.OverlordMetadataCleanupConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.metadata.CompactionStateStorage;
import org.apache.druid.segment.metadata.SqlCompactionStateStorage;
import org.apache.druid.timeline.CompactionState;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class KillUnreferencedCompactionStateTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule();

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private TestDerbyConnector derbyConnector;
  private MetadataStorageTablesConfig tablesConfig;
  private SqlCompactionStateStorage compactionStateStorage;

  @Before
  public void setUp()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    derbyConnector.createCompactionStatesTable();
    derbyConnector.createSegmentTable();

    compactionStateStorage = new SqlCompactionStateStorage(tablesConfig, jsonMapper, derbyConnector);
  }

  @Test
  public void test_killUnreferencedCompactionState_validateLifecycleOfActiveCompactionState()
  {
    // Setup time progression: now, +1hr, +7hrs
    List<DateTime> dateTimes = new ArrayList<>();
    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));
    dateTimes.add(now.plusMinutes(6 * 60 + 1));

    OverlordMetadataCleanupConfig cleanupConfig = new OverlordMetadataCleanupConfig(
        true,
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("PT6H").toStandardDuration(), // Unused and over 6 hours old should be deleted
        Period.parse("P8D").toStandardDuration()
    );

    KillUnreferencedCompactionState duty =
        new TestKillUnreferencedCompactionState(cleanupConfig, compactionStateStorage, dateTimes);

    String fingerprint = "test_fingerprint";
    CompactionState state = createTestCompactionState();

    compactionStateStorage.upsertCompactionState("test-ds", fingerprint, state, DateTimes.nowUtc());
    compactionStateStorage.markCompactionStatesAsActive(fingerprint);

    Assert.assertEquals(Boolean.TRUE, getCompactionStateUsedStatus(fingerprint));

    // Run 1: Should mark as unused (no segments reference it)
    duty.run();
    Assert.assertEquals(Boolean.FALSE, getCompactionStateUsedStatus(fingerprint));

    // Run 2: Still unused, but within retention period - should not delete
    duty.run();
    Assert.assertNotNull(getCompactionStateUsedStatus(fingerprint));

    // Run 3: Past retention period - should delete
    duty.run();
    Assert.assertNull(getCompactionStateUsedStatus(fingerprint));
  }

  @Test
  public void test_killUnreferencedCompactionState_validateRepair()
  {
    List<DateTime> dateTimes = new ArrayList<>();
    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));

    OverlordMetadataCleanupConfig cleanupConfig = new OverlordMetadataCleanupConfig(
        true,
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("PT6H").toStandardDuration(),
        Period.parse("P8D").toStandardDuration()
    );

    KillUnreferencedCompactionState duty =
        new TestKillUnreferencedCompactionState(cleanupConfig, compactionStateStorage, dateTimes);

    // Insert compaction state
    String fingerprint = "repair_fingerprint";
    CompactionState state = createTestCompactionState();

    compactionStateStorage.upsertCompactionState("test-ds", fingerprint, state, DateTimes.nowUtc());
    compactionStateStorage.markCompactionStatesAsActive(fingerprint);

    Assert.assertEquals(Boolean.TRUE, getCompactionStateUsedStatus(fingerprint));
    duty.run();
    Assert.assertEquals(Boolean.FALSE, getCompactionStateUsedStatus(fingerprint));

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

    // Confirm that the state is "repaired" now that it is referenced
    duty.run();
    Assert.assertEquals(Boolean.TRUE, getCompactionStateUsedStatus(fingerprint));
  }

  @Test
  public void test_killUnreferencedCompactionState_disabled()
  {
    OverlordMetadataCleanupConfig cleanupConfig = new OverlordMetadataCleanupConfig(
        false, // cleanup disabled
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("PT6H").toStandardDuration(),
        Period.parse("P8D").toStandardDuration()
    );

    KillUnreferencedCompactionState duty =
        new KillUnreferencedCompactionState(cleanupConfig, compactionStateStorage);

    // Insert compaction state
    String fingerprint = "disabled_fingerprint";
    compactionStateStorage.upsertCompactionState("test-ds", fingerprint, createTestCompactionState(), DateTimes.nowUtc());
    compactionStateStorage.markCompactionStatesAsActive(fingerprint);

    // Run duty - should do nothing
    duty.run();

    // Should still be used (not marked as unused since cleanup is disabled)
    Assert.assertEquals(Boolean.TRUE, getCompactionStateUsedStatus(fingerprint));
  }

  @Test
  public void test_killUnreferencedCompactionState_validateLifecycleOfPendingCompactionState()
  {
    List<DateTime> dateTimes = new ArrayList<>();
    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now.plusDays(8));
    dateTimes.add(now.plusDays(15));

    OverlordMetadataCleanupConfig cleanupConfig = new OverlordMetadataCleanupConfig(
        true,
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("P7D").toStandardDuration(),
        Period.parse("P10D").toStandardDuration() // Pending states older than 10 days should be deleted
    );

    KillUnreferencedCompactionState duty =
        new TestKillUnreferencedCompactionState(cleanupConfig, compactionStateStorage, dateTimes);

    String fingerprint = "pending_fingerprint";
    CompactionState state = createTestCompactionState();
    compactionStateStorage.upsertCompactionState("test-ds", fingerprint, state, DateTimes.nowUtc());

    Assert.assertEquals(Boolean.TRUE, compactionStateStorage.isCompactionStatePending(fingerprint));

    duty.run();
    Assert.assertNotNull(compactionStateStorage.isCompactionStatePending(fingerprint));

    duty.run();
    Assert.assertNull(compactionStateStorage.isCompactionStatePending(fingerprint));
  }

  /**
   * Validate multiple states cleaned up as per their individual retention policies.
   */
  @Test
  public void test_killUnreferencedCompactionState_validateMixedPendingAndActiveCompactionStateCleanup()
  {
    List<DateTime> dateTimes = new ArrayList<>();
    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now.plusDays(8));
    dateTimes.add(now.plusDays(31));

    OverlordMetadataCleanupConfig cleanupConfig = new OverlordMetadataCleanupConfig(
        true,
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("P7D").toStandardDuration(),
        Period.parse("P30D").toStandardDuration()
    );

    KillUnreferencedCompactionState duty =
        new TestKillUnreferencedCompactionState(cleanupConfig, compactionStateStorage, dateTimes);

    String pendingFingerprint = "pending_fp";
    String nonPendingFingerprint = "non_pending_fp";
    CompactionState state = createTestCompactionState();

    compactionStateStorage.upsertCompactionState("test-ds", pendingFingerprint, state, DateTimes.nowUtc());
    compactionStateStorage.upsertCompactionState("test-ds", nonPendingFingerprint, state, DateTimes.nowUtc());
    compactionStateStorage.markCompactionStatesAsActive(nonPendingFingerprint);

    Assert.assertEquals(Boolean.TRUE, compactionStateStorage.isCompactionStatePending(pendingFingerprint));
    Assert.assertNotNull(getCompactionStateUsedStatus(nonPendingFingerprint));

    duty.run();
    Assert.assertNotNull(compactionStateStorage.isCompactionStatePending(pendingFingerprint));
    Assert.assertNull(getCompactionStateUsedStatus(nonPendingFingerprint));

    duty.run();
    Assert.assertNull(getCompactionStateUsedStatus(nonPendingFingerprint));
    Assert.assertNull(compactionStateStorage.isCompactionStatePending(pendingFingerprint));
  }

  @Test
  public void test_killUnreferencedCompactionState_pendingStateMarkedActiveNotDeleted()
  {
    List<DateTime> dateTimes = new ArrayList<>();
    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now.plusDays(31)); // The state would be removed if it was still pending

    OverlordMetadataCleanupConfig cleanupConfig = new OverlordMetadataCleanupConfig(
        true,
        Period.parse("PT1H").toStandardDuration(),
        Period.parse("P7D").toStandardDuration(),
        Period.parse("P30D").toStandardDuration()
    );

    KillUnreferencedCompactionState duty =
        new TestKillUnreferencedCompactionState(cleanupConfig, compactionStateStorage, dateTimes);

    String fingerprint = "pending_marked_active_fp";
    CompactionState state = createTestCompactionState();

    compactionStateStorage.upsertCompactionState("test-ds", fingerprint, state, DateTimes.nowUtc());
    Assert.assertEquals(Boolean.TRUE, compactionStateStorage.isCompactionStatePending(fingerprint));

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

    compactionStateStorage.markCompactionStatesAsActive(fingerprint);
    Assert.assertNotEquals(Boolean.TRUE, compactionStateStorage.isCompactionStatePending(fingerprint));

    duty.run();
    Assert.assertNotNull(compactionStateStorage.isCompactionStatePending(fingerprint));
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

  /**
   * Extension of KillUnreferencedCompactionState that allows controlling the reference time used for cleanup decisions.
   * <p>
   * Allowing time control enables realistic testing of time-based retention logic.
   */
  private static class TestKillUnreferencedCompactionState extends KillUnreferencedCompactionState
  {
    private final List<DateTime> dateTimes;
    private int index = -1;

    public TestKillUnreferencedCompactionState(
        OverlordMetadataCleanupConfig config,
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
}
