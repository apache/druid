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

package org.apache.druid.server.audit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.TestDerbyConnector;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;


@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class SQLAuditManagerTest
{
  @RegisterExtension
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private SQLAuditManager auditManager;
  private StubServiceEmitter serviceEmitter;

  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final ObjectMapper mapperSkipNull
      = new DefaultObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @BeforeEach
  public void setUp()
  {
    serviceEmitter = new StubServiceEmitter("audit-test", "localhost");
    connector = derbyConnectorRule.getConnector();
    connector.createAuditTable();
    auditManager = createAuditManager(new SQLAuditManagerConfig(null, null, null, null, null));
  }

  private SQLAuditManager createAuditManager(SQLAuditManagerConfig config)
  {
    return new SQLAuditManager(
        config,
        new AuditSerdeHelper(config, null, mapper, mapperSkipNull),
        connector,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        serviceEmitter,
        mapper
    );
  }

  @Test
  public void testAuditMetricEventWithPayload() throws IOException
  {
    SQLAuditManager auditManager = createAuditManager(
        new SQLAuditManagerConfig(null, null, null, null, true)
    );

    final AuditEntry entry = createAuditEntry("testKey", "testType", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    List<ServiceMetricEvent> auditMetricEvents = serviceEmitter.getMetricEvents("config/audit");
    Assertions.assertEquals(1, auditMetricEvents.size());

    ServiceMetricEvent metric = auditMetricEvents.get(0);

    final AuditEntry dbEntry = lookupAuditEntryForKey("testKey");
    Assertions.assertNotNull(dbEntry);
    Assertions.assertEquals(dbEntry.getKey(), metric.getUserDims().get("key"));
    Assertions.assertEquals(dbEntry.getType(), metric.getUserDims().get("type"));
    Assertions.assertEquals(dbEntry.getPayload().serialized(), metric.getUserDims().get("payload"));
    Assertions.assertEquals(dbEntry.getAuditInfo().getAuthor(), metric.getUserDims().get("author"));
    Assertions.assertEquals(dbEntry.getAuditInfo().getComment(), metric.getUserDims().get("comment"));
    Assertions.assertEquals(dbEntry.getAuditInfo().getIp(), metric.getUserDims().get("remote_address"));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testCreateAuditEntry() throws IOException
  {
    final AuditEntry entry = createAuditEntry("key1", "type1", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assertions.assertEquals(entry, dbEntry);

    // Verify emitted metrics
    List<ServiceMetricEvent> auditMetricEvents
        = serviceEmitter.getMetricEvents("config/audit");
    Assertions.assertNotNull(auditMetricEvents);
    Assertions.assertEquals(1, auditMetricEvents.size());

    ServiceMetricEvent metric = auditMetricEvents.get(0);
    Assertions.assertEquals(dbEntry.getKey(), metric.getUserDims().get("key"));
    Assertions.assertEquals(dbEntry.getType(), metric.getUserDims().get("type"));
    Assertions.assertNull(metric.getUserDims().get("payload"));
    Assertions.assertEquals(dbEntry.getAuditInfo().getAuthor(), metric.getUserDims().get("author"));
    Assertions.assertEquals(dbEntry.getAuditInfo().getComment(), metric.getUserDims().get("comment"));
    Assertions.assertEquals(dbEntry.getAuditInfo().getIp(), metric.getUserDims().get("remote_address"));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testFetchAuditHistory()
  {
    final AuditEntry event = createAuditEntry("testKey", "testType", DateTimes.nowUtc());
    auditManager.doAudit(event);
    auditManager.doAudit(event);

    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(
        "testKey",
        "testType",
        Intervals.of("2000-01-01T00:00:00Z/2100-01-03T00:00:00Z")
    );

    Assertions.assertEquals(2, auditEntries.size());
    Assertions.assertEquals(event, auditEntries.get(0));
    Assertions.assertEquals(event, auditEntries.get(1));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testFetchAuditHistoryByKeyAndTypeWithLimit()
  {
    final AuditEntry entry1 = createAuditEntry("key1", "type1", DateTimes.nowUtc());
    final AuditEntry entry2 = createAuditEntry("key2", "type2", DateTimes.nowUtc());

    auditManager.doAudit(entry1);
    auditManager.doAudit(entry2);

    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(entry1.getKey(), entry1.getType(), 1);
    Assertions.assertEquals(1, auditEntries.size());
    Assertions.assertEquals(entry1, auditEntries.get(0));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testRemoveAuditLogsOlderThanWithEntryOlderThanTime() throws IOException
  {
    final AuditEntry entry = createAuditEntry("key1", "type1", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assertions.assertEquals(entry, dbEntry);

    // Verify that the audit entry gets deleted
    auditManager.removeAuditLogsOlderThan(System.currentTimeMillis());
    Assertions.assertNull(lookupAuditEntryForKey(entry.getKey()));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testRemoveAuditLogsOlderThanWithEntryNotOlderThanTime() throws IOException
  {
    AuditEntry entry = createAuditEntry("key", "type", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assertions.assertEquals(entry, dbEntry);

    // Delete old audit logs
    auditManager.removeAuditLogsOlderThan(DateTimes.of("2012-01-01T00:00:00Z").getMillis());

    dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assertions.assertEquals(entry, dbEntry);
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testFetchAuditHistoryByTypeWithLimit()
  {
    final AuditEntry entry1 = createAuditEntry("testKey", "testType", DateTimes.of("2022-01"));
    final AuditEntry entry2 = createAuditEntry("testKey", "testType", DateTimes.of("2022-03"));
    final AuditEntry entry3 = createAuditEntry("testKey", "testType", DateTimes.of("2022-02"));

    auditManager.doAudit(entry1);
    auditManager.doAudit(entry2);
    auditManager.doAudit(entry3);

    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory("testType", 2);
    Assertions.assertEquals(2, auditEntries.size());
    Assertions.assertEquals(entry2, auditEntries.get(0));
    Assertions.assertEquals(entry3, auditEntries.get(1));
  }

  @Test
  @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
  public void testFetchAuditHistoryLimitBelowZero()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
      auditManager.fetchAuditHistory("testType", -1));
  }

  @Test
  @Timeout(value = 10_000L, unit = TimeUnit.MILLISECONDS)
  public void testFetchAuditHistoryLimitZero()
  {
    org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
      auditManager.fetchAuditHistory("testType", 0));
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testCreateAuditEntryWithPayloadOverSkipPayloadLimit() throws IOException
  {
    final SQLAuditManager auditManager = createAuditManager(
        new SQLAuditManagerConfig(null, HumanReadableBytes.valueOf(10), null, null, null)
    );

    final AuditEntry entry = createAuditEntry("key", "type", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    // Verify that all the fields are the same except for the payload
    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assertions.assertEquals(entry.getKey(), dbEntry.getKey());
    // Assert.assertNotEquals(entry.getPayload(), dbEntry.getPayload());
    Assertions.assertEquals(
        "Payload truncated as it exceeds 'druid.audit.manager.maxPayloadSizeBytes'[10].",
        dbEntry.getPayload().serialized()
    );
    Assertions.assertEquals(entry.getType(), dbEntry.getType());
    Assertions.assertEquals(entry.getAuditInfo(), dbEntry.getAuditInfo());
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testCreateAuditEntryWithPayloadUnderSkipPayloadLimit() throws IOException
  {
    SQLAuditManager auditManager = createAuditManager(
        new SQLAuditManagerConfig(null, HumanReadableBytes.valueOf(500), null, null, null)
    );

    final AuditEntry entry = createAuditEntry("key", "type", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    // Verify that the actual payload has been persisted
    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assertions.assertEquals(entry, dbEntry);
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
  public void testCreateAuditEntryWithSkipNullsInPayload() throws IOException
  {
    final SQLAuditManager auditManagerSkipNull = createAuditManager(
        new SQLAuditManagerConfig(null, null, true, null, null)
    );

    AuditInfo auditInfo = new AuditInfo("testAuthor", "testIdentity", "testComment", "127.0.0.1");

    final Map<String, String> payloadMap = new TreeMap<>();
    payloadMap.put("version", "x");
    payloadMap.put("something", null);

    auditManager.doAudit(
        AuditEntry.builder().key("key1").type("type1").auditInfo(auditInfo).payload(payloadMap).build()
    );
    AuditEntry entryWithNulls = lookupAuditEntryForKey("key1");
    Assertions.assertEquals("{\"something\":null,\"version\":\"x\"}", entryWithNulls.getPayload().serialized());

    auditManagerSkipNull.doAudit(
        AuditEntry.builder().key("key2").type("type2").auditInfo(auditInfo).payload(payloadMap).build()
    );
    AuditEntry entryWithoutNulls = lookupAuditEntryForKey("key2");
    Assertions.assertEquals("{\"version\":\"x\"}", entryWithoutNulls.getPayload().serialized());
  }

  @AfterEach
  public void cleanup()
  {
    dropTable(derbyConnectorRule.metadataTablesConfigSupplier().get().getAuditTable());
  }

  private void dropTable(final String tableName)
  {
    int rowsAffected = connector.getDBI().withHandle(
        handle -> handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                        .execute()
    );
    Assertions.assertEquals(0, rowsAffected);
  }

  private AuditEntry lookupAuditEntryForKey(String key) throws IOException
  {
    byte[] payload = connector.lookup(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getAuditTable(),
        "audit_key",
        "payload",
        key
    );

    if (payload == null) {
      return null;
    } else {
      return mapper.readValue(payload, AuditEntry.class);
    }
  }

  private AuditEntry createAuditEntry(String key, String type, DateTime auditTime)
  {
    return AuditEntry.builder()
                     .key(key)
                     .type(type)
                     .serializedPayload(StringUtils.format("Test payload for key[%s], type[%s]", key, type))
                     .auditInfo(new AuditInfo("author", "identity", "comment", "127.0.0.1"))
                     .auditTime(auditTime)
                     .build();
  }
}
