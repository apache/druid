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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@RunWith(MockitoJUnitRunner.class)
public class SQLAuditManagerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule
      = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private SQLAuditManager auditManager;
  private StubServiceEmitter serviceEmitter;

  private final ObjectMapper mapper = new DefaultObjectMapper();
  private final ObjectMapper mapperSkipNull
      = new DefaultObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @Before
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

    Map<String, List<ServiceMetricEvent>> metricEvents = serviceEmitter.getMetricEvents();
    Assert.assertEquals(1, metricEvents.size());

    List<ServiceMetricEvent> auditMetricEvents = metricEvents.get("config/audit");
    Assert.assertNotNull(auditMetricEvents);
    Assert.assertEquals(1, auditMetricEvents.size());

    ServiceMetricEvent metric = auditMetricEvents.get(0);

    final AuditEntry dbEntry = lookupAuditEntryForKey("testKey");
    Assert.assertNotNull(dbEntry);
    Assert.assertEquals(dbEntry.getKey(), metric.getUserDims().get("key"));
    Assert.assertEquals(dbEntry.getType(), metric.getUserDims().get("type"));
    Assert.assertEquals(dbEntry.getPayload().serialized(), metric.getUserDims().get("payload"));
    Assert.assertEquals(dbEntry.getAuditInfo().getAuthor(), metric.getUserDims().get("author"));
    Assert.assertEquals(dbEntry.getAuditInfo().getComment(), metric.getUserDims().get("comment"));
    Assert.assertEquals(dbEntry.getAuditInfo().getIp(), metric.getUserDims().get("remote_address"));
  }

  @Test(timeout = 60_000L)
  public void testCreateAuditEntry() throws IOException
  {
    final AuditEntry entry = createAuditEntry("key1", "type1", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assert.assertEquals(entry, dbEntry);

    // Verify emitted metrics
    Map<String, List<ServiceMetricEvent>> metricEvents = serviceEmitter.getMetricEvents();
    Assert.assertEquals(1, metricEvents.size());

    List<ServiceMetricEvent> auditMetricEvents = metricEvents.get("config/audit");
    Assert.assertNotNull(auditMetricEvents);
    Assert.assertEquals(1, auditMetricEvents.size());

    ServiceMetricEvent metric = auditMetricEvents.get(0);
    Assert.assertEquals(dbEntry.getKey(), metric.getUserDims().get("key"));
    Assert.assertEquals(dbEntry.getType(), metric.getUserDims().get("type"));
    Assert.assertNull(metric.getUserDims().get("payload"));
    Assert.assertEquals(dbEntry.getAuditInfo().getAuthor(), metric.getUserDims().get("author"));
    Assert.assertEquals(dbEntry.getAuditInfo().getComment(), metric.getUserDims().get("comment"));
    Assert.assertEquals(dbEntry.getAuditInfo().getIp(), metric.getUserDims().get("remote_address"));
  }

  @Test(timeout = 60_000L)
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

    Assert.assertEquals(2, auditEntries.size());
    Assert.assertEquals(event, auditEntries.get(0));
    Assert.assertEquals(event, auditEntries.get(1));
  }

  @Test(timeout = 60_000L)
  public void testFetchAuditHistoryByKeyAndTypeWithLimit()
  {
    final AuditEntry entry1 = createAuditEntry("key1", "type1", DateTimes.nowUtc());
    final AuditEntry entry2 = createAuditEntry("key2", "type2", DateTimes.nowUtc());

    auditManager.doAudit(entry1);
    auditManager.doAudit(entry2);

    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(entry1.getKey(), entry1.getType(), 1);
    Assert.assertEquals(1, auditEntries.size());
    Assert.assertEquals(entry1, auditEntries.get(0));
  }

  @Test(timeout = 60_000L)
  public void testRemoveAuditLogsOlderThanWithEntryOlderThanTime() throws IOException
  {
    final AuditEntry entry = createAuditEntry("key1", "type1", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assert.assertEquals(entry, dbEntry);

    // Verify that the audit entry gets deleted
    auditManager.removeAuditLogsOlderThan(System.currentTimeMillis());
    Assert.assertNull(lookupAuditEntryForKey(entry.getKey()));
  }

  @Test(timeout = 60_000L)
  public void testRemoveAuditLogsOlderThanWithEntryNotOlderThanTime() throws IOException
  {
    AuditEntry entry = createAuditEntry("key", "type", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assert.assertEquals(entry, dbEntry);

    // Delete old audit logs
    auditManager.removeAuditLogsOlderThan(DateTimes.of("2012-01-01T00:00:00Z").getMillis());

    dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assert.assertEquals(entry, dbEntry);
  }

  @Test(timeout = 60_000L)
  public void testFetchAuditHistoryByTypeWithLimit()
  {
    final AuditEntry entry1 = createAuditEntry("testKey", "testType", DateTimes.of("2022-01"));
    final AuditEntry entry2 = createAuditEntry("testKey", "testType", DateTimes.of("2022-03"));
    final AuditEntry entry3 = createAuditEntry("testKey", "testType", DateTimes.of("2022-02"));

    auditManager.doAudit(entry1);
    auditManager.doAudit(entry2);
    auditManager.doAudit(entry3);

    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory("testType", 2);
    Assert.assertEquals(2, auditEntries.size());
    Assert.assertEquals(entry2, auditEntries.get(0));
    Assert.assertEquals(entry3, auditEntries.get(1));
  }

  @Test(expected = IllegalArgumentException.class, timeout = 10_000L)
  public void testFetchAuditHistoryLimitBelowZero()
  {
    auditManager.fetchAuditHistory("testType", -1);
  }

  @Test(expected = IllegalArgumentException.class, timeout = 10_000L)
  public void testFetchAuditHistoryLimitZero()
  {
    auditManager.fetchAuditHistory("testType", 0);
  }

  @Test(timeout = 60_000L)
  public void testCreateAuditEntryWithPayloadOverSkipPayloadLimit() throws IOException
  {
    final SQLAuditManager auditManager = createAuditManager(
        new SQLAuditManagerConfig(null, HumanReadableBytes.valueOf(10), null, null, null)
    );

    final AuditEntry entry = createAuditEntry("key", "type", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    // Verify that all the fields are the same except for the payload
    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assert.assertEquals(entry.getKey(), dbEntry.getKey());
    // Assert.assertNotEquals(entry.getPayload(), dbEntry.getPayload());
    Assert.assertEquals(
        "Payload truncated as it exceeds 'druid.audit.manager.maxPayloadSizeBytes'[10].",
        dbEntry.getPayload().serialized()
    );
    Assert.assertEquals(entry.getType(), dbEntry.getType());
    Assert.assertEquals(entry.getAuditInfo(), dbEntry.getAuditInfo());
  }

  @Test(timeout = 60_000L)
  public void testCreateAuditEntryWithPayloadUnderSkipPayloadLimit() throws IOException
  {
    SQLAuditManager auditManager = createAuditManager(
        new SQLAuditManagerConfig(null, HumanReadableBytes.valueOf(500), null, null, null)
    );

    final AuditEntry entry = createAuditEntry("key", "type", DateTimes.nowUtc());
    auditManager.doAudit(entry);

    // Verify that the actual payload has been persisted
    AuditEntry dbEntry = lookupAuditEntryForKey(entry.getKey());
    Assert.assertEquals(entry, dbEntry);
  }

  @Test(timeout = 60_000L)
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
    Assert.assertEquals("{\"something\":null,\"version\":\"x\"}", entryWithNulls.getPayload().serialized());

    auditManagerSkipNull.doAudit(
        AuditEntry.builder().key("key2").type("type2").auditInfo(auditInfo).payload(payloadMap).build()
    );
    AuditEntry entryWithoutNulls = lookupAuditEntryForKey("key2");
    Assert.assertEquals("{\"version\":\"x\"}", entryWithoutNulls.getPayload().serialized());
  }

  @After
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
    Assert.assertEquals(0, rowsAffected);
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
