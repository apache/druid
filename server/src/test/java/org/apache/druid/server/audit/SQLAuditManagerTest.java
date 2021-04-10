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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigSerde;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class SQLAuditManagerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private AuditManager auditManager;
  private final String PAYLOAD_DIMENSION_KEY = "payload";
  private ConfigSerde<String> stringConfigSerde;


  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    connector.createAuditTable();
    auditManager = new SQLAuditManager(
        connector,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        new NoopServiceEmitter(),
        mapper,
        new SQLAuditManagerConfig()
    );
    stringConfigSerde = new ConfigSerde<String>()
    {
      @Override
      public byte[] serialize(String obj)
      {
        try {
          return mapper.writeValueAsBytes(obj);
        }
        catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public String serializeToString(String obj, boolean skipNull)
      {
        // In our test, payload Object is already a String
        // So to serialize to String, we just return a String
        return obj;
      }

      @Override
      public String deserialize(byte[] bytes)
      {
        return JacksonUtils.readValue(mapper, bytes, String.class);
      }
    };
  }

  @Test(timeout = 60_000L)
  public void testAuditEntrySerde() throws IOException
  {
    AuditEntry entry = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        DateTimes.of("2013-01-01T00:00:00Z")
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    AuditEntry serde = mapper.readValue(mapper.writeValueAsString(entry), AuditEntry.class);
    Assert.assertEquals(entry, serde);
  }

  @Test
  public void testAuditMetricEventBuilderConfig()
  {
    AuditEntry entry = new AuditEntry(
            "testKey",
            "testType",
            new AuditInfo(
                    "testAuthor",
                    "testComment",
                    "127.0.0.1"
            ),
            "testPayload",
            DateTimes.of("2013-01-01T00:00:00Z")
    );

    SQLAuditManager auditManagerWithPayloadAsDimension = new SQLAuditManager(
        connector,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        new NoopServiceEmitter(),
        mapper,
        new SQLAuditManagerConfig()
        {
          @Override
          public boolean getIncludePayloadAsDimensionInMetric()
          {
            return true;
          }
        }
    );

    ServiceMetricEvent.Builder auditEntryBuilder = ((SQLAuditManager) auditManager).getAuditMetricEventBuilder(entry);
    Assert.assertEquals(null, auditEntryBuilder.getDimension(PAYLOAD_DIMENSION_KEY));

    ServiceMetricEvent.Builder auditEntryBuilderWithPayload = auditManagerWithPayloadAsDimension.getAuditMetricEventBuilder(entry);
    Assert.assertEquals("testPayload", auditEntryBuilderWithPayload.getDimension(PAYLOAD_DIMENSION_KEY));
  }

  @Test(timeout = 60_000L)
  public void testCreateAuditEntry() throws IOException
  {
    String entry1Key = "testKey";
    String entry1Type = "testType";
    AuditInfo entry1AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry1Payload = "testPayload";

    auditManager.doAudit(entry1Key, entry1Type, entry1AuditInfo, entry1Payload, stringConfigSerde);

    byte[] payload = connector.lookup(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getAuditTable(),
        "audit_key",
        "payload",
        "testKey"
    );
    AuditEntry dbEntry = mapper.readValue(payload, AuditEntry.class);
    Assert.assertEquals(entry1Key, dbEntry.getKey());
    Assert.assertEquals(entry1Payload, dbEntry.getPayload());
    Assert.assertEquals(entry1Type, dbEntry.getType());
    Assert.assertEquals(entry1AuditInfo, dbEntry.getAuditInfo());
  }

  @Test(timeout = 60_000L)
  public void testFetchAuditHistory()
  {
    String entry1Key = "testKey";
    String entry1Type = "testType";
    AuditInfo entry1AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry1Payload = "testPayload";

    auditManager.doAudit(entry1Key, entry1Type, entry1AuditInfo, entry1Payload, stringConfigSerde);
    auditManager.doAudit(entry1Key, entry1Type, entry1AuditInfo, entry1Payload, stringConfigSerde);

    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(
        "testKey",
        "testType",
        Intervals.of("2000-01-01T00:00:00Z/2100-01-03T00:00:00Z")
    );
    Assert.assertEquals(2, auditEntries.size());

    Assert.assertEquals(entry1Key, auditEntries.get(0).getKey());
    Assert.assertEquals(entry1Payload, auditEntries.get(0).getPayload());
    Assert.assertEquals(entry1Type, auditEntries.get(0).getType());
    Assert.assertEquals(entry1AuditInfo, auditEntries.get(0).getAuditInfo());

    Assert.assertEquals(entry1Key, auditEntries.get(1).getKey());
    Assert.assertEquals(entry1Payload, auditEntries.get(1).getPayload());
    Assert.assertEquals(entry1Type, auditEntries.get(1).getType());
    Assert.assertEquals(entry1AuditInfo, auditEntries.get(1).getAuditInfo());
  }

  @Test(timeout = 60_000L)
  public void testFetchAuditHistoryByKeyAndTypeWithLimit()
  {
    String entry1Key = "testKey1";
    String entry1Type = "testType";
    AuditInfo entry1AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry1Payload = "testPayload";

    String entry2Key = "testKey2";
    String entry2Type = "testType";
    AuditInfo entry2AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry2Payload = "testPayload";

    auditManager.doAudit(entry1Key, entry1Type, entry1AuditInfo, entry1Payload, stringConfigSerde);
    auditManager.doAudit(entry2Key, entry2Type, entry2AuditInfo, entry2Payload, stringConfigSerde);
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(
        "testKey1",
        "testType",
        1
    );
    Assert.assertEquals(1, auditEntries.size());
    Assert.assertEquals(entry1Key, auditEntries.get(0).getKey());
    Assert.assertEquals(entry1Payload, auditEntries.get(0).getPayload());
    Assert.assertEquals(entry1Type, auditEntries.get(0).getType());
    Assert.assertEquals(entry1AuditInfo, auditEntries.get(0).getAuditInfo());
  }

  @Test(timeout = 60_000L)
  public void testFetchAuditHistoryByTypeWithLimit()
  {
    String entry1Key = "testKey";
    String entry1Type = "testType";
    AuditInfo entry1AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry1Payload = "testPayload1";

    String entry2Key = "testKey";
    String entry2Type = "testType";
    AuditInfo entry2AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry2Payload = "testPayload2";

    String entry3Key = "testKey";
    String entry3Type = "testType";
    AuditInfo entry3AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry3Payload = "testPayload3";

    auditManager.doAudit(entry1Key, entry1Type, entry1AuditInfo, entry1Payload, stringConfigSerde);
    auditManager.doAudit(entry2Key, entry2Type, entry2AuditInfo, entry2Payload, stringConfigSerde);
    auditManager.doAudit(entry3Key, entry3Type, entry3AuditInfo, entry3Payload, stringConfigSerde);

    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(
        "testType",
        2
    );
    Assert.assertEquals(2, auditEntries.size());
    Assert.assertEquals(entry3Key, auditEntries.get(0).getKey());
    Assert.assertEquals(entry3Payload, auditEntries.get(0).getPayload());
    Assert.assertEquals(entry3Type, auditEntries.get(0).getType());
    Assert.assertEquals(entry3AuditInfo, auditEntries.get(0).getAuditInfo());

    Assert.assertEquals(entry2Key, auditEntries.get(1).getKey());
    Assert.assertEquals(entry2Payload, auditEntries.get(1).getPayload());
    Assert.assertEquals(entry2Type, auditEntries.get(1).getType());
    Assert.assertEquals(entry2AuditInfo, auditEntries.get(1).getAuditInfo());
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
    int maxPayloadSize = 10;
    SQLAuditManager auditManagerWithMaxPayloadSizeBytes = new SQLAuditManager(
        connector,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        new NoopServiceEmitter(),
        mapper,
        new SQLAuditManagerConfig()
        {
          @Override
          public long getMaxPayloadSizeBytes()
          {
            return maxPayloadSize;
          }
        }
    );

    String entry1Key = "testKey";
    String entry1Type = "testType";
    AuditInfo entry1AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry1Payload = "payload audit to store";

    auditManagerWithMaxPayloadSizeBytes.doAudit(entry1Key, entry1Type, entry1AuditInfo, entry1Payload,
                                                stringConfigSerde
    );

    byte[] payload = connector.lookup(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getAuditTable(),
        "audit_key",
        "payload",
        "testKey"
    );

    AuditEntry dbEntry = mapper.readValue(payload, AuditEntry.class);
    Assert.assertEquals(entry1Key, dbEntry.getKey());
    Assert.assertNotEquals(entry1Payload, dbEntry.getPayload());
    Assert.assertEquals(StringUtils.format(AuditManager.PAYLOAD_SKIP_MSG_FORMAT, maxPayloadSize), dbEntry.getPayload());
    Assert.assertEquals(entry1Type, dbEntry.getType());
    Assert.assertEquals(entry1AuditInfo, dbEntry.getAuditInfo());
  }

  @Test(timeout = 60_000L)
  public void testCreateAuditEntryWithPayloadUnderSkipPayloadLimit() throws IOException
  {
    SQLAuditManager auditManagerWithMaxPayloadSizeBytes = new SQLAuditManager(
        connector,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        new NoopServiceEmitter(),
        mapper,
        new SQLAuditManagerConfig()
        {
          @Override
          public long getMaxPayloadSizeBytes()
          {
            return 500;
          }
        }
    );
    String entry1Key = "testKey";
    String entry1Type = "testType";
    AuditInfo entry1AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    String entry1Payload = "payload audit to store";

    auditManagerWithMaxPayloadSizeBytes.doAudit(entry1Key, entry1Type, entry1AuditInfo, entry1Payload,
                                                stringConfigSerde
    );

    byte[] payload = connector.lookup(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getAuditTable(),
        "audit_key",
        "payload",
        "testKey"
    );

    AuditEntry dbEntry = mapper.readValue(payload, AuditEntry.class);
    Assert.assertEquals(entry1Key, dbEntry.getKey());
    Assert.assertEquals(entry1Payload, dbEntry.getPayload());
    Assert.assertEquals(entry1Type, dbEntry.getType());
    Assert.assertEquals(entry1AuditInfo, dbEntry.getAuditInfo());
  }

  @Test(timeout = 60_000L)
  public void testCreateAuditEntryWithSkipNullConfigTrue()
  {
    ConfigSerde<Map<String, String>> mockConfigSerde = Mockito.mock(ConfigSerde.class);
    SQLAuditManager auditManagerWithSkipNull = new SQLAuditManager(
        connector,
        derbyConnectorRule.metadataTablesConfigSupplier(),
        new NoopServiceEmitter(),
        mapper,
        new SQLAuditManagerConfig()
        {
          @Override
          public boolean isSkipNullField()
          {
            return true;
          }
        }
    );

    String entry1Key = "test1Key";
    String entry1Type = "test1Type";
    AuditInfo entry1AuditInfo = new AuditInfo(
        "testAuthor",
        "testComment",
        "127.0.0.1"
    );
    // Entry 1 payload has a null field for one of the property
    Map<String, String> entryPayload1WithNull = new HashMap<>();
    entryPayload1WithNull.put("version", "x");
    entryPayload1WithNull.put("something", null);

    auditManagerWithSkipNull.doAudit(entry1Key, entry1Type, entry1AuditInfo, entryPayload1WithNull, mockConfigSerde);
    Mockito.verify(mockConfigSerde).serializeToString(ArgumentMatchers.eq(entryPayload1WithNull), ArgumentMatchers.eq(true));
  }

  @After
  public void cleanup()
  {
    dropTable(derbyConnectorRule.metadataTablesConfigSupplier().get().getAuditTable());
  }

  private void dropTable(final String tableName)
  {
    Assert.assertNull(connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle)
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    ));
  }
}
