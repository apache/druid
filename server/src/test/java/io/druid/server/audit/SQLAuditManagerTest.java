/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.TestDerbyConnector;
import io.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.List;

public class SQLAuditManagerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private AuditManager auditManager;

  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp() throws Exception
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
  }

  @Test(timeout = 10_000L)
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
        new DateTime("2013-01-01T00:00:00Z")
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    AuditEntry serde = mapper.readValue(mapper.writeValueAsString(entry), AuditEntry.class);
    Assert.assertEquals(entry, serde);
  }

  @Test(timeout = 10_000L)
  public void testCreateAuditEntry() throws IOException
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
        new DateTime("2013-01-01T00:00:00Z")
    );
    auditManager.doAudit(entry);
    byte[] payload = connector.lookup(
        derbyConnectorRule.metadataTablesConfigSupplier().get().getAuditTable(),
        "audit_key",
        "payload",
        "testKey"
    );
    AuditEntry dbEntry = mapper.readValue(payload, AuditEntry.class);
    Assert.assertEquals(entry, dbEntry);

  }

  @Test(timeout = 10_000L)
  public void testFetchAuditHistory() throws IOException
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
        new DateTime("2013-01-01T00:00:00Z")
    );
    auditManager.doAudit(entry);
    auditManager.doAudit(entry);
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(
        "testKey",
        "testType",
        new Interval(
            "2012-01-01T00:00:00Z/2013-01-03T00:00:00Z"
        )
    );
    Assert.assertEquals(2, auditEntries.size());
    Assert.assertEquals(entry, auditEntries.get(0));
    Assert.assertEquals(entry, auditEntries.get(1));
  }

  @Test(timeout = 10_000L)
  public void testFetchAuditHistoryByKeyAndTypeWithLimit() throws IOException
  {
    AuditEntry entry1 = new AuditEntry(
        "testKey1",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        new DateTime("2013-01-01T00:00:00Z")
    );
    AuditEntry entry2 = new AuditEntry(
        "testKey2",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        new DateTime("2013-01-02T00:00:00Z")
    );
    auditManager.doAudit(entry1);
    auditManager.doAudit(entry2);
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(
        "testKey1",
        "testType",
        1
    );
    Assert.assertEquals(1, auditEntries.size());
    Assert.assertEquals(entry1, auditEntries.get(0));
  }

  @Test(timeout = 10_000L)
  public void testFetchAuditHistoryByTypeWithLimit() throws IOException
  {
    AuditEntry entry1 = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        new DateTime("2013-01-01T00:00:00Z")
    );
    AuditEntry entry2 = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        new DateTime("2013-01-02T00:00:00Z")
    );
    AuditEntry entry3 = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        new DateTime("2013-01-03T00:00:00Z")
    );
    auditManager.doAudit(entry1);
    auditManager.doAudit(entry2);
    auditManager.doAudit(entry3);
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(
        "testType",
        2
    );
    Assert.assertEquals(2, auditEntries.size());
    Assert.assertEquals(entry3, auditEntries.get(0));
    Assert.assertEquals(entry2, auditEntries.get(1));
  }

  @Test(expected=IllegalArgumentException.class, timeout = 10_000L)
  public void testFetchAuditHistoryLimitBelowZero() throws IOException
  {
    auditManager.fetchAuditHistory("testType", -1);
  }

  @Test(expected=IllegalArgumentException.class, timeout = 10_000L)
  public void testFetchAuditHistoryLimitZero() throws IOException
  {
    auditManager.fetchAuditHistory("testType", 0);
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
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    ));
  }
}
