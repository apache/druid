/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.server.metrics.NoopServiceEmitter;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.io.IOException;
import java.util.List;

public class SQLAuditManagerTest
{
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("test");
  private AuditManager auditManager;

  private final ObjectMapper mapper = new DefaultObjectMapper();


  @Before
  public void setUp() throws Exception
  {
    connector = new TestDerbyConnector(
        Suppliers.ofInstance(new MetadataStorageConnectorConfig()),
        Suppliers.ofInstance(tablesConfig)
    );
    connector.createAuditTable();
    auditManager = new SQLAuditManager(
        connector,
        Suppliers.ofInstance(tablesConfig),
        new NoopServiceEmitter(),
        mapper,
        new SQLAuditManagerConfig()
    );
  }

  @Test
  public void testAuditEntrySerde() throws IOException
  {
    AuditEntry entry = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment"
        ),
        "testPayload",
        new DateTime("2013-01-01T00:00:00Z")
    );
    ObjectMapper mapper = new DefaultObjectMapper();
    AuditEntry serde = mapper.readValue(mapper.writeValueAsString(entry), AuditEntry.class);
    Assert.assertEquals(entry, serde);
  }

  @Test
  public void testCreateAuditEntry() throws IOException
  {
    AuditEntry entry = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment"
        ),
        "testPayload",
        new DateTime("2013-01-01T00:00:00Z")
    );
    auditManager.doAudit(entry);
    byte[] payload = connector.lookup(tablesConfig.getAuditTable(), "audit_key", "payload", "testKey");
    AuditEntry dbEntry = mapper.readValue(payload, AuditEntry.class);
    Assert.assertEquals(entry, dbEntry);

  }

  @Test
  public void testFetchAuditHistory() throws IOException
  {
    AuditEntry entry = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment"
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

  @After
  public void cleanup()
  {
    dropTable(tablesConfig.getAuditTable());
  }

  private void dropTable(final String tableName)
  {
    connector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(String.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }


}
