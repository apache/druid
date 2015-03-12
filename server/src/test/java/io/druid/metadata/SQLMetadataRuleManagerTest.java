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

package io.druid.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.audit.SQLAuditManager;
import io.druid.server.audit.SQLAuditManagerConfig;
import io.druid.server.coordinator.rules.ForeverDropRule;
import io.druid.server.coordinator.rules.ForeverLoadRule;
import io.druid.server.coordinator.rules.IntervalLoadRule;
import io.druid.server.coordinator.rules.PeriodLoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.server.metrics.NoopServiceEmitter;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SQLMetadataRuleManagerTest
{
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("test");
  private SQLMetadataRuleManager ruleManager;
  private AuditManager auditManager;
  private final ObjectMapper mapper = new DefaultObjectMapper();


  @Before
  public void setUp()
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

    connector.createRulesTable();
    ruleManager = new SQLMetadataRuleManager(
        mapper,
        Suppliers.ofInstance(new MetadataRuleManagerConfig()),
        Suppliers.ofInstance(tablesConfig),
        connector,
        auditManager
    );
  }

  @Test
  public void testAuditEntryCreated() throws Exception
  {
    List<Rule> rules = Arrays.<Rule>asList(
        new IntervalLoadRule(
            new Interval("2015-01-01/2015-02-01"), ImmutableMap.<String, Integer>of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        )
    );
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment");
    ruleManager.overrideRule(
        "test_dataSource",
        rules,
       auditInfo
    );
    // fetch rules from metadata storage
    ruleManager.poll();

    Assert.assertEquals(rules, ruleManager.getRules("test_dataSource"));

    // verify audit entry is created
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory("test_dataSource", "rules", null);
    Assert.assertEquals(1, auditEntries.size());
    AuditEntry entry = auditEntries.get(0);
    Assert.assertEquals(mapper.writeValueAsString(rules),entry.getPayload());
    Assert.assertEquals(auditInfo,entry.getAuditInfo());
    Assert.assertEquals("test_dataSource", entry.getKey());
  }

  @After
  public void cleanup()
  {
    dropTable(tablesConfig.getAuditTable());
    dropTable(tablesConfig.getRulesTable());
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
