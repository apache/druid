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

package io.druid.metadata;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.StringUtils;
import io.druid.server.audit.SQLAuditManager;
import io.druid.server.audit.SQLAuditManagerConfig;
import io.druid.server.coordinator.rules.IntervalLoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SQLMetadataRuleManagerTest
{
  @org.junit.Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private SQLMetadataRuleManager ruleManager;
  private AuditManager auditManager;
  private final ObjectMapper mapper = new DefaultObjectMapper();


  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
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
        new MetadataRuleManagerConfig(),
        tablesConfig,
        connector,
        auditManager
    );
  }

  @Test
  public void testRuleInsert()
  {
    List<Rule> rules = Arrays.<Rule>asList(
        new IntervalLoadRule(
            new Interval("2015-01-01/2015-02-01"), ImmutableMap.<String, Integer>of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        )
    );
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
    ruleManager.overrideRule(
        "test_dataSource",
        rules,
        auditInfo
    );
    // New rule should be be reflected in the in memory rules map immediately after being set by user
    Map<String, List<Rule>> allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get("test_dataSource").size());
    Assert.assertEquals(rules.get(0), allRules.get("test_dataSource").get(0));
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
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
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

    Assert.assertEquals(
        rules, mapper.readValue(
            entry.getPayload(), new TypeReference<List<Rule>>()
            {
            }
        )
    );
    Assert.assertEquals(auditInfo, entry.getAuditInfo());
    Assert.assertEquals("test_dataSource", entry.getKey());
  }

  @Test
  public void testFetchAuditEntriesForAllDataSources() throws Exception
  {
    List<Rule> rules = Arrays.<Rule>asList(
        new IntervalLoadRule(
            new Interval("2015-01-01/2015-02-01"), ImmutableMap.<String, Integer>of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        )
    );
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
    ruleManager.overrideRule(
        "test_dataSource",
        rules,
        auditInfo
    );
    ruleManager.overrideRule(
        "test_dataSource2",
        rules,
        auditInfo
    );
    // fetch rules from metadata storage
    ruleManager.poll();

    Assert.assertEquals(rules, ruleManager.getRules("test_dataSource"));
    Assert.assertEquals(rules, ruleManager.getRules("test_dataSource2"));

    // test fetch audit entries
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory("rules", null);
    Assert.assertEquals(2, auditEntries.size());
    for (AuditEntry entry : auditEntries) {
      Assert.assertEquals(
          rules, mapper.readValue(
              entry.getPayload(), new TypeReference<List<Rule>>()
              {
              }
          )
      );
      Assert.assertEquals(auditInfo, entry.getAuditInfo());
    }
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
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }

}
