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

package org.apache.druid.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.DruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.audit.AuditSerdeHelper;
import org.apache.druid.server.audit.SQLAuditManager;
import org.apache.druid.server.audit.SQLAuditManagerConfig;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SQLMetadataRuleManagerTest
{
  private static final String DATASOURCE = "wiki";
  
  @org.junit.Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private MetadataRuleManagerConfig managerConfig;
  private SQLMetadataRuleManager ruleManager;
  private AuditManager auditManager;
  private SQLMetadataSegmentPublisher publisher;
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createAuditTable();

    final SQLAuditManagerConfig auditManagerConfig = new SQLAuditManagerConfig(null, null, null, null, null);
    auditManager = new SQLAuditManager(
        auditManagerConfig,
        new AuditSerdeHelper(auditManagerConfig, null, mapper, mapper),
        connector,
        Suppliers.ofInstance(tablesConfig),
        new NoopServiceEmitter(),
        mapper
    );

    connector.createRulesTable();
    managerConfig = new MetadataRuleManagerConfig();
    ruleManager = new SQLMetadataRuleManager(mapper, managerConfig, tablesConfig, connector, auditManager);
    connector.createSegmentTable();
    publisher = new SQLMetadataSegmentPublisher(
        mapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        connector
    );
  }

  @Test
  public void testMultipleStopAndStart()
  {
    // Simulate successive losing and getting the coordinator leadership
    ruleManager.start();
    ruleManager.stop();
    ruleManager.start();
    ruleManager.stop();
  }

  @Test
  public void testRuleInsert()
  {
    List<Rule> rules = Collections.singletonList(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"),
            ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
            null
        )
    );
    ruleManager.overrideRule(DATASOURCE, rules, createAuditInfo("override rule"));
    // New rule should be be reflected in the in memory rules map immediately after being set by user
    Map<String, List<Rule>> allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get(DATASOURCE).size());
    Assert.assertEquals(rules.get(0), allRules.get(DATASOURCE).get(0));
  }

  @Test
  public void testOverrideRuleWithNull()
  {
    // Datasource level rules cannot be null
    IAE exception = Assert.assertThrows(
        IAE.class,
        () -> ruleManager.overrideRule(DATASOURCE, null, createAuditInfo("null rule"))
    );
    Assert.assertEquals("Rules cannot be null.", exception.getMessage());

    // Cluster level rules cannot be null
    exception = Assert.assertThrows(
        IAE.class,
        () -> ruleManager.overrideRule(
            managerConfig.getDefaultRule(),
            null,
            createAuditInfo("null cluster rule")
        )
    );
    Assert.assertEquals("Rules cannot be null.", exception.getMessage());
  }

  @Test
  public void testOverrideRuleWithEmpty()
  {
    // Cluster level rules cannot be empty
    IAE exception = Assert.assertThrows(
        IAE.class,
        () -> ruleManager.overrideRule(
            managerConfig.getDefaultRule(),
            Collections.emptyList(),
            createAuditInfo("empty cluster rule")
        )
    );
    Assert.assertEquals("Cluster-level rules cannot be empty.", exception.getMessage());

    // Datasource level rules can be empty
    Assert.assertTrue(
        ruleManager.overrideRule(
            DATASOURCE,
            Collections.emptyList(),
            createAuditInfo("empty rule")
        )
    );
  }

  @Test
  public void testAuditEntryCreated() throws Exception
  {
    List<Rule> rules = Collections.singletonList(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"),
            ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
            null
        )
    );
    final AuditInfo auditInfo = createAuditInfo("create audit entry");
    ruleManager.overrideRule(DATASOURCE, rules, auditInfo);
    // fetch rules from metadata storage
    ruleManager.poll();

    Assert.assertEquals(rules, ruleManager.getRules(DATASOURCE));

    // verify audit entry is created
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory(DATASOURCE, "rules", null);
    Assert.assertEquals(1, auditEntries.size());
    AuditEntry entry = auditEntries.get(0);

    Assert.assertEquals(
        rules,
        mapper.readValue(entry.getPayload().serialized(), new TypeReference<List<Rule>>() {})
    );
    Assert.assertEquals(auditInfo, entry.getAuditInfo());
    Assert.assertEquals(DATASOURCE, entry.getKey());
  }

  @Test
  public void testFetchAuditEntriesForAllDataSources() throws Exception
  {
    List<Rule> rules = Collections.singletonList(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
                DruidServer.DEFAULT_TIER,
                DruidServer.DEFAULT_NUM_REPLICANTS
        ),
            null
        )
    );
    final AuditInfo auditInfo = createAuditInfo("test_comment");
    ruleManager.overrideRule(DATASOURCE, rules, auditInfo);
    ruleManager.overrideRule("test_dataSource2", rules, auditInfo);
    // fetch rules from metadata storage
    ruleManager.poll();

    Assert.assertEquals(rules, ruleManager.getRules(DATASOURCE));
    Assert.assertEquals(rules, ruleManager.getRules("test_dataSource2"));

    // test fetch audit entries
    List<AuditEntry> auditEntries = auditManager.fetchAuditHistory("rules", null);
    Assert.assertEquals(2, auditEntries.size());
    for (AuditEntry entry : auditEntries) {
      Assert.assertEquals(
          rules,
          mapper.readValue(entry.getPayload().serialized(), new TypeReference<List<Rule>>() {})
      );
      Assert.assertEquals(auditInfo, entry.getAuditInfo());
    }
  }

  @Test
  public void testRemoveRulesOlderThanWithNonExistenceDatasourceAndOlderThanTimestampShouldDelete()
  {
    List<Rule> rules = ImmutableList.of(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"),
            ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
            null
        )
    );
    ruleManager.overrideRule(DATASOURCE, rules, createAuditInfo("test"));

    // Verify that the rule was added
    ruleManager.poll();
    Map<String, List<Rule>> allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get(DATASOURCE).size());

    // Now delete rules
    ruleManager.removeRulesForEmptyDatasourcesOlderThan(System.currentTimeMillis());

    // Verify that rule was deleted
    ruleManager.poll();
    allRules = ruleManager.getAllRules();
    Assert.assertEquals(0, allRules.size());
  }

  @Test
  public void testRemoveRulesOlderThanWithNonExistenceDatasourceAndNewerThanTimestampShouldNotDelete()
  {
    List<Rule> rules = ImmutableList.of(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"),
            ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
            null
        )
    );
    ruleManager.overrideRule(DATASOURCE, rules, createAuditInfo("update rules"));

    // Verify that rule was added
    ruleManager.poll();
    Map<String, List<Rule>> allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get(DATASOURCE).size());

    // This will not delete the rule as the rule was created just now so it will have the created timestamp later than
    // the timestamp 2012-01-01T00:00:00Z
    ruleManager.removeRulesForEmptyDatasourcesOlderThan(DateTimes.of("2012-01-01T00:00:00Z").getMillis());

    // Verify that rule was not deleted
    ruleManager.poll();
    allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get(DATASOURCE).size());
  }

  @Test
  public void testRemoveRulesOlderThanWithActiveDatasourceShouldNotDelete() throws Exception
  {
    List<Rule> rules = ImmutableList.of(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"),
            ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS),
            null
        )
    );
    ruleManager.overrideRule(DATASOURCE, rules, createAuditInfo("update rules"));

    // Verify that rule was added
    ruleManager.poll();
    Map<String, List<Rule>> allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get(DATASOURCE).size());

    // Add segment metadata to segment table so that the datasource is considered active
    DataSegment dataSegment = new DataSegment(
        DATASOURCE,
        Intervals.of("2015-01-01/2015-02-01"),
        "1",
        ImmutableMap.of(
            "type", "s3_zip",
            "bucket", "test",
            "key", "test_dataSource/xxx"
        ),
        ImmutableList.of("dim1", "dim2", "dim3"),
        ImmutableList.of("count", "value"),
        NoneShardSpec.instance(),
        1,
        1234L
    );
    publisher.publishSegment(dataSegment);

    // This will not delete the rule as the datasource has segment in the segment metadata table
    ruleManager.removeRulesForEmptyDatasourcesOlderThan(System.currentTimeMillis());

    // Verify that rule was not deleted
    ruleManager.poll();
    allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get(DATASOURCE).size());
  }

  @Test
  public void testRemoveRulesOlderThanShouldNotDeleteDefault()
  {
    // Create the default rule
    ruleManager.start();
    // Verify the default rule
    ruleManager.poll();
    Map<String, List<Rule>> allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get("_default").size());
    // Delete everything
    ruleManager.removeRulesForEmptyDatasourcesOlderThan(System.currentTimeMillis());
    // Verify the default rule was not deleted
    ruleManager.poll();
    allRules = ruleManager.getAllRules();
    Assert.assertEquals(1, allRules.size());
    Assert.assertEquals(1, allRules.get("_default").size());
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
        handle -> handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                      .execute()
    );
  }

  private AuditInfo createAuditInfo(String comment)
  {
    return new AuditInfo("test", "id", comment, "127.0.0.1");
  }

}
