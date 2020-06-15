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
import org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.DruidServer;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.audit.SQLAuditManager;
import org.apache.druid.server.audit.SQLAuditManagerConfig;
import org.apache.druid.server.coordinator.rules.ForeverDropRule;
import org.apache.druid.server.coordinator.rules.ImportRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.server.coordinator.rules.PeriodLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.ArrayList;
import java.util.Collections;
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
  private MetadataRuleManagerConfig ruleManagerConfig = new MetadataRuleManagerConfig();

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
        ruleManagerConfig,
        tablesConfig,
        connector,
        auditManager
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
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
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
    List<Rule> rules = Collections.singletonList(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
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
        rules,
        mapper.readValue(
            entry.getPayload(),
            new TypeReference<List<Rule>>()
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
    List<Rule> rules = Collections.singletonList(
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
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
          rules,
          mapper.readValue(
              entry.getPayload(),
              new TypeReference<List<Rule>>()
              {
              }
          )
      );
      Assert.assertEquals(auditInfo, entry.getAuditInfo());
    }
  }

  @Test
  public void testImportOneRuleset()
  {
    //ensure default rule is created
    ruleManager.start();
    
    List<Rule> rules = ImmutableList.of(
        new PeriodLoadRule(new Period("P1M"), true, null),
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        ),
        new ForeverDropRule()
    );
    
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
    ruleManager.overrideRule(
        "test_dataSource",
        rules,
        auditInfo
    );
    
    List<Rule> rulesWithImport = ImmutableList.of(
        new PeriodLoadRule(new Period("P1W"), true, null),
        new ImportRule("test_dataSource")
    );
    
    ruleManager.overrideRule("test_dataSource", rules, auditInfo);
    ruleManager.overrideRule("import_dataSource", rulesWithImport, auditInfo);
    List<Rule> managerRules = ruleManager.getRules("test_dataSource");
    Assert.assertTrue("List in manager matches origin list", Iterables.elementsEqual(rules, managerRules));
    
    List<Rule> rulesWithDefault = new ArrayList<>(rules);
    rulesWithDefault.addAll(getDefaultRules());
    Assert.assertTrue("Rules with default matches", Iterables.elementsEqual(rulesWithDefault, ruleManager.getRulesWithDefault("test_dataSource"))); 
    
    List<Rule> expandedImportRules = new ArrayList<>();
    expandedImportRules.add(new PeriodLoadRule(new Period("P1W"), true, null));
    expandedImportRules.addAll(rules);
    expandedImportRules.addAll(getDefaultRules());
    Assert.assertTrue("Expanded Import Rules matches", Iterables.elementsEqual(expandedImportRules, ruleManager.getRulesWithDefault("import_dataSource")));    

    ruleManager.stop();
  }
  
  @Test
  public void testInvalidImport()
  {
    //ensure default rule is created
    ruleManager.start();
    
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
    
    List<Rule> rulesWithImport = ImmutableList.of(
        new PeriodLoadRule(new Period("P1W"), true, null),
        new ImportRule("test_dataSource")
    );
    
    ruleManager.overrideRule("import_dataSource", rulesWithImport, auditInfo);
        
    List<Rule> expandedImportRules = new ArrayList<>();
    expandedImportRules.add(new PeriodLoadRule(new Period("P1W"), true, null));
    expandedImportRules.addAll(getDefaultRules());
    Assert.assertTrue("Expanded Import Rules matches", Iterables.elementsEqual(expandedImportRules, ruleManager.getRulesWithDefault("import_dataSource")));    

    ruleManager.stop();    
  }

  @Test
  public void testImportedRulesetChanges()
  {
    //ensure default rule is created
    ruleManager.start();
    
    List<Rule> rules = ImmutableList.of(
        new PeriodLoadRule(new Period("P1M"), true, null),
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        ),
        new ForeverDropRule()
    );
    
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
    ruleManager.overrideRule(
        "test_dataSource",
        rules,
        auditInfo
    );
    
    List<Rule> rulesWithImport = ImmutableList.of(
        new PeriodLoadRule(new Period("P1W"), true, null),
        new ImportRule("test_dataSource")
    );
    
    ruleManager.overrideRule("test_dataSource", rules, auditInfo);
    ruleManager.overrideRule("import_dataSource", rulesWithImport, auditInfo);
    
    List<Rule> expandedImportRules = new ArrayList<>();
    expandedImportRules.add(new PeriodLoadRule(new Period("P1W"), true, null));
    expandedImportRules.addAll(rules);
    expandedImportRules.addAll(getDefaultRules());
    Assert.assertTrue("Check Import Before Change", Iterables.elementsEqual(expandedImportRules, ruleManager.getRulesWithDefault("import_dataSource")));    

    List<Rule> modifiedImports = ImmutableList.of(
        new PeriodLoadRule(new Period("P3M"), true, null)
    );
    ruleManager.overrideRule("test_dataSource", modifiedImports, auditInfo);
    List<Rule> modifiedExpandedImportRules = new ArrayList<>();
    modifiedExpandedImportRules.add(new PeriodLoadRule(new Period("P1W"), true, null));
    modifiedExpandedImportRules.addAll(modifiedImports);
    modifiedExpandedImportRules.addAll(getDefaultRules());
    Assert.assertTrue("Check Import After Change", Iterables.elementsEqual(modifiedExpandedImportRules, ruleManager.getRulesWithDefault("import_dataSource")));    
    
    ruleManager.stop();

  }
  
  @Test
  public void testImportNestedRuleset()
  {
    //ensure default rule is created
    ruleManager.start();
    
    List<Rule> rules = ImmutableList.of(
        new PeriodLoadRule(new Period("P1M"), true, null),
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        ),
        new ForeverDropRule()
    );
    
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
    ruleManager.overrideRule(
        "test_dataSource",
        rules,
        auditInfo
    );
    
    List<Rule> rulesWithImport = ImmutableList.of(
        new PeriodLoadRule(new Period("P1W"), true, null),
        new ImportRule("test_dataSource")
    );
    
    List<Rule> rulesWithImport2 = ImmutableList.of(
        new PeriodLoadRule(new Period("P3W"), true, null),
        new ImportRule("import_dataSource")
    );
    
    ruleManager.overrideRule("test_dataSource", rules, auditInfo);
    ruleManager.overrideRule("import_dataSource", rulesWithImport, auditInfo);
    ruleManager.overrideRule("import_dataSource2", rulesWithImport2, auditInfo);
    
    List<Rule> combinedImports = new ArrayList<Rule>(ImmutableList.of(
        new PeriodLoadRule(new Period("P3W"), true, null),
        new PeriodLoadRule(new Period("P1W"), true, null),
        new PeriodLoadRule(new Period("P1M"), true, null),
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        ),
        new ForeverDropRule()        
    ));
    combinedImports.addAll(getDefaultRules());
    Assert.assertTrue("Check Import After Change", Iterables.elementsEqual(combinedImports, ruleManager.getRulesWithDefault("import_dataSource2")));    
    
    ruleManager.stop();
  }

  @Test
  public void testImportNestedCycle()
  {
    //ensure default rule is created
    ruleManager.start();
    
    List<Rule> rules = ImmutableList.of(
        new PeriodLoadRule(new Period("P1M"), true, null),
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        ),
        new ForeverDropRule(),
        new ImportRule("import_dataSource2")
    );
    
    AuditInfo auditInfo = new AuditInfo("test_author", "test_comment", "127.0.0.1");
    ruleManager.overrideRule(
        "test_dataSource",
        rules,
        auditInfo
    );
    
    List<Rule> rulesWithImport = ImmutableList.of(
        new PeriodLoadRule(new Period("P1W"), true, null),
        new ImportRule("test_dataSource"),
        new ImportRule("import_dataSource2")
    );
    
    List<Rule> rulesWithImport2 = ImmutableList.of(
        new ImportRule("import_dataSource2"),
        new PeriodLoadRule(new Period("P3W"), true, null),
        new ImportRule("import_dataSource"),
        new ImportRule("import_dataSource")
    );
    
    ruleManager.overrideRule("test_dataSource", rules, auditInfo);
    ruleManager.overrideRule("import_dataSource", rulesWithImport, auditInfo);
    ruleManager.overrideRule("import_dataSource2", rulesWithImport2, auditInfo);
    
    List<Rule> combinedImports = new ArrayList<Rule>(ImmutableList.of(
        new PeriodLoadRule(new Period("P3W"), true, null),
        new PeriodLoadRule(new Period("P1W"), true, null),
        new PeriodLoadRule(new Period("P1M"), true, null),
        new IntervalLoadRule(
            Intervals.of("2015-01-01/2015-02-01"), ImmutableMap.of(
            DruidServer.DEFAULT_TIER,
            DruidServer.DEFAULT_NUM_REPLICANTS
        )
        ),
        new ForeverDropRule()        
    ));
    combinedImports.addAll(getDefaultRules());
    Assert.assertTrue("Check Import After Change", Iterables.elementsEqual(combinedImports, ruleManager.getRulesWithDefault("import_dataSource2")));    
    
    ruleManager.stop();
  }
  
  @Test
  public void testGetDefaultWithDefault()
  {
    //ensure default rule is created
    ruleManager.start();
    Assert.assertEquals(getDefaultRules(), ruleManager.getRulesWithDefault(ruleManagerConfig.getDefaultRule()));
    ruleManager.stop();    
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
          public Void withHandle(Handle handle)
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }
  
  private List<Rule> getDefaultRules()
  {
    return ruleManager.getRules(ruleManagerConfig.getDefaultRule());
  }

}
