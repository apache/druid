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

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableList;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
import org.apache.druid.server.coordinator.rules.PeriodBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.PeriodDropBeforeRule;
import org.apache.druid.server.coordinator.rules.PeriodDropRule;
import org.apache.druid.server.coordinator.rules.PeriodLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RulesResourceTest
{
  private MetadataRuleManager databaseRuleManager;
  private AuditManager auditManager;

  private static final PeriodLoadRule LOAD_P3M = new PeriodLoadRule(
      new Period("P3M"), true, null, null
  );
  private final ForeverLoadRule LOAD_FOREVER = new ForeverLoadRule(null, null);
  private static final PeriodDropRule DROP_P2M = new PeriodDropRule(new Period("P2M"), true);
  private static final PeriodDropBeforeRule DROP_BEFORE_P6M = new PeriodDropBeforeRule(new Period("P6M"));
  private static final IntervalDropRule DROP_2010_2020 = new IntervalDropRule(Intervals.of("2010/2020"));
  private static final PeriodBroadcastDistributionRule BROADCAST_PT2H = new PeriodBroadcastDistributionRule(
      new Period("PT2H"), true
  );

  @Before
  public void setUp()
  {
    databaseRuleManager = EasyMock.createStrictMock(MetadataRuleManager.class);
    auditManager = EasyMock.createStrictMock(AuditManager.class);
  }

  @Test
  public void testGetDatasourceRuleHistoryWithCount()
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
        DateTimes.of("2013-01-02T00:00:00Z")
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
        DateTimes.of("2013-01-01T00:00:00Z")
    );
    EasyMock.expect(auditManager.fetchAuditHistory(EasyMock.eq("datasource1"), EasyMock.eq("rules"), EasyMock.eq(2)))
            .andReturn(ImmutableList.of(entry1, entry2))
            .once();
    EasyMock.replay(auditManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    Response response = rulesResource.getDatasourceRuleHistory("datasource1", null, 2);
    List<AuditEntry> rulesHistory = (List) response.getEntity();
    Assert.assertEquals(2, rulesHistory.size());
    Assert.assertEquals(entry1, rulesHistory.get(0));
    Assert.assertEquals(entry2, rulesHistory.get(1));

    EasyMock.verify(auditManager);
  }

  @Test
  public void testGetDatasourceRuleHistoryWithInterval()
  {
    String interval = "P2D/2013-01-02T00:00:00Z";
    Interval theInterval = Intervals.of(interval);
    AuditEntry entry1 = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        DateTimes.of("2013-01-02T00:00:00Z")
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
        DateTimes.of("2013-01-01T00:00:00Z")
    );
    EasyMock.expect(auditManager.fetchAuditHistory(EasyMock.eq("datasource1"), EasyMock.eq("rules"), EasyMock.eq(theInterval)))
            .andReturn(ImmutableList.of(entry1, entry2))
            .once();
    EasyMock.replay(auditManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    Response response = rulesResource.getDatasourceRuleHistory("datasource1", interval, null);
    List<AuditEntry> rulesHistory = (List) response.getEntity();
    Assert.assertEquals(2, rulesHistory.size());
    Assert.assertEquals(entry1, rulesHistory.get(0));
    Assert.assertEquals(entry2, rulesHistory.get(1));

    EasyMock.verify(auditManager);
  }

  @Test
  public void testGetDatasourceRuleHistoryWithWrongCount()
  {
    EasyMock.expect(auditManager.fetchAuditHistory(EasyMock.eq("datasource1"), EasyMock.eq("rules"), EasyMock.eq(-1)))
        .andThrow(new IllegalArgumentException("Limit must be greater than zero!"))
        .once();
    EasyMock.replay(auditManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    Response response = rulesResource.getDatasourceRuleHistory("datasource1", null, -1);
    Map<String, Object> rulesHistory = (Map) response.getEntity();
    Assert.assertEquals(400, response.getStatus());
    Assert.assertTrue(rulesHistory.containsKey("error"));
    Assert.assertEquals("Limit must be greater than zero!", rulesHistory.get("error"));

    EasyMock.verify(auditManager);
  }

  @Test
  public void testSetDatasourceRulesWithEffectivelyNoRule()
  {
    EasyMock.expect(databaseRuleManager.overrideRule(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
                .andReturn(true).times(2);
    EasyMock.replay(databaseRuleManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final Response resp1 = rulesResource.setDatasourceRules("dataSource1", null, null, null, EasyMock.createMock(HttpServletRequest.class));
    Assert.assertEquals(200, resp1.getStatus());

    final Response resp2 = rulesResource.setDatasourceRules("dataSource1", new ArrayList<>(), null, null, EasyMock.createMock(HttpServletRequest.class));
    Assert.assertEquals(200, resp2.getStatus());
    EasyMock.verify(databaseRuleManager);
  }

  @Test
  public void testSetDatasourceRulesWithValidRules()
  {
    EasyMock.expect(databaseRuleManager.overrideRule(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true).anyTimes();
    EasyMock.replay(databaseRuleManager);
    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    final List<Rule> rules = new ArrayList<>();
    rules.add(BROADCAST_PT2H);
    rules.add(DROP_P2M);
    rules.add(DROP_2010_2020);
    rules.add(LOAD_P3M);
    rules.add(DROP_BEFORE_P6M);
    rules.add(LOAD_FOREVER);

    final Response resp = rulesResource.setDatasourceRules("dataSource1", rules, null, null, EasyMock.createMock(HttpServletRequest.class));
    Assert.assertEquals(200, resp.getStatus());
    EasyMock.verify(databaseRuleManager);
  }

  @Test
  public void testSetDatasourceRulesWithInvalidRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final List<Rule> rules = new ArrayList<>();
    rules.add(DROP_P2M);
    rules.add(LOAD_P3M);
    rules.add(DROP_BEFORE_P6M);
    rules.add(BROADCAST_PT2H);
    rules.add(DROP_2010_2020);
    rules.add(LOAD_FOREVER);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", DROP_P2M, BROADCAST_PT2H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testGetAllDatasourcesRuleHistoryWithCount()
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
        DateTimes.of("2013-01-02T00:00:00Z")
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
        DateTimes.of("2013-01-01T00:00:00Z")
    );
    EasyMock.expect(auditManager.fetchAuditHistory(EasyMock.eq("rules"), EasyMock.eq(2)))
            .andReturn(ImmutableList.of(entry1, entry2))
            .once();
    EasyMock.replay(auditManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    Response response = rulesResource.getDatasourceRuleHistory(null, 2);
    List<AuditEntry> rulesHistory = (List) response.getEntity();
    Assert.assertEquals(2, rulesHistory.size());
    Assert.assertEquals(entry1, rulesHistory.get(0));
    Assert.assertEquals(entry2, rulesHistory.get(1));

    EasyMock.verify(auditManager);
  }

  @Test
  public void testGetAllDatasourcesRuleHistoryWithInterval()
  {
    String interval = "P2D/2013-01-02T00:00:00Z";
    Interval theInterval = Intervals.of(interval);
    AuditEntry entry1 = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        DateTimes.of("2013-01-02T00:00:00Z")
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
        DateTimes.of("2013-01-01T00:00:00Z")
    );
    EasyMock.expect(auditManager.fetchAuditHistory(EasyMock.eq("rules"), EasyMock.eq(theInterval)))
            .andReturn(ImmutableList.of(entry1, entry2))
            .once();
    EasyMock.replay(auditManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    Response response = rulesResource.getDatasourceRuleHistory(interval, null);
    List<AuditEntry> rulesHistory = (List) response.getEntity();
    Assert.assertEquals(2, rulesHistory.size());
    Assert.assertEquals(entry1, rulesHistory.get(0));
    Assert.assertEquals(entry2, rulesHistory.get(1));

    EasyMock.verify(auditManager);
  }

  @Test
  public void testGetAllDatasourcesRuleHistoryWithWrongCount()
  {
    EasyMock.expect(auditManager.fetchAuditHistory(EasyMock.eq("rules"), EasyMock.eq(-1)))
        .andThrow(new IllegalArgumentException("Limit must be greater than zero!"))
        .once();
    EasyMock.replay(auditManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    Response response = rulesResource.getDatasourceRuleHistory(null, -1);
    Map<String, Object> rulesHistory = (Map) response.getEntity();
    Assert.assertEquals(400, response.getStatus());
    Assert.assertTrue(rulesHistory.containsKey("error"));
    Assert.assertEquals("Limit must be greater than zero!", rulesHistory.get("error"));

    EasyMock.verify(auditManager);
  }

}
