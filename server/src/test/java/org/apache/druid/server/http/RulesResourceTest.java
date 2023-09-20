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
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.ForeverDropRule;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.IntervalBroadcastDistributionRule;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
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
  public void testSetDatasourceRulesWithInvalidLoadRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final IntervalLoadRule loadInterval1 = new IntervalLoadRule(
        Intervals.of("2023-07-29T01:00:00Z/2023-12-30T01:00:00Z"),
        null,
        null
    );
    final IntervalLoadRule loadInterval2 = new IntervalLoadRule(
        Intervals.of("2023-09-29T01:00:00Z/2023-10-30T01:00:00Z"),
        null,
        null
    );
    final PeriodLoadRule loadPT1H = new PeriodLoadRule(new Period("PT1H"), true, null, null);
    final PeriodLoadRule loadPT1HNoFuture = new PeriodLoadRule(new Period("PT1H"), false, null, null);
    final PeriodLoadRule loadP3M = new PeriodLoadRule(new Period("P3M"), true, null, null);
    final PeriodLoadRule loadP6M = new PeriodLoadRule(new Period("P6M"), true, null, null);
    final ForeverLoadRule loadForever = new ForeverLoadRule(null, null);

    final List<Rule> rules = new ArrayList<>();
    rules.add(loadP6M);
    rules.add(loadP3M);
    rules.add(loadForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", loadP6M, loadP3M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadP3M);
    rules.add(loadForever);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", loadForever, loadP6M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadForever);
    rules.add(loadPT1H);
    rules.add(loadInterval2);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", loadForever, loadPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadPT1H);
    rules.add(loadPT1HNoFuture);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", loadPT1H, loadPT1HNoFuture)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadPT1H);
    rules.add(loadPT1H);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", loadPT1H, loadPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadInterval1);
    rules.add(loadInterval2);
    rules.add(loadP6M);
    rules.add(loadForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", loadInterval1, loadInterval2)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadP6M);
    rules.add(loadInterval1);
    rules.add(loadForever);
    rules.add(loadInterval2);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", loadForever, loadInterval2)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testDatasourceRulesWithInvalidDropRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final PeriodDropBeforeRule dropBeforeP3M = new PeriodDropBeforeRule(new Period("P3M"));
    final PeriodDropBeforeRule dropBeforeP6M = new PeriodDropBeforeRule(new Period("P6M"));
    final PeriodDropRule dropByP1M = new PeriodDropRule(new Period("P1M"), true);
    final PeriodDropRule dropByP1MNoFuture = new PeriodDropRule(new Period("P1M"), false);
    final PeriodDropRule dropByP2M = new PeriodDropRule(new Period("P2M"), true);
    final ForeverDropRule dropForever = new ForeverDropRule();

    final List<Rule> rules = new ArrayList<>();
    rules.add(dropBeforeP3M);
    rules.add(dropBeforeP6M);
    rules.add(dropByP1M);
    rules.add(dropForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", dropBeforeP3M, dropBeforeP6M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(dropByP2M);
    rules.add(dropByP1M);
    rules.add(dropForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", dropByP2M, dropByP1M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(dropByP1M);
    rules.add(dropByP1MNoFuture);
    rules.add(dropForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", dropByP1M, dropByP1MNoFuture)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));


    rules.clear();
    rules.add(dropForever);
    rules.add(dropByP1M);
    rules.add(dropByP2M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", dropForever, dropByP1M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testDatasourceRulesWithInvalidBroadcastRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final ForeverBroadcastDistributionRule broadcastForever = new ForeverBroadcastDistributionRule();
    final PeriodBroadcastDistributionRule broadcastPeriodPT1H = new PeriodBroadcastDistributionRule(new Period("PT1H"), true);
    final PeriodBroadcastDistributionRule broadcastPeriodPT1HNoFuture = new PeriodBroadcastDistributionRule(new Period("PT1H"), false);
    final PeriodBroadcastDistributionRule broadcastPeriodPT2H = new PeriodBroadcastDistributionRule(new Period("PT2H"), true);
    final IntervalBroadcastDistributionRule broadcastInterval1 = new IntervalBroadcastDistributionRule(
        Intervals.of("2000-09-29T01:00:00Z/2050-10-30T01:00:00Z")
    );
    final IntervalBroadcastDistributionRule broadcastInterval2 = new IntervalBroadcastDistributionRule(
        Intervals.of("2010-09-29T01:00:00Z/2020-10-30T01:00:00Z")
    );

    final List<Rule> rules = new ArrayList<>();
    rules.add(broadcastInterval1);
    rules.add(broadcastInterval2);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", broadcastInterval1, broadcastInterval2)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(broadcastPeriodPT2H);
    rules.add(broadcastPeriodPT1H);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", broadcastPeriodPT2H, broadcastPeriodPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(broadcastPeriodPT1H);
    rules.add(broadcastPeriodPT1H);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", broadcastPeriodPT1H, broadcastPeriodPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    // Same interval with and without future.
    rules.clear();
    rules.add(broadcastPeriodPT1H);
    rules.add(broadcastPeriodPT1HNoFuture);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", broadcastPeriodPT1H, broadcastPeriodPT1HNoFuture)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(broadcastPeriodPT1H);
    rules.add(broadcastPeriodPT2H);
    rules.add(broadcastInterval2);
    rules.add(broadcastForever);
    rules.add(broadcastInterval1);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", broadcastForever, broadcastInterval1)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testSetDatasourceRulesWithDifferentInvalidRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final IntervalLoadRule loadLargeInteval = new IntervalLoadRule(
        Intervals.of("1980-07-29T01:00:00Z/2050-12-30T01:00:00Z"),
        null,
        null
    );
    final IntervalLoadRule loadSmallInterval = new IntervalLoadRule(
        Intervals.of("2020-09-29T01:00:00Z/2025-10-30T01:00:00Z"),
        null,
        null
    );
    final PeriodLoadRule periodPT1H = new PeriodLoadRule(new Period("PT1H"), null, null, null);
    final PeriodLoadRule periodP3M = new PeriodLoadRule(new Period("P3M"), null, null, null);
    final PeriodLoadRule periodP6M = new PeriodLoadRule(new Period("P6M"), null, null, null);
    final ForeverLoadRule foreverLoadRule = new ForeverLoadRule(null, null);
    final ForeverDropRule foreverDropRule = new ForeverDropRule();
    final PeriodBroadcastDistributionRule broadcastPeriodPT15m = new PeriodBroadcastDistributionRule(new Period("PT15m"), true);

    final List<Rule> rules = new ArrayList<>();
    rules.add(loadSmallInterval);
    rules.add(loadLargeInteval);
    rules.add(broadcastPeriodPT15m);
    rules.add(periodPT1H);
    rules.add(periodP3M);
    rules.add(periodP6M);
    rules.add(foreverLoadRule);
    rules.add(foreverDropRule);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that contains interval for rule[%s].", foreverLoadRule, foreverDropRule)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testSetDatasourceRulesWithValidRules()
  {
    EasyMock.expect(databaseRuleManager.overrideRule(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(true).anyTimes();
    EasyMock.replay(databaseRuleManager);
    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);

    final IntervalLoadRule loadLargeInteval = new IntervalLoadRule(
        Intervals.of("1980-07-29T01:00:00Z/2050-12-30T01:00:00Z"),
        null,
        null
    );
    final IntervalLoadRule loadSmallInterval = new IntervalLoadRule(
        Intervals.of("2020-09-29T01:00:00Z/2025-10-30T01:00:00Z"),
        null,
        null
    );
    final PeriodLoadRule periodPT1H = new PeriodLoadRule(new Period("PT1H"), true, null, null);
    final PeriodLoadRule periodPT1HNoFuture = new PeriodLoadRule(new Period("PT1H"), false, null, null);
    final PeriodLoadRule periodP3M = new PeriodLoadRule(new Period("P3M"), true, null, null);
    final PeriodLoadRule periodP6M = new PeriodLoadRule(new Period("P6M"), true, null, null);
    final PeriodDropRule dropByP1M = new PeriodDropRule(new Period("P1M"), true);
    final PeriodDropRule dropByP1MNoFuture = new PeriodDropRule(new Period("P1M"), false);
    final ForeverLoadRule foreverLoadRule = new ForeverLoadRule(null, null);
    final ForeverDropRule foreverDropRule = new ForeverDropRule();
    final PeriodBroadcastDistributionRule broadcastPeriodPT15m = new PeriodBroadcastDistributionRule(new Period("PT15m"), true);
    final PeriodBroadcastDistributionRule broadcastPeriodPT15mNoFuture = new PeriodBroadcastDistributionRule(new Period("PT15m"), false);

    final List<Rule> rules = new ArrayList<>();
    rules.add(loadSmallInterval);
    rules.add(loadLargeInteval);
    rules.add(broadcastPeriodPT15m);
    rules.add(periodPT1HNoFuture);
    rules.add(periodPT1H);
    rules.add(dropByP1MNoFuture);
    rules.add(dropByP1M);
    rules.add(periodP3M);
    rules.add(periodP6M);
    rules.add(foreverLoadRule);

    final Response resp = rulesResource.setDatasourceRules("dataSource1", rules, null, null, EasyMock.createMock(HttpServletRequest.class));
    Assert.assertEquals(200, resp.getStatus());
    EasyMock.verify(databaseRuleManager);

    rules.clear();
    rules.add(broadcastPeriodPT15mNoFuture);
    rules.add(broadcastPeriodPT15m);
    rules.add(periodPT1HNoFuture);
    rules.add(periodPT1H);
    rules.add(dropByP1MNoFuture);
    rules.add(dropByP1M);
    rules.add(periodP3M);
    rules.add(periodP6M);
    rules.add(loadSmallInterval);
    rules.add(loadLargeInteval);
    rules.add(foreverDropRule);

    final Response resp2 = rulesResource.setDatasourceRules("dataSource1", rules, null, null, EasyMock.createMock(HttpServletRequest.class));
    Assert.assertEquals(200, resp2.getStatus());
    EasyMock.verify(databaseRuleManager);
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
