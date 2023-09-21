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
import org.apache.druid.server.coordinator.rules.IntervalDropRule;
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

    final PeriodLoadRule loadPT1H = new PeriodLoadRule(new Period("PT1H"), true, null, null);
    final PeriodLoadRule loadPT1HNoFuture = new PeriodLoadRule(new Period("PT1H"), false, null, null);
    final PeriodLoadRule loadP3M = new PeriodLoadRule(new Period("P3M"), true, null, null);
    final PeriodLoadRule loadP6M = new PeriodLoadRule(new Period("P6M"), true, null, null);
    final IntervalLoadRule loadInterval2020To2023 = new IntervalLoadRule(
        Intervals.of("2020/2023"),
        null,
        null
    );
    final IntervalLoadRule loadInterval2021To2022 = new IntervalLoadRule(
        Intervals.of("2021/2022"),
        null,
        null
    );
    final ForeverLoadRule loadForever = new ForeverLoadRule(null, null);

    final List<Rule> rules = new ArrayList<>();
    rules.add(loadP6M);
    rules.add(loadP3M);
    rules.add(loadForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadP6M, loadP3M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadP3M);
    rules.add(loadForever);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadForever, loadP6M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadForever);
    rules.add(loadPT1H);
    rules.add(loadInterval2021To2022);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadForever, loadPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadPT1H);
    rules.add(loadPT1HNoFuture);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadPT1H, loadPT1HNoFuture)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadPT1H);
    rules.add(loadPT1H);
    rules.add(loadP6M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadPT1H, loadPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadInterval2020To2023);
    rules.add(loadInterval2021To2022);
    rules.add(loadP6M);
    rules.add(loadForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadInterval2020To2023, loadInterval2021To2022)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(loadP6M);
    rules.add(loadInterval2021To2022);
    rules.add(loadP3M);
    rules.add(loadInterval2020To2023);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadP6M, loadP3M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));


    rules.clear();
    rules.add(loadP6M);
    rules.add(loadInterval2020To2023);
    rules.add(loadForever);
    rules.add(loadInterval2021To2022);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadInterval2020To2023, loadInterval2021To2022)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testDatasourceRulesWithInvalidDropRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final PeriodDropBeforeRule dropBeforeByP3M = new PeriodDropBeforeRule(new Period("P3M"));
    final PeriodDropBeforeRule dropBeforeByP6M = new PeriodDropBeforeRule(new Period("P6M"));
    final PeriodDropRule dropByP1M = new PeriodDropRule(new Period("P1M"), true);
    final IntervalDropRule dropInterval2000To2020 = new IntervalDropRule(Intervals.of("2000/2020"));
    final IntervalDropRule dropInterval2010To2020 = new IntervalDropRule(Intervals.of("2010/2020"));
    final PeriodDropRule dropByP1MNoFuture = new PeriodDropRule(new Period("P1M"), false);
    final PeriodDropRule dropByP2M = new PeriodDropRule(new Period("P2M"), true);
    final ForeverDropRule dropForever = new ForeverDropRule();

    final List<Rule> rules = new ArrayList<>();
    rules.add(dropBeforeByP3M);
    rules.add(dropBeforeByP6M);
    rules.add(dropByP1M);
    rules.add(dropForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", dropBeforeByP3M, dropBeforeByP6M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(dropByP2M);
    rules.add(dropByP1M);
    rules.add(dropForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", dropByP2M, dropByP1M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(dropInterval2000To2020);
    rules.add(dropByP1M);
    rules.add(dropByP2M);
    rules.add(dropInterval2010To2020);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", dropInterval2000To2020, dropInterval2010To2020)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(dropByP1M);
    rules.add(dropByP1MNoFuture);
    rules.add(dropForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", dropByP1M, dropByP1MNoFuture)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));


    rules.clear();
    rules.add(dropForever);
    rules.add(dropByP1M);
    rules.add(dropByP2M);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", dropForever, dropByP1M)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testDatasourceRulesWithInvalidBroadcastRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final ForeverBroadcastDistributionRule broadcastForever = new ForeverBroadcastDistributionRule();
    final PeriodBroadcastDistributionRule broadcastPT1H = new PeriodBroadcastDistributionRule(new Period("PT1H"), true);
    final PeriodBroadcastDistributionRule broadcastPT1HNoFuture = new PeriodBroadcastDistributionRule(new Period("PT1H"), false);
    final PeriodBroadcastDistributionRule broadcastPT2H = new PeriodBroadcastDistributionRule(new Period("PT2H"), true);
    final IntervalBroadcastDistributionRule broadcastInterval2000To2050 = new IntervalBroadcastDistributionRule(
        Intervals.of("2000/2050")
    );
    final IntervalBroadcastDistributionRule broadcastInterval2010To2020 = new IntervalBroadcastDistributionRule(
        Intervals.of("2010/2020")
    );

    final List<Rule> rules = new ArrayList<>();
    rules.add(broadcastInterval2000To2050);
    rules.add(broadcastInterval2010To2020);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", broadcastInterval2000To2050, broadcastInterval2010To2020)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(broadcastPT2H);
    rules.add(broadcastPT1H);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", broadcastPT2H, broadcastPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(broadcastPT1H);
    rules.add(broadcastPT1H);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", broadcastPT1H, broadcastPT1H)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    // Same interval with and without future.
    rules.clear();
    rules.add(broadcastPT1H);
    rules.add(broadcastPT1HNoFuture);
    rules.add(broadcastForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", broadcastPT1H, broadcastPT1HNoFuture)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));

    rules.clear();
    rules.add(broadcastPT1H);
    rules.add(broadcastPT2H);
    rules.add(broadcastInterval2010To2020);
    rules.add(broadcastForever);
    rules.add(broadcastInterval2000To2050);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", broadcastForever, broadcastInterval2000To2050)
    ).assertThrowsAndMatches(() -> rulesResource.setDatasourceRules("dataSource1", rules, null, null, req));
  }

  @Test
  public void testSetDatasourceRulesWithDifferentInvalidRules()
  {
    EasyMock.replay(auditManager);

    final RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    final HttpServletRequest req = EasyMock.createMock(HttpServletRequest.class);

    final IntervalLoadRule loadInterval1980To2050 = new IntervalLoadRule(
        Intervals.of("1980/2050"),
        null,
        null
    );
    final IntervalLoadRule loadInterval2020To2025 = new IntervalLoadRule(
        Intervals.of("2020/2025"),
        null,
        null
    );
    final PeriodLoadRule loadPT1H = new PeriodLoadRule(new Period("PT1H"), null, null, null);
    final PeriodLoadRule loadP3M = new PeriodLoadRule(new Period("P3M"), null, null, null);
    final PeriodLoadRule loadP6M = new PeriodLoadRule(new Period("P6M"), null, null, null);
    final ForeverLoadRule loadForever = new ForeverLoadRule(null, null);
    final ForeverDropRule dropForever = new ForeverDropRule();
    final PeriodBroadcastDistributionRule broadcastPT15m = new PeriodBroadcastDistributionRule(new Period("PT15m"), true);

    final List<Rule> rules = new ArrayList<>();
    rules.add(loadInterval2020To2025);
    rules.add(broadcastPT15m);
    rules.add(loadInterval1980To2050);
    rules.add(loadPT1H);
    rules.add(loadP3M);
    rules.add(loadP6M);
    rules.add(loadForever);
    rules.add(dropForever);

    DruidExceptionMatcher.invalidInput().expectMessageContains(
        StringUtils.format("Rule[%s] has an interval that fully contains the interval for rule[%s].", loadForever, dropForever)
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
    final PeriodLoadRule loadPT1H = new PeriodLoadRule(new Period("PT1H"), true, null, null);
    final PeriodLoadRule loadPT1HNoFuture = new PeriodLoadRule(new Period("PT1H"), false, null, null);
    final PeriodLoadRule loadP3M = new PeriodLoadRule(new Period("P3M"), true, null, null);
    final PeriodLoadRule loadP6M = new PeriodLoadRule(new Period("P6M"), true, null, null);
    final PeriodDropRule dropByP1M = new PeriodDropRule(new Period("P1M"), true);
    final PeriodDropRule dropByP1MNoFuture = new PeriodDropRule(new Period("P1M"), false);
    final ForeverLoadRule loadForever = new ForeverLoadRule(null, null);
    final ForeverDropRule dropForever = new ForeverDropRule();
    final PeriodBroadcastDistributionRule broadcastPT15m = new PeriodBroadcastDistributionRule(new Period("PT15m"), true);
    final PeriodBroadcastDistributionRule broadcastPT15mNoFuture = new PeriodBroadcastDistributionRule(new Period("PT15m"), false);

    final List<Rule> rules = new ArrayList<>();
    rules.add(loadSmallInterval);
    rules.add(loadLargeInteval);
    rules.add(broadcastPT15m);
    rules.add(loadPT1HNoFuture);
    rules.add(loadPT1H);
    rules.add(dropByP1MNoFuture);
    rules.add(dropByP1M);
    rules.add(loadP3M);
    rules.add(loadP6M);
    rules.add(loadForever);

    final Response resp = rulesResource.setDatasourceRules("dataSource1", rules, null, null, EasyMock.createMock(HttpServletRequest.class));
    Assert.assertEquals(200, resp.getStatus());
    EasyMock.verify(databaseRuleManager);

    rules.clear();
    rules.add(broadcastPT15mNoFuture);
    rules.add(broadcastPT15m);
    rules.add(loadPT1HNoFuture);
    rules.add(loadPT1H);
    rules.add(dropByP1MNoFuture);
    rules.add(dropByP1M);
    rules.add(loadP3M);
    rules.add(loadP6M);
    rules.add(loadSmallInterval);
    rules.add(loadLargeInteval);
    rules.add(dropForever);

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
