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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class RulesResourceTest
{
  private ObjectMapper objectMapper = new DefaultObjectMapper();
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
    EasyMock.expect(auditManager.fetchAuditHistory(
                EasyMock.eq("datasource1"),
                EasyMock.eq("rules"),
                EasyMock.eq(theInterval)
            ))
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

  @Test
  public void testGetRules() throws JsonProcessingException
  {
    Map rules = ImmutableMap.of("a", Collections.singletonList(new ForeverLoadRule(null)));
    EasyMock.expect(databaseRuleManager.getAllRules()).andReturn(rules);
    EasyMock.replay(databaseRuleManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    Response response = rulesResource.getRules();

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(objectMapper.writeValueAsString(rules),
                        objectMapper.writeValueAsString(response.getEntity()));

    EasyMock.verify(databaseRuleManager);
  }

  @Test
  public void testGetDataSourceRulesFull() throws JsonProcessingException
  {
    List<Rule> rules = Collections.singletonList(new ForeverLoadRule(null));
    EasyMock.expect(databaseRuleManager.getRulesWithDefault("b")).andReturn(rules);
    EasyMock.replay(databaseRuleManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    Response response = rulesResource.getDatasourceRules("b", "full");

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    Assert.assertEquals(objectMapper.writeValueAsString(rules),
                        objectMapper.writeValueAsString(response.getEntity()));

    EasyMock.verify(databaseRuleManager);
  }

  @Test
  public void testSetDataSourceRule()
  {
    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn("127.0.0.1");
    EasyMock.replay(request);

    List<Rule> rules = Collections.singletonList(new ForeverLoadRule(null));
    EasyMock.expect(databaseRuleManager.overrideRule("b", rules, new AuditInfo("author", "comment", "127.0.0.1"))).andReturn(true);
    EasyMock.replay(databaseRuleManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    Response response = rulesResource.setDatasourceRules("b", rules, "author", "comment", request);

    Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    EasyMock.verify(databaseRuleManager);
  }

  @Test
  public void testSetDataSourceRuleError()
  {
    HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getRemoteAddr()).andReturn("127.0.0.1");
    EasyMock.replay(request);

    List<Rule> rules = Collections.singletonList(new ForeverLoadRule(null));
    EasyMock.expect(databaseRuleManager.overrideRule("b", rules, new AuditInfo("author", "comment", "127.0.0.1"))).andReturn(false);
    EasyMock.replay(databaseRuleManager);

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager);
    Response response = rulesResource.setDatasourceRules("b", rules, "author", "comment", request);

    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), response.getStatus());

    EasyMock.verify(databaseRuleManager);
  }
}
