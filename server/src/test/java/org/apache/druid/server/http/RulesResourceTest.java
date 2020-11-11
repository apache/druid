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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.Resource;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
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
  public void testGetRules()
  {
    final String authorizerName = "testAuthorizer";

    EasyMock.expect(databaseRuleManager.getAllRules()).andReturn(
        ImmutableMap.of(
            "ds1",
            ImmutableList.of(new ForeverLoadRule(null)),
            "ds2",
            ImmutableList.of(new ForeverLoadRule(null))
        )
    ).once();

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager, new AuthorizerMapper(
        ImmutableMap.of(authorizerName,
            new Authorizer()
            {
              @Override
              public Access authorize(AuthenticationResult authenticationResult, Resource resource,
                  Action action
              )
              {
                return null;
              }

              @Override
              public Access authorizeV2(AuthenticationResult authenticationResult, Resource resource, Action action)
              {
                if (resource.getName().equals("ds1")) {
                  return new Access(true);
                }
                return new Access(false);
              }
            }
        ), AuthConfig.AUTH_VERSION_2));

    final HttpServletRequest request = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).once();
    EasyMock.expect(request.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT))
        .andReturn(new AuthenticationResult("", authorizerName, "", null)).once();
    request.setAttribute(EasyMock.anyString(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();

    EasyMock.replay(databaseRuleManager, request);

    Response response = rulesResource.getRules(request);
    final Map<String, List<Rule>> rules = (Map<String, List<Rule>>) response.getEntity();
    Assert.assertEquals(1, rules.size());
    Assert.assertEquals(1, rules.get("ds1").size());
    Assert.assertEquals(new ForeverLoadRule(null), rules.get("ds1").get(0));

    EasyMock.verify(databaseRuleManager, request);
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

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager, new AuthorizerMapper(null, null));

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

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager, new AuthorizerMapper(null, null));

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

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager, new AuthorizerMapper(null, null));

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

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager, new AuthorizerMapper(null, null));

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

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager, new AuthorizerMapper(null, null));

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

    RulesResource rulesResource = new RulesResource(databaseRuleManager, auditManager, new AuthorizerMapper(null, null));

    Response response = rulesResource.getDatasourceRuleHistory(null, -1);
    Map<String, Object> rulesHistory = (Map) response.getEntity();
    Assert.assertEquals(400, response.getStatus());
    Assert.assertTrue(rulesHistory.containsKey("error"));
    Assert.assertEquals("Limit must be greater than zero!", rulesHistory.get("error"));

    EasyMock.verify(auditManager);
  }

}
