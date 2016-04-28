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

package io.druid.server.http;

import com.google.common.collect.ImmutableList;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.metadata.MetadataRuleManager;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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
        new DateTime("2013-01-02T00:00:00Z")
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
        new DateTime("2013-01-01T00:00:00Z")
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
    Interval theInterval = new Interval(interval);
    AuditEntry entry1 = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        new DateTime("2013-01-02T00:00:00Z")
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
        new DateTime("2013-01-01T00:00:00Z")
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
        new DateTime("2013-01-02T00:00:00Z")
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
        new DateTime("2013-01-01T00:00:00Z")
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
    Interval theInterval = new Interval(interval);
    AuditEntry entry1 = new AuditEntry(
        "testKey",
        "testType",
        new AuditInfo(
            "testAuthor",
            "testComment",
            "127.0.0.1"
        ),
        "testPayload",
        new DateTime("2013-01-02T00:00:00Z")
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
        new DateTime("2013-01-01T00:00:00Z")
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
