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
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicReference;

public class CoordinatorBrokerConfigsResourceTest
{
  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private BrokerDynamicConfigSyncer brokerDynamicConfigSyncer;

  @Before
  public void setUp() throws Exception
  {
    configManager = EasyMock.createStrictMock(JacksonConfigManager.class);
    auditManager = EasyMock.createStrictMock(AuditManager.class);
    brokerDynamicConfigSyncer = EasyMock.createNiceMock(BrokerDynamicConfigSyncer.class);
  }

  @Test
  public void testGetBrokerDynamicConfig()
  {
    BrokerDynamicConfig config = BrokerDynamicConfig.builder().build();
    AtomicReference<BrokerDynamicConfig> currentConfig = new AtomicReference<>(config);

    EasyMock.expect(
        configManager.watch(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject(BrokerDynamicConfig.class)
        )
    ).andReturn(currentConfig).once();

    EasyMock.replay(configManager, auditManager, brokerDynamicConfigSyncer);

    final Response response = new CoordinatorBrokerConfigsResource(
        configManager,
        auditManager,
        brokerDynamicConfigSyncer
    ).getBrokerDynamicConfig();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(config, response.getEntity());

    EasyMock.verify(configManager, auditManager, brokerDynamicConfigSyncer);
  }

  @Test
  public void testGetBrokerDynamicConfigHistory()
  {
    BrokerDynamicConfig config = BrokerDynamicConfig.builder().build();
    AtomicReference<BrokerDynamicConfig> currentConfig = new AtomicReference<>(config);

    EasyMock.expect(
        configManager.watch(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject(BrokerDynamicConfig.class)
        )
    ).andReturn(currentConfig).once();

    EasyMock.expect(
        auditManager.fetchAuditHistory(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(String.class),
            EasyMock.anyInt()
        )
    ).andReturn(ImmutableList.of()).once();

    EasyMock.replay(configManager, auditManager, brokerDynamicConfigSyncer);

    CoordinatorBrokerConfigsResource resource = new CoordinatorBrokerConfigsResource(
        configManager,
        auditManager,
        brokerDynamicConfigSyncer
    );

    Response response = resource.getBrokerDynamicConfigHistory(null, 10);
    Assert.assertEquals(200, response.getStatus());

    EasyMock.verify(configManager, auditManager, brokerDynamicConfigSyncer);
  }

  @Test
  public void testGetBrokerDynamicConfigHistoryWithNullIntervalAndCount()
  {
    BrokerDynamicConfig config = BrokerDynamicConfig.builder().build();
    AtomicReference<BrokerDynamicConfig> currentConfig = new AtomicReference<>(config);

    EasyMock.expect(
        configManager.watch(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject(BrokerDynamicConfig.class)
        )
    ).andReturn(currentConfig).once();

    EasyMock.expect(
        auditManager.fetchAuditHistory(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(String.class),
            EasyMock.eq((org.joda.time.Interval) null)
        )
    ).andReturn(ImmutableList.of()).once();

    EasyMock.replay(configManager, auditManager, brokerDynamicConfigSyncer);

    CoordinatorBrokerConfigsResource resource = new CoordinatorBrokerConfigsResource(
        configManager,
        auditManager,
        brokerDynamicConfigSyncer
    );

    Response response = resource.getBrokerDynamicConfigHistory(null, null);
    Assert.assertEquals(200, response.getStatus());

    EasyMock.verify(configManager, auditManager, brokerDynamicConfigSyncer);
  }

  @Test
  public void testSetBrokerDynamicConfig()
  {
    BrokerDynamicConfig config = BrokerDynamicConfig.builder().build();
    AtomicReference<BrokerDynamicConfig> currentConfig = new AtomicReference<>(config);
    HttpServletRequest request = EasyMock.createNiceMock(HttpServletRequest.class);

    EasyMock.expect(
        configManager.watch(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject(BrokerDynamicConfig.class)
        )
    ).andReturn(currentConfig).once();

    EasyMock.expect(
        configManager.set(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(BrokerDynamicConfig.class),
            EasyMock.anyObject(AuditInfo.class)
        )
    ).andReturn(SetResult.ok()).once();

    brokerDynamicConfigSyncer.queueBroadcastConfigToBrokers();
    EasyMock.expectLastCall().once();

    EasyMock.replay(configManager, auditManager, brokerDynamicConfigSyncer, request);

    CoordinatorBrokerConfigsResource resource = new CoordinatorBrokerConfigsResource(
        configManager,
        auditManager,
        brokerDynamicConfigSyncer
    );

    Response response = resource.setBrokerDynamicConfig(BrokerDynamicConfig.builder(), request);
    Assert.assertEquals(200, response.getStatus());

    EasyMock.verify(configManager, auditManager, brokerDynamicConfigSyncer);
  }
}
