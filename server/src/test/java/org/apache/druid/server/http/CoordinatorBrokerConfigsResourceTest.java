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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.concurrent.atomic.AtomicReference;

public class CoordinatorBrokerConfigsResourceTest
{
  private JacksonConfigManager configManager;
  private AuditManager auditManager;
  private ServiceClientFactory clientFactory;
  private ObjectMapper jsonMapper;
  private DruidNodeDiscoveryProvider druidNodeDiscovery;

  @Before
  public void setUp() throws Exception
  {
    configManager = EasyMock.createStrictMock(JacksonConfigManager.class);
    auditManager = EasyMock.createStrictMock(AuditManager.class);
    clientFactory = EasyMock.createNiceMock(ServiceClientFactory.class);
    jsonMapper = new DefaultObjectMapper();
    druidNodeDiscovery = EasyMock.createStrictMock(DruidNodeDiscoveryProvider.class);
  }

  @Test
  public void testGetBrokerDynamicConfig()
  {
    BrokerDynamicConfig config = new BrokerDynamicConfig(null);
    AtomicReference<BrokerDynamicConfig> currentConfig = new AtomicReference<>(config);

    EasyMock.expect(
        configManager.watch(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject(BrokerDynamicConfig.class)
        )
    ).andReturn(currentConfig).once();

    EasyMock.replay(configManager, auditManager, druidNodeDiscovery);

    final Response response = new CoordinatorBrokerConfigsResource(
        configManager,
        auditManager,
        clientFactory,
        jsonMapper,
        druidNodeDiscovery
    ).getBrokerDynamicConfig();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(config, response.getEntity());

    EasyMock.verify(configManager, auditManager, druidNodeDiscovery);
  }

  @Test
  public void testGetBrokerDynamicConfigHistory()
  {
    BrokerDynamicConfig config = new BrokerDynamicConfig(null);
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

    EasyMock.replay(configManager, auditManager, druidNodeDiscovery);

    CoordinatorBrokerConfigsResource resource = new CoordinatorBrokerConfigsResource(
        configManager,
        auditManager,
        clientFactory,
        jsonMapper,
        druidNodeDiscovery
    );

    Response response = resource.getBrokerDynamicConfigHistory(10);
    Assert.assertEquals(200, response.getStatus());

    EasyMock.verify(configManager, auditManager, druidNodeDiscovery);
  }

  @Test
  public void testGetBrokerDynamicConfigHistoryMissingCount()
  {
    BrokerDynamicConfig config = new BrokerDynamicConfig(null);
    AtomicReference<BrokerDynamicConfig> currentConfig = new AtomicReference<>(config);

    EasyMock.expect(
        configManager.watch(
            EasyMock.anyObject(String.class),
            EasyMock.anyObject(Class.class),
            EasyMock.anyObject(BrokerDynamicConfig.class)
        )
    ).andReturn(currentConfig).once();

    EasyMock.replay(configManager, auditManager, druidNodeDiscovery);

    CoordinatorBrokerConfigsResource resource = new CoordinatorBrokerConfigsResource(
        configManager,
        auditManager,
        clientFactory,
        jsonMapper,
        druidNodeDiscovery
    );

    Response response = resource.getBrokerDynamicConfigHistory(null);
    Assert.assertEquals(400, response.getStatus());
    Assert.assertEquals("count parameter is required", response.getEntity());

    EasyMock.verify(configManager, auditManager, druidNodeDiscovery);
  }
}
