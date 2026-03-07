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

package org.apache.druid.server;

import org.apache.druid.client.BrokerViewOfBrokerConfig;
import org.apache.druid.client.BrokerViewOfCoordinatorConfig;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.Response;

public class BrokerDynamicConfigResourceTest
{
  private BrokerViewOfCoordinatorConfig brokerViewOfCoordinatorConfig;
  private BrokerViewOfBrokerConfig brokerViewOfBrokerConfig;
  private BrokerDynamicConfigResource resource;

  @Before
  public void setUp()
  {
    brokerViewOfCoordinatorConfig = Mockito.mock(BrokerViewOfCoordinatorConfig.class);
    brokerViewOfBrokerConfig = Mockito.mock(BrokerViewOfBrokerConfig.class);
    resource = new BrokerDynamicConfigResource(
        brokerViewOfCoordinatorConfig,
        brokerViewOfBrokerConfig
    );
  }

  @Test
  public void testGetBrokerDynamicConfig()
  {
    BrokerDynamicConfig config = new BrokerDynamicConfig(null);
    Mockito.when(brokerViewOfBrokerConfig.getDynamicConfig()).thenReturn(config);

    Response response = resource.getBrokerDynamicConfig();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(config, response.getEntity());
    Mockito.verify(brokerViewOfBrokerConfig, Mockito.times(1)).getDynamicConfig();
  }

  @Test
  public void testSetBrokerDynamicConfig()
  {
    BrokerDynamicConfig config = new BrokerDynamicConfig(null);

    Response response = resource.setBrokerDynamicConfig(config);

    Assert.assertEquals(200, response.getStatus());
    Mockito.verify(brokerViewOfBrokerConfig, Mockito.times(1)).setDynamicConfig(config);
  }

  @Test
  public void testGetCoordinatorDynamicConfig()
  {
    CoordinatorDynamicConfig config = CoordinatorDynamicConfig.builder().build();
    Mockito.when(brokerViewOfCoordinatorConfig.getDynamicConfig()).thenReturn(config);

    Response response = resource.getDynamicConfig();

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(config, response.getEntity());
    Mockito.verify(brokerViewOfCoordinatorConfig, Mockito.times(1)).getDynamicConfig();
  }

  @Test
  public void testSetCoordinatorDynamicConfig()
  {
    CoordinatorDynamicConfig config = CoordinatorDynamicConfig.builder().build();

    Response response = resource.setDynamicConfig(config);

    Assert.assertEquals(200, response.getStatus());
    Mockito.verify(brokerViewOfCoordinatorConfig, Mockito.times(1)).setDynamicConfig(config);
  }
}
