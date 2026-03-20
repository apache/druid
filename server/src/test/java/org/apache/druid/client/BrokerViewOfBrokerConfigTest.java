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

package org.apache.druid.client;

import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class BrokerViewOfBrokerConfigTest
{
  private BrokerViewOfBrokerConfig target;

  private CoordinatorClient coordinatorClient;
  private BrokerDynamicConfig config;


  @Before
  public void setUp() throws Exception
  {
    config = BrokerDynamicConfig.builder().build();
    coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.getBrokerDynamicConfig()).thenReturn(Futures.immediateFuture(config));
    target = new BrokerViewOfBrokerConfig(coordinatorClient);
  }

  @Test
  public void testFetchesConfigOnStartup()
  {
    target.start();
    Mockito.verify(coordinatorClient, Mockito.times(1)).getBrokerDynamicConfig();
    Assert.assertEquals(config, target.getDynamicConfig());
  }
}
