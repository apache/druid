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

package org.apache.druid.rpc.guice;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertNotNull;

public class ServiceClientModuleTest
{
  private Injector injector;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock
  private HttpClient httpClient;

  @Mock
  private DruidNodeDiscoveryProvider discoveryProvider;

  @Mock
  private ServiceLocator serviceLocator;

  @Mock
  private ServiceClientFactory serviceClientFactory;

  @Before
  public void setUp()
  {
    injector = Guice.createInjector(
        ImmutableList.of(
            new DruidGuiceExtensions(),
            new LifecycleModule(),
          new JacksonModule(),
          new ServiceClientModule(),
          binder -> {
            binder.bind(HttpClient.class).annotatedWith(EscalatedGlobal.class).toInstance(httpClient);
            binder.bind(ServiceLocator.class).toInstance(serviceLocator);
            binder.bind(DruidNodeDiscoveryProvider.class).toInstance(discoveryProvider);
            binder.bind(ServiceClientFactory.class).toInstance(serviceClientFactory);
          }
          )
    );
  }

  @Test
  public void testGetServiceClientFactory()
  {
    assertNotNull(injector.getInstance(ServiceClientFactory.class));
  }

  @Test
  public void testGetOverlordClient()
  {
    assertNotNull(injector.getInstance(OverlordClient.class));
  }

  @Test
  public void testGetCoordinatorClient()
  {
    assertNotNull(injector.getInstance(CoordinatorClient.class));
  }

  @Test
  public void testGetBrokerClient()
  {
    assertNotNull(injector.getInstance(BrokerClient.class));
  }
}
