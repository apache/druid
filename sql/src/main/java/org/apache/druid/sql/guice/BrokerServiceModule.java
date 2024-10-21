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

package org.apache.druid.sql.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provides;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.DiscoveryServiceLocator;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.guice.ServiceClientModule;
import org.apache.druid.sql.client.Broker;
import org.apache.druid.sql.client.BrokerClient;
import org.apache.druid.sql.client.BrokerClientImpl;

/**
 * Module that processes can install if they require a {@link BrokerClient}.
 * <p>
 * Similar to {@link ServiceClientModule}, but since {@link BrokerClient} depends
 * on classes from the sql module, this is a separate module within the sql package.
 * </p>
 */
public class BrokerServiceModule
{
  @Provides
  @LazySingleton
  @EscalatedGlobal
  public ServiceClientFactory makeServiceClientFactory(@EscalatedGlobal final HttpClient httpClient)
  {
    return ServiceClientModule.getServiceClientFactory(httpClient);
  }

  @Provides
  @ManageLifecycle
  @Broker
  public ServiceLocator makeBrokerServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
  }

  @Provides
  @LazySingleton
  public BrokerClient makeBrokerClient(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Broker final ServiceLocator serviceLocator
  )
  {
    return new BrokerClientImpl(
        clientFactory.makeClient(
            NodeRole.BROKER.getJsonName(),
            serviceLocator,
            StandardRetryPolicy.builder().maxAttempts(ServiceClientModule.CLIENT_MAX_ATTEMPTS).build()
        ),
        jsonMapper
    );
  }
}

