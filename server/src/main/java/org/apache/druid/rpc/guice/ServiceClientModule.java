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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.client.broker.Broker;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.broker.BrokerClientImpl;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.CoordinatorClientImpl;
import org.apache.druid.client.coordinator.CoordinatorServiceClient;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.DiscoveryServiceLocator;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceClientFactoryImpl;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.rpc.indexing.OverlordClientImpl;

import java.util.concurrent.ScheduledExecutorService;

public class ServiceClientModule implements DruidModule
{
  private static final int CLIENT_MAX_ATTEMPTS = 6;
  private static final int CONNECT_EXEC_THREADS = 4;

  @Override
  public void configure(Binder binder)
  {
    // Nothing to do.
  }

  @Provides
  @LazySingleton
  @EscalatedGlobal
  public ServiceClientFactory getServiceClientFactory(@EscalatedGlobal final HttpClient httpClient)
  {
    return makeServiceClientFactory(httpClient);
  }

  @Provides
  @ManageLifecycle
  @IndexingService
  public ServiceLocator makeOverlordServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.OVERLORD);
  }

  @Provides
  @IndexingService
  public ServiceClient makeServiceClientForOverlord(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @IndexingService final ServiceLocator serviceLocator
  )
  {
    return clientFactory.makeClient(
        NodeRole.OVERLORD.getJsonName(),
        serviceLocator,
        StandardRetryPolicy.builder().maxAttempts(CLIENT_MAX_ATTEMPTS).build()
    );
  }

  @Provides
  @LazySingleton
  public OverlordClient makeOverlordClient(
      @Json final ObjectMapper jsonMapper,
      @IndexingService final ServiceClient serviceClient
  )
  {
    return new OverlordClientImpl(serviceClient, jsonMapper);
  }

  @Provides
  @ManageLifecycle
  @Coordinator
  public ServiceLocator makeCoordinatorServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.COORDINATOR);
  }

  @Provides
  @Coordinator
  public ServiceClient makeServiceClientForCoordinator(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator
  )
  {
    return clientFactory.makeClient(
        NodeRole.COORDINATOR.getJsonName(),
        serviceLocator,
        StandardRetryPolicy.builder().maxAttempts(CLIENT_MAX_ATTEMPTS).build()
    );
  }

  @Provides
  @LazySingleton
  public CoordinatorClient makeCoordinatorClient(
      @Json final ObjectMapper jsonMapper,
      @Coordinator final ServiceClient serviceClient
  )
  {
    return new CoordinatorClientImpl(serviceClient, jsonMapper);
  }

  /**
   * Creates a {@link CoordinatorServiceClient} used by extensions to send
   * requests to the Coordinator. For core Coordinator APIs,
   * {@link CoordinatorClient} should be used instead.
   */
  @Provides
  @LazySingleton
  public static CoordinatorServiceClient createCoordinatorServiceClient(
      @Coordinator final ServiceClient serviceClient
  )
  {
    return new CoordinatorServiceClient(serviceClient);
  }

  @Provides
  @ManageLifecycle
  @Broker
  public ServiceLocator makeBrokerServiceLocator(final DruidNodeDiscoveryProvider discoveryProvider)
  {
    return new DiscoveryServiceLocator(discoveryProvider, NodeRole.BROKER);
  }

  @Provides
  @Broker
  public ServiceClient makeServiceClientForBroker(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Broker final ServiceLocator serviceLocator
  )
  {
    return clientFactory.makeClient(
        NodeRole.BROKER.getJsonName(),
        serviceLocator,
        StandardRetryPolicy.builder().maxAttempts(ServiceClientModule.CLIENT_MAX_ATTEMPTS).build()
    );
  }

  @Provides
  @LazySingleton
  public BrokerClient makeBrokerClient(
      @Json final ObjectMapper jsonMapper,
      @Broker final ServiceClient serviceClient
  )
  {
    return new BrokerClientImpl(serviceClient, jsonMapper);
  }

  public static ServiceClientFactory makeServiceClientFactory(@EscalatedGlobal final HttpClient httpClient)
  {
    final ScheduledExecutorService connectExec =
        ScheduledExecutors.fixed(CONNECT_EXEC_THREADS, "ServiceClientFactory-%d");
    return new ServiceClientFactoryImpl(httpClient, connectExec);
  }
}
