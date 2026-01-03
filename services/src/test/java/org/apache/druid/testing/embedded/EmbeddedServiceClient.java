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

package org.apache.druid.testing.embedded;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.broker.Broker;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceClientFactoryImpl;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.guice.ServiceClientModule;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.security.Escalator;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Client to make requests to various services in an embedded test cluster.
 *
 * @see #onLeaderOverlord(Function)
 * @see #onLeaderCoordinator(Function)
 * @see #onAnyBroker(Function)
 */
public class EmbeddedServiceClient
{
  private final EmbeddedDruidCluster cluster;
  private final ServiceClientModule module;

  private final ServiceClient coordinatorServiceClient;
  private final ServiceClient overlordServiceClient;
  private final ServiceClient brokerServiceClient;

  private final ScheduledExecutorService clientConnectExec;
  private final StatusResponseHandler responseHandler;

  private EmbeddedServiceClient(EmbeddedDruidCluster cluster, Escalator escalator)
  {
    // Use a ServiceClientModule to create the various clients
    this.module = new ServiceClientModule();
    this.cluster = cluster;
    this.clientConnectExec = ScheduledExecutors.fixed(4, "ServiceClientFactory-%d");
    this.responseHandler = StatusResponseHandler.getInstance();

    // If this server is stopped, the client becomes invalid
    final EmbeddedDruidServer<?> anyServer = cluster.anyServer();

    final HttpClient escalatedHttpClient =
        escalator == null
        ? anyServer.bindings().escalatedHttpClient()
        : escalator.createEscalatedClient(anyServer.bindings().globalHttpClient());

    // Create service clients
    final ServiceClientFactory factory = new ServiceClientFactoryImpl(escalatedHttpClient, clientConnectExec);
    this.overlordServiceClient = module.makeServiceClientForOverlord(
        factory,
        anyServer.bindings().getInstance(ServiceLocator.class, IndexingService.class)
    );

    this.brokerServiceClient = module.makeServiceClientForBroker(
        factory,
        anyServer.bindings().getInstance(ServiceLocator.class, Broker.class)
    );
    this.coordinatorServiceClient = module.makeServiceClientForCoordinator(
        factory,
        anyServer.bindings().getInstance(ServiceLocator.class, Coordinator.class)
    );
  }

  /**
   * Creates a client that uses the {@link Escalator} bound to the embedded servers.
   */
  public static EmbeddedServiceClient create(EmbeddedDruidCluster cluster)
  {
    return new EmbeddedServiceClient(cluster, null);
  }

  /**
   * Creates a client using the specified {@link Escalator}. All requests made by this
   * client will use the given escalator.
   */
  public static EmbeddedServiceClient create(EmbeddedDruidCluster cluster, Escalator escalator)
  {
    return new EmbeddedServiceClient(cluster, escalator);
  }

  /**
   * Stops the executor service used by this client.
   */
  public void stop() throws InterruptedException
  {
    clientConnectExec.shutdownNow();
    clientConnectExec.awaitTermination(1, TimeUnit.MINUTES);
  }

  @Nullable
  public <T> T onLeaderCoordinator(
      Function<ObjectMapper, RequestBuilder> request,
      TypeReference<T> resultType
  )
  {
    return makeRequest(request, resultType, coordinatorServiceClient, getMapper(EmbeddedCoordinator.class));
  }

  public <T> T onLeaderCoordinator(Function<CoordinatorClient, ListenableFuture<T>> coordinatorApi)
  {
    return getResult(coordinatorApi.apply(createCoordinatorClient()));
  }

  public <T> T onLeaderCoordinatorSync(Function<CoordinatorClient, T> coordinatorApi)
  {
    return coordinatorApi.apply(createCoordinatorClient());
  }

  public <T> T onLeaderOverlord(Function<OverlordClient, ListenableFuture<T>> overlordApi)
  {
    return getResult(overlordApi.apply(createOverlordClient()));
  }

  @Nullable
  public <T> T onLeaderOverlord(
      Function<ObjectMapper, RequestBuilder> request,
      TypeReference<T> resultType
  )
  {
    return makeRequest(request, resultType, overlordServiceClient, getMapper(EmbeddedOverlord.class));
  }

  public <T> T onAnyBroker(Function<BrokerClient, ListenableFuture<T>> brokerApi)
  {
    return getResult(brokerApi.apply(createBrokerClient()));
  }

  public <T> T onAnyBroker(
      Function<ObjectMapper, RequestBuilder> request,
      TypeReference<T> resultType
  )
  {
    return makeRequest(request, resultType, brokerServiceClient, getMapper(EmbeddedBroker.class));
  }

  @Nullable
  private <T> T makeRequest(
      Function<ObjectMapper, RequestBuilder> request,
      TypeReference<T> resultType,
      ServiceClient serviceClient,
      ObjectMapper mapper
  )
  {
    final RequestBuilder requestBuilder = request.apply(mapper);

    try {
      StatusResponseHolder response = serviceClient.request(requestBuilder, responseHandler);
      if (!response.getStatus().equals(HttpResponseStatus.OK)
        && !response.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
        throw new ISE(
            "Request[%s] failed with status[%s] content[%s].",
            requestBuilder.toString(),
            response.getStatus(),
            response.getContent()
        );
      }

      if (resultType == null) {
        return null;
      } else {
        return mapper.readValue(response.getContent(), resultType);
      }
    }
    catch (Exception e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private CoordinatorClient createCoordinatorClient()
  {
    return module.makeCoordinatorClient(getMapper(EmbeddedCoordinator.class), coordinatorServiceClient);
  }

  private OverlordClient createOverlordClient()
  {
    return module.makeOverlordClient(getMapper(EmbeddedOverlord.class), overlordServiceClient);
  }

  private BrokerClient createBrokerClient()
  {
    return module.makeBrokerClient(getMapper(EmbeddedBroker.class), brokerServiceClient);
  }

  private <S extends EmbeddedDruidServer<S>> ObjectMapper getMapper(Class<S> serverType)
  {
    return cluster.findServerOfType(serverType).bindings().jsonMapper();
  }

  private static <T> T getResult(ListenableFuture<T> future)
  {
    return FutureUtils.getUnchecked(future, true);
  }
}
