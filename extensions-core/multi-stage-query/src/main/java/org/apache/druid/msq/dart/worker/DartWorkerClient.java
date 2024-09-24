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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.msq.dart.controller.ControllerServerId;
import org.apache.druid.msq.dart.controller.DartWorkerManager;
import org.apache.druid.msq.dart.worker.http.DartWorkerResource;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.rpc.BaseWorkerClientImpl;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Dart implementation of {@link WorkerClient}. Uses the same {@link BaseWorkerClientImpl} as the task-based engine.
 */
public class DartWorkerClient extends BaseWorkerClientImpl
{
  private final ServiceClientFactory clientFactory;

  @Nullable
  private final ControllerServerId controllerServerId;

  @GuardedBy("clientMap")
  private final Map<String, ServiceClient> clientMap = new HashMap<>();

  /**
   * Create a worker client.
   *
   * @param clientFactory      service client factor
   * @param smileMapper        Smile object mapper
   * @param controllerServerId Controller ID (see {@link DartWorkerResource#HEADER_CONTROLLER_ID}) if this is a
   *                           controller-to-worker client. Null if this is a worker-to-worker client.
   */
  public DartWorkerClient(
      final ServiceClientFactory clientFactory,
      final ObjectMapper smileMapper,
      @Nullable final ControllerServerId controllerServerId
  )
  {
    super(smileMapper, SmileMediaTypes.APPLICATION_JACKSON_SMILE);
    this.clientFactory = clientFactory;
    this.controllerServerId = controllerServerId;
  }

  @Override
  protected ServiceClient getClient(String workerId)
  {
    synchronized (clientMap) {
      return clientMap.computeIfAbsent(
          workerId,
          id -> {
            final URI uri = WorkerId.fromString(id).toUri();
            final ServiceLocation location = ServiceLocation.fromUri(uri);
            final FixedServiceLocator locator = new FixedServiceLocator(location);
            final ServiceClient client = clientFactory.makeClient(id, locator, DartWorkerRetryPolicy.INSTANCE);

            if (controllerServerId != null) {
              return new ControllerDecoratedClient(client, controllerServerId);
            } else {
              return client;
            }
          }
      );
    }
  }

  @Override
  public void close()
  {
    synchronized (clientMap) {
      clientMap.clear();
    }
  }

  /**
   * Stops a worker. Dart-only API, used by the {@link DartWorkerManager#toString()}.
   */
  public ListenableFuture<?> stopWorker(String workerId)
  {
    return getClient(workerId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/stop"),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  /**
   * Service client that adds the {@link DartWorkerResource#HEADER_CONTROLLER_ID} header.
   */
  private static class ControllerDecoratedClient implements ServiceClient
  {
    private final ServiceClient delegate;
    private final ControllerServerId controllerServerId;

    ControllerDecoratedClient(final ServiceClient delegate, final ControllerServerId controllerServerId)
    {
      this.delegate = delegate;
      this.controllerServerId = controllerServerId;
    }

    @Override
    public <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
        final RequestBuilder requestBuilder,
        final HttpResponseHandler<IntermediateType, FinalType> handler
    )
    {
      return delegate.asyncRequest(
          requestBuilder.header(DartWorkerResource.HEADER_CONTROLLER_ID, controllerServerId.toString()),
          handler
      );
    }

    @Override
    public ServiceClient withRetryPolicy(final ServiceRetryPolicy retryPolicy)
    {
      return new ControllerDecoratedClient(delegate.withRetryPolicy(retryPolicy), controllerServerId);
    }
  }
}
