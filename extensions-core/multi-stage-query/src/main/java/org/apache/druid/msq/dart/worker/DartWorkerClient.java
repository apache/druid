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
import it.unimi.dsi.fastutil.Pair;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.msq.dart.controller.DartWorkerManager;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
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
import org.apache.druid.utils.CloseableUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Dart implementation of {@link WorkerClient}. Uses the same {@link BaseWorkerClientImpl} as the task-based engine.
 * Each instance of this class is scoped to a single query.
 */
public class DartWorkerClient extends BaseWorkerClientImpl
{
  private static final Logger log = new Logger(DartWorkerClient.class);

  private final String queryId;
  private final ServiceClientFactory clientFactory;
  private final ServiceRetryPolicy retryPolicy;

  @Nullable
  private final String controllerHost;

  @GuardedBy("clientMap")
  private final Map<String, Pair<ServiceClient, Closeable>> clientMap = new HashMap<>();

  /**
   * Create a worker client.
   *
   * @param queryId        dart query ID. see {@link DartSqlEngine#CTX_DART_QUERY_ID}
   * @param clientFactory  service client factor
   * @param smileMapper    Smile object mapper
   * @param controllerHost Controller host (see {@link DartWorkerResource#HEADER_CONTROLLER_HOST}) if this is a
   *                       controller-to-worker client. Null if this is a worker-to-worker client.
   */
  public DartWorkerClient(
      final String queryId,
      final ServiceClientFactory clientFactory,
      final ObjectMapper smileMapper,
      @Nullable final String controllerHost
  )
  {
    super(smileMapper, SmileMediaTypes.APPLICATION_JACKSON_SMILE);
    this.queryId = queryId;
    this.clientFactory = clientFactory;
    this.controllerHost = controllerHost;

    if (controllerHost == null) {
      // worker -> worker client. Retry HTTP 503 in case worker A starts up before worker B, and needs to
      // contact it immediately.
      this.retryPolicy = new DartWorkerRetryPolicy(true);
    } else {
      // controller -> worker client. Do not retry any HTTP error codes. If we retry HTTP 503 for controller -> worker,
      // we can get stuck trying to contact workers that have exited.
      this.retryPolicy = new DartWorkerRetryPolicy(false);
    }
  }

  @Override
  protected ServiceClient getClient(final String workerIdString)
  {
    final WorkerId workerId = WorkerId.fromString(workerIdString);
    if (!queryId.equals(workerId.getQueryId())) {
      throw DruidException.defensive("Unexpected queryId[%s]. Expected queryId[%s]", workerId.getQueryId(), queryId);
    }

    synchronized (clientMap) {
      return clientMap.computeIfAbsent(workerId.getHostAndPort(), ignored -> makeNewClient(workerId)).left();
    }
  }

  /**
   * Close a single worker's clients. Used when that worker fails, so we stop trying to contact it.
   *
   * @param workerHost worker host:port
   */
  public void closeClient(final String workerHost)
  {
    synchronized (clientMap) {
      final Pair<ServiceClient, Closeable> clientPair = clientMap.remove(workerHost);
      if (clientPair != null) {
        CloseableUtils.closeAndWrapExceptions(clientPair.right());
      }
    }
  }

  /**
   * Close all outstanding clients.
   */
  @Override
  public void close()
  {
    synchronized (clientMap) {
      for (Map.Entry<String, Pair<ServiceClient, Closeable>> entry : clientMap.entrySet()) {
        CloseableUtils.closeAndSuppressExceptions(
            entry.getValue().right(),
            e -> log.warn(e, "Failed to close client[%s]", entry.getKey())
        );
      }

      clientMap.clear();
    }
  }

  /**
   * Stops a worker. Dart-only API, used by the {@link DartWorkerManager}.
   */
  public ListenableFuture<?> stopWorker(String workerId)
  {
    return getClient(workerId).asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/stop"),
        IgnoreHttpResponseHandler.INSTANCE
    );
  }

  /**
   * Create a new client. Called by {@link #getClient(String)} if a new one is needed.
   */
  private Pair<ServiceClient, Closeable> makeNewClient(final WorkerId workerId)
  {
    final URI uri = workerId.toUri();
    final FixedServiceLocator locator = new FixedServiceLocator(ServiceLocation.fromUri(uri));
    final ServiceClient baseClient =
        clientFactory.makeClient(workerId.toString(), locator, retryPolicy);
    final ServiceClient client;

    if (controllerHost != null) {
      client = new ControllerDecoratedClient(baseClient, controllerHost);
    } else {
      client = baseClient;
    }

    return Pair.of(client, locator);
  }

  /**
   * Service client that adds the {@link DartWorkerResource#HEADER_CONTROLLER_HOST} header.
   */
  private static class ControllerDecoratedClient implements ServiceClient
  {
    private final ServiceClient delegate;
    private final String controllerHost;

    ControllerDecoratedClient(final ServiceClient delegate, final String controllerHost)
    {
      this.delegate = delegate;
      this.controllerHost = controllerHost;
    }

    @Override
    public <IntermediateType, FinalType> ListenableFuture<FinalType> asyncRequest(
        final RequestBuilder requestBuilder,
        final HttpResponseHandler<IntermediateType, FinalType> handler
    )
    {
      return delegate.asyncRequest(
          requestBuilder.header(DartWorkerResource.HEADER_CONTROLLER_HOST, controllerHost),
          handler
      );
    }

    @Override
    public ServiceClient withRetryPolicy(final ServiceRetryPolicy retryPolicy)
    {
      return new ControllerDecoratedClient(delegate.withRetryPolicy(retryPolicy), controllerHost);
    }
  }
}
