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

package org.apache.druid.msq.dart.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.msq.dart.guice.DartControllerConfig;
import org.apache.druid.msq.dart.worker.DartWorkerClientImpl;
import org.apache.druid.msq.dart.worker.DartWorkerRunner;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.dart.worker.http.DartWorkerResource;
import org.apache.druid.msq.dart.worker.http.SyncWorkersRequest;
import org.apache.druid.msq.dart.worker.http.SyncWorkersResponse;
import org.apache.druid.msq.exec.ControllerHolder;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Sends periodic syncs from the Broker to the servers that run Dart workers. Each sync sends a
 * {@link SyncWorkersRequest} with the list of queries the Broker believes are running on that worker. The worker
 * replies with a {@link SyncWorkersResponse} indicating what queries the worker is currently running. This allows
 * discrepancies to be resolved.
 *
 * @see DartWorkerRunner#syncController the worker side
 */
public class DartWorkerSyncer
{
  private static final Logger log = new Logger(DartWorkerSyncer.class);

  /**
   * Timeout for each sync request.
   */
  static final long SYNC_TIMEOUT_MILLIS = 10_000;

  private final DartControllerRegistry controllerRegistry;
  private final Supplier<DruidNodeDiscovery> workerDiscovery;
  private final ServiceClientFactory clientFactory;
  private final ObjectMapper jsonMapper;
  private final String selfHost;
  private final Duration syncPeriod;

  /**
   * Worker host:port -> state of syncs to that server.
   */
  @GuardedBy("servers")
  private final Map<String, ServerHolder> servers = new HashMap<>();

  @Nullable
  private ScheduledExecutorService exec;

  public DartWorkerSyncer(
      final DartControllerRegistry controllerRegistry,
      final Supplier<DruidNodeDiscovery> workerDiscovery,
      final ServiceClientFactory clientFactory,
      final ObjectMapper jsonMapper,
      final String selfHost,
      final DartControllerConfig config
  )
  {
    this.controllerRegistry = controllerRegistry;
    this.workerDiscovery = Suppliers.memoize(workerDiscovery);
    this.clientFactory = clientFactory;
    this.jsonMapper = jsonMapper;
    this.selfHost = selfHost;
    this.syncPeriod = config.getSyncPeriod().toStandardDuration();
    if (syncPeriod.getMillis() <= 0) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("druid.msq.dart.controller.syncPeriod[%s] must be positive", config.getSyncPeriod());
    }
  }

  @LifecycleStart
  public void start()
  {
    exec = Execs.scheduledSingleThreaded("dart-worker-syncer-%s");
    ScheduledExecutors.scheduleWithFixedDelay(exec, syncPeriod, syncPeriod, this::sync);
  }

  @LifecycleStop
  public void stop()
  {
    if (exec != null) {
      exec.shutdownNow();
    }

    synchronized (servers) {
      for (final ServerHolder server : servers.values()) {
        server.close();
      }

      servers.clear();
    }
  }

  /**
   * Sends a sync to each discovered server.
   */
  void sync()
  {
    // Collect the queries each server is running for us, according to our controllers.
    final Map<String, Set<String>> queryIdsByServer = new HashMap<>(); // workerHost -> queryId

    for (final ControllerHolder holder : controllerRegistry.getAllControllers()) {
      for (final String workerIdString : holder.getController().getWorkerIds()) {
        final WorkerId workerId = WorkerId.fromString(workerIdString);
        queryIdsByServer.computeIfAbsent(workerId.getHostAndPort(), _ -> new HashSet<>()).add(workerId.getQueryId());
      }
    }

    // Collect the servers that are currently present in service discovery.
    final Map<String, DruidNode> discoveredServers = new HashMap<>(); // workerHost -> node
    for (final DiscoveryDruidNode node : workerDiscovery.get().getAllNodes()) {
      discoveredServers.put(node.getDruidNode().getHostAndPortToUse(), node.getDruidNode());
    }

    synchronized (servers) {
      for (final Map.Entry<String, DruidNode> entry : discoveredServers.entrySet()) {
        servers.computeIfAbsent(entry.getKey(), _ -> makeServerHolder(entry.getValue()));
      }

      // Stop tracking servers that are gone. Note that this treats leaving discovery as lack of liveness, which must
      // change once worker liveness is separated from discovery.
      final Iterator<Map.Entry<String, ServerHolder>> it = servers.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<String, ServerHolder> entry = it.next();
        if (!discoveredServers.containsKey(entry.getKey())) {
          entry.getValue().close();
          it.remove();
        }
      }

      // Sync the remaining servers.
      for (final Map.Entry<String, ServerHolder> entry : servers.entrySet()) {
        final String serverHost = entry.getKey();
        final ServerHolder server = entry.getValue();

        if (server.inFlight == null || server.inFlight.isDone()) {
          sendOneSync(serverHost, server, queryIdsByServer.getOrDefault(serverHost, Set.of()));
        }
      }
    }
  }

  /**
   * Sends a {@link SyncWorkersRequest} to a particular worker server. Called by {@link #sync()}.
   */
  @GuardedBy("servers")
  private void sendOneSync(final String serverHost, final ServerHolder server, final Set<String> queryIds)
  {
    final ListenableFuture<BytesFullResponseHolder> future = server.client.asyncRequest(
        new RequestBuilder(HttpMethod.POST, "/sync")
            .jsonContent(jsonMapper, new SyncWorkersRequest(queryIds))
            .timeout(Duration.millis(SYNC_TIMEOUT_MILLIS)),
        new BytesFullResponseHandler()
    );

    // Mark this server as having an in-flight request, so we don't send another until this one is done.
    server.inFlight = future;

    Futures.addCallback(
        future,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(final BytesFullResponseHolder result)
          {
            // Nothing to do.
          }

          @Override
          public void onFailure(final Throwable e)
          {
            log.noStackTrace().warn(e, "Failed to sync server[%s]", serverHost);
          }
        },
        Execs.directExecutor()
    );
  }

  private ServerHolder makeServerHolder(final DruidNode node)
  {
    final FixedServiceLocator locator =
        new FixedServiceLocator(ServiceLocation.fromDruidNode(node).withBasePath(DartWorkerResource.PATH));
    final ServiceClient client = new DartWorkerClientImpl.ControllerDecoratedClient(
        clientFactory.makeClient(node.getHostAndPortToUse(), locator, StandardRetryPolicy.noRetries()),
        selfHost
    );

    return new ServerHolder(client, locator);
  }

  /**
   * Sync state for a single server. Guarded by {@link DartWorkerSyncer#servers}.
   */
  private static class ServerHolder
  {
    private final ServiceClient client;
    private final ServiceLocator locator;

    /**
     * Currently in-flight sync future from {@link #sendOneSync}.
     */
    @Nullable
    private ListenableFuture<?> inFlight;

    ServerHolder(final ServiceClient client, final ServiceLocator locator)
    {
      this.client = client;
      this.locator = locator;
    }

    void close()
    {
      if (inFlight != null) {
        inFlight.cancel(true);
      }

      CloseableUtils.closeAndSuppressExceptions(locator, e -> log.warn(e, "Failed to close locator"));
    }
  }
}
