/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DataNodeService;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.coordination.DataSegmentChangeCallback;
import io.druid.server.coordination.DataSegmentChangeHandler;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.SegmentChangeRequestHistory;
import io.druid.server.coordination.SegmentChangeRequestsSnapshot;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * This class uses CuratorInventoryManager to listen for queryable server membership which serve segments(e.g. Historicals).
 * For each queryable server, it uses HTTP GET /druid-internal/v1/segments (see docs in SegmentListerResource.getSegments(..).
 */
public class HttpServerInventoryView implements ServerInventoryView, FilteredServerInventoryView
{
  private final EmittingLogger log = new EmittingLogger(HttpServerInventoryView.class);
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ConcurrentMap<ServerRemovedCallback, Executor> serverCallbacks = new MapMaker().makeMap();
  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks = new MapMaker().makeMap();

  private final ConcurrentMap<SegmentCallback, Predicate<Pair<DruidServerMetadata, DataSegment>>> segmentPredicates = new MapMaker()
      .makeMap();
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter;
  private volatile Predicate<Pair<DruidServerMetadata, DataSegment>> finalPredicate;

  // For each queryable server, a name -> DruidServerHolder entry is kept
  private final ConcurrentHashMap<String, DruidServerHolder> servers = new ConcurrentHashMap<>();

  private volatile ExecutorService executor;

  // the work queue, all items in this are sequentially processed by main thread setup in start()
  // used to call inventoryInitialized on all SegmentCallbacks and
  // for keeping segment list for each queryable server uptodate.
  private final BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>();

  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final HttpServerInventoryViewConfig config;

  @Inject
  public HttpServerInventoryView(
      final @Json ObjectMapper jsonMapper,
      final @Smile ObjectMapper smileMapper,
      final @Global HttpClient httpClient,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter,
      final HttpServerInventoryViewConfig config
  )
  {
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.defaultFilter = defaultFilter;
    this.finalPredicate = defaultFilter;
    this.config = config;
  }


  @LifecycleStart
  public void start() throws Exception
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStart()) {
        throw new ISE("can't start.");
      }

      log.info("Starting HttpServerInventoryView.");

      try {
        executor = Executors.newFixedThreadPool(
            config.getNumThreads(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("HttpServerInventoryView-%s").build()
        );

        executor.execute(
            new Runnable()
            {
              @Override
              public void run()
              {
                if (!lifecycleLock.awaitStarted()) {
                  log.error("WTF! lifecycle not started, segments will not be discovered.");
                  return;
                }

                while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                  try {
                    queue.take().run();
                  }
                  catch (InterruptedException ex) {
                    log.info("main thread interrupted, served segments list is not synced anymore.");
                    Thread.currentThread().interrupt();
                  }
                  catch (Throwable th) {
                    log.makeAlert(th, "main thread ignored error").emit();
                  }
                }

                log.info("HttpServerInventoryView main thread exited.");
              }
            }
        );

        DruidNodeDiscovery druidNodeDiscovery = druidNodeDiscoveryProvider.getForService(DataNodeService.DISCOVERY_SERVICE_KEY);
        druidNodeDiscovery.registerListener(
            new DruidNodeDiscovery.Listener()
            {
              private volatile boolean initialized = false;

              @Override
              public void nodesAdded(List<DiscoveryDruidNode> nodes)
              {
                nodes.forEach(
                    node -> serverAddedOrUpdated(toDruidServer(node))
                );

                if (!initialized) {
                  initialized = true;
                  queue.add(HttpServerInventoryView.this::serverInventoryInitialized);
                }
              }

              @Override
              public void nodesRemoved(List<DiscoveryDruidNode> nodes)
              {
                nodes.forEach(
                    node -> serverRemoved(toDruidServer(node))
                );
              }

              private DruidServer toDruidServer(DiscoveryDruidNode node)
              {

                return new DruidServer(
                    node.getDruidNode().getHostAndPortToUse(),
                    node.getDruidNode().getHostAndPort(),
                    node.getDruidNode().getHostAndTlsPort(),
                    ((DataNodeService) node.getServices().get(DataNodeService.DISCOVERY_SERVICE_KEY)).getMaxSize(),
                    ((DataNodeService) node.getServices().get(DataNodeService.DISCOVERY_SERVICE_KEY)).getType(),
                    ((DataNodeService) node.getServices().get(DataNodeService.DISCOVERY_SERVICE_KEY)).getTier(),
                    ((DataNodeService) node.getServices().get(DataNodeService.DISCOVERY_SERVICE_KEY)).getPriority()
                );
              }
            }
        );

        log.info("Started HttpServerInventoryView.");
        lifecycleLock.started();
      }
      finally {
        lifecycleLock.exitStart();
      }
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStop()) {
        throw new ISE("can't stop.");
      }

      log.info("Stopping HttpServerInventoryView.");

      if (executor != null) {
        executor.shutdownNow();
        executor = null;
      }

      queue.clear();

      log.info("Stopped HttpServerInventoryView.");
    }
  }

  @Override
  public void registerSegmentCallback(
      Executor exec, SegmentCallback callback, Predicate<Pair<DruidServerMetadata, DataSegment>> filter
  )
  {
    segmentCallbacks.put(callback, exec);
    segmentPredicates.put(callback, filter);

    finalPredicate = Predicates.or(
        defaultFilter,
        Predicates.or(segmentPredicates.values())
    );
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    serverCallbacks.put(callback, exec);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    segmentCallbacks.put(callback, exec);
  }

  @Override
  public DruidServer getInventoryValue(String containerKey)
  {
    DruidServerHolder holder = servers.get(containerKey);
    if (holder != null) {
      return holder.druidServer;
    }
    return null;
  }

  @Override
  public Iterable<DruidServer> getInventory()
  {
    return Iterables.transform(
        servers.values(), new Function<DruidServerHolder, DruidServer>()
        {
          @Override
          public DruidServer apply(DruidServerHolder input)
          {
            return input.druidServer;
          }
        }
    );
  }

  private void runSegmentCallbacks(
      final Function<SegmentCallback, CallbackAction> fn
  )
  {
    for (final Map.Entry<SegmentCallback, Executor> entry : segmentCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == fn.apply(entry.getKey())) {
                segmentCallbacks.remove(entry.getKey());
                if (segmentPredicates.remove(entry.getKey()) != null) {
                  finalPredicate = Predicates.or(
                      defaultFilter,
                      Predicates.or(segmentPredicates.values())
                  );
                }
              }
            }
          }
      );
    }
  }

  private void runServerCallbacks(final DruidServer server)
  {
    for (final Map.Entry<ServerRemovedCallback, Executor> entry : serverCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == entry.getKey().serverRemoved(server)) {
                serverCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }

  //best effort wait for first segment listing fetch from all servers and then call
  //segmentViewInitialized on all registered segment callbacks.
  private void serverInventoryInitialized()
  {
    for (DruidServerHolder server : servers.values()) {
      server.awaitInitialization();
    }

    log.info("Calling SegmentCallback.segmentViewInitialized() for all callbacks.");

    runSegmentCallbacks(
        new Function<SegmentCallback, CallbackAction>()
        {
          @Override
          public CallbackAction apply(SegmentCallback input)
          {
            return input.segmentViewInitialized();
          }
        }
    );
  }

  private DruidServer serverAddedOrUpdated(DruidServer server)
  {
    DruidServerHolder newHolder = servers.compute(
        server.getName(),
        (k, v) -> v == null ? new DruidServerHolder(server) : v.updatedHolder(server)
    );
    newHolder.updateSegmentsListAsync();
    return newHolder.druidServer;
  }

  private void serverRemoved(DruidServer server)
  {
    servers.remove(server.getName());
  }

  @Override
  public boolean isStarted()
  {
    return lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS);
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    DruidServerHolder holder = servers.get(serverKey);
    return holder != null && holder.druidServer.getSegment(segment.getIdentifier()) != null;
  }

  private class DruidServerHolder
  {
    private final Object lock = new Object();

    //lock is used to keep state in counter and and segment list in druidServer consistent
    // so that in "updateHolder()" method, new DruidServerHolder with updated DruidServer info
    // can be safely created
    private final DruidServer druidServer;

    private volatile SegmentChangeRequestHistory.Counter counter = null;

    private final HostAndPort serverHostAndPort;

    private final DataSegmentChangeHandler changeHandler;
    private final long serverHttpTimeout = config.getServerTimeout() + 1000;

    private final CountDownLatch initializationLatch = new CountDownLatch(1);

    private volatile boolean isUnstable = false;
    private volatile long unstableStartTime = -1;

    DruidServerHolder(DruidServer druidServer)
    {
      this(druidServer, null);
    }

    private DruidServerHolder(final DruidServer druidServer, final SegmentChangeRequestHistory.Counter counter)
    {
      this.druidServer = druidServer;
      this.serverHostAndPort = HostAndPort.fromString(druidServer.getHost());
      this.counter = counter;
      changeHandler = new DataSegmentChangeHandler()
      {
        @Override
        public void addSegment(
            final DataSegment segment, final DataSegmentChangeCallback callback
        )
        {
          if (finalPredicate.apply(Pair.of(druidServer.getMetadata(), segment))) {
            druidServer.addDataSegment(segment.getIdentifier(), segment);
            runSegmentCallbacks(
                new Function<SegmentCallback, CallbackAction>()
                {
                  @Override
                  public CallbackAction apply(SegmentCallback input)
                  {
                    return input.segmentAdded(druidServer.getMetadata(), segment);
                  }
                }
            );
          }
        }

        @Override
        public void removeSegment(
            final DataSegment segment, final DataSegmentChangeCallback callback
        )
        {
          druidServer.removeDataSegment(segment.getIdentifier());

          runSegmentCallbacks(
              new Function<SegmentCallback, CallbackAction>()
              {
                @Override
                public CallbackAction apply(SegmentCallback input)
                {
                  return input.segmentRemoved(druidServer.getMetadata(), segment);
                }
              }
          );
        }
      };
    }

    //wait for first fetch of segment listing from server.
    void awaitInitialization()
    {
      try {
        if (!initializationLatch.await(serverHttpTimeout, TimeUnit.MILLISECONDS)) {
          log.warn("Await initialization timed out for server [%s].", druidServer.getName());
        }
      }
      catch (InterruptedException ex) {
        log.warn("Await initialization interrupted while waiting on server [%s].", druidServer.getName());
        Thread.currentThread().interrupt();
      }
    }

    DruidServerHolder updatedHolder(DruidServer server)
    {
      synchronized (lock) {
        return new DruidServerHolder(server.addDataSegments(druidServer), counter);
      }
    }

    Future<?> updateSegmentsListAsync()
    {
      try {
        final String req;
        if (counter != null) {
          req = StringUtils.format(
              "/druid-internal/v1/segments?counter=%s&hash=%s&timeout=%s",
              counter.getCounter(),
              counter.getHash(),
              config.getServerTimeout()
          );
        } else {
          req = StringUtils.format(
              "/druid-internal/v1/segments?counter=-1&timeout=%s",
              config.getServerTimeout()
          );
        }
        URL url = new URL(druidServer.getScheme(), serverHostAndPort.getHostText(), serverHostAndPort.getPort(), req);

        BytesAccumulatingResponseHandler responseHandler = new BytesAccumulatingResponseHandler();

        log.debug("Sending segment list fetch request to [%s] on URL [%s]", druidServer.getName(), url);

        ListenableFuture<InputStream> future = httpClient.go(
            new Request(HttpMethod.GET, url)
                .addHeader(
                    HttpHeaders.Names.ACCEPT,
                    SmileMediaTypes.APPLICATION_JACKSON_SMILE
                )
                .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
            responseHandler,
            new Duration(serverHttpTimeout)
        );

        log.debug("Sent segment list fetch request to [%s]", druidServer.getName());

        Futures.addCallback(
            future,
            new FutureCallback<InputStream>()
            {
              @Override
              public void onSuccess(InputStream stream)
              {
                try {
                  if (responseHandler.status == HttpServletResponse.SC_NO_CONTENT) {
                    log.debug("Received NO CONTENT from [%s]", druidServer.getName());
                    return;
                  } else if (responseHandler.status != HttpServletResponse.SC_OK) {
                    onFailure(null);
                    return;
                  }

                  log.debug("Received segment list response from [%s]", druidServer.getName());

                  SegmentChangeRequestsSnapshot delta = smileMapper.readValue(
                      stream,
                      SegmentChangeRequestsSnapshot.class
                  );

                  log.debug("Finished reading segment list response from [%s]", druidServer.getName());

                  synchronized (lock) {
                    if (delta.isResetCounter()) {
                      log.debug(
                          "Server [%s] requested resetCounter for reason [%s].",
                          druidServer.getName(),
                          delta.getResetCause()
                      );
                      counter = null;
                      return;
                    }

                    if (counter == null) {
                      druidServer.removeAllSegments();
                    }

                    for (DataSegmentChangeRequest request : delta.getRequests()) {
                      request.go(changeHandler, null);
                    }
                    counter = delta.getCounter();
                  }

                  initializationLatch.countDown();
                  isUnstable = false;
                }
                catch (Exception ex) {
                  log.error(ex, "error processing segment list response from server [%s]", druidServer.getName());
                }
                finally {
                  addNextSyncToWorkQueue(druidServer.getName());
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                try {
                  String logMsg = StringUtils.nonStrictFormat(
                      "failed to fetch segment list from server [%s]. Return code [%s], Reason: [%s]",
                      druidServer.getName(),
                      responseHandler.status,
                      responseHandler.description
                  );

                  if (hasUnstabilityTimeoutPassed()) {
                    if (t != null) {
                      log.error(t, logMsg);
                    } else {
                      log.error(logMsg);
                    }
                  } else {
                    log.info("Temporary Failure. %s", logMsg);
                    if (t != null) {
                      log.debug(t, logMsg);
                    } else {
                      log.debug(logMsg);
                    }
                  }

                  // sleep for a bit so that retry does not happen immediately.
                  try {
                    Thread.sleep(5000);
                  }
                  catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                  }
                }
                finally {
                  addNextSyncToWorkQueue(druidServer.getName());
                }
              }
            },
            executor
        );

        return future;
      }
      catch (Throwable th) {
        addNextSyncToWorkQueue(druidServer.getName());

        String logMsg = StringUtils.nonStrictFormat(
            "Fatal error while fetching segment list from server [%s].", druidServer.getName()
        );

        if (hasUnstabilityTimeoutPassed()) {
          log.makeAlert(th, logMsg).emit();
        } else {
          log.info("Temporary Failure. %s", logMsg);
          log.debug(th, logMsg);
        }

        // sleep for a bit so that retry does not happen immediately.
        try {
          Thread.sleep(5000);
        }
        catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }

        throw Throwables.propagate(th);
      }
    }

    private void addNextSyncToWorkQueue(final String serverId)
    {
      queue.add(
          () -> {
            DruidServerHolder holder = servers.get(serverId);
            if (holder != null) {
              holder.updateSegmentsListAsync();
            }
          }
      );
    }

    private boolean hasUnstabilityTimeoutPassed()
    {
      if (isUnstable && (System.currentTimeMillis() - unstableStartTime) > config.getServerUnstabilityTimeout()) {
        return true;
      }

      if (!isUnstable) {
        isUnstable = true;
        unstableStartTime = System.currentTimeMillis();
      }

      return false;
    }
  }

  private static class BytesAccumulatingResponseHandler extends InputStreamResponseHandler
  {
    private int status;
    private String description;

    @Override
    public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
    {
      status = response.getStatus().getCode();
      description = response.getStatus().getReasonPhrase();
      return ClientResponse.unfinished(super.handleResponse(response).getObj());
    }
  }
}
