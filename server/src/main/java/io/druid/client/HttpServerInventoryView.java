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
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DataNodeService;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.io.AppendableByteArrayInputStream;
import io.druid.java.util.http.client.response.ClientResponse;
import io.druid.java.util.http.client.response.InputStreamResponseHandler;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestHistory;
import io.druid.server.coordination.SegmentChangeRequestLoad;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * This class uses CuratorInventoryManager to listen for queryable server membership which serve segments(e.g. Historicals).
 * For each queryable server, it uses HTTP GET /druid-internal/v1/segments (see docs in SegmentListerResource.getSegments(..).
 */
public class HttpServerInventoryView implements ServerInventoryView, FilteredServerInventoryView
{
  private final EmittingLogger log = new EmittingLogger(HttpServerInventoryView.class);
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final ConcurrentMap<ServerRemovedCallback, Executor> serverCallbacks = new ConcurrentHashMap<>();
  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks = new ConcurrentHashMap<>();

  private final ConcurrentMap<SegmentCallback, Predicate<Pair<DruidServerMetadata, DataSegment>>> segmentPredicates =
      new ConcurrentHashMap<>();

  /**
   * Users of this instance can register filters for what segments should be stored and reported to registered
   * listeners. For example, A Broker node can be configured to keep state for segments of specific DataSource
   * by using this feature. In that way, Different Broker nodes can be used for dealing with Queries of Different
   * DataSources and not maintaining any segment information of other DataSources in memory.
   */
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter;
  private volatile Predicate<Pair<DruidServerMetadata, DataSegment>> finalPredicate;

  // For each queryable server, a name -> DruidServerHolder entry is kept
  private final ConcurrentHashMap<String, DruidServerHolder> servers = new ConcurrentHashMap<>();

  private volatile ScheduledExecutorService executor;

  // the work queue, all items in this are sequentially processed by main thread setup in start()
  // used to call inventoryInitialized on all SegmentCallbacks and
  // for keeping segment list for each queryable server uptodate.
  private final BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>();

  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final HttpServerInventoryViewConfig config;

  private final CountDownLatch inventoryInitializationLatch = new CountDownLatch(1);

  @Inject
  public HttpServerInventoryView(
      final @Smile ObjectMapper smileMapper,
      final @EscalatedGlobal HttpClient httpClient,
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
        executor = ScheduledExecutors.fixed(
            config.getNumThreads(),
            "HttpServerInventoryView-%s"
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
              private final AtomicBoolean initialized = new AtomicBoolean(false);

              @Override
              public void nodesAdded(List<DiscoveryDruidNode> nodes)
              {
                nodes.forEach(
                    node -> serverAdded(toDruidServer(node))
                );

                if (!initialized.getAndSet(true)) {
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

        lifecycleLock.started();
      }
      finally {
        lifecycleLock.exitStart();
      }

      log.info("Waiting for Server Inventory Initialization...");

      while (!inventoryInitializationLatch.await(1, TimeUnit.MINUTES)) {
        log.info("Still waiting for Server Inventory Initialization...");
      }

      log.info("Started HttpServerInventoryView.");
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
    if (lifecycleLock.isStarted()) {
      throw new ISE("Lifecycle has already started.");
    }

    SegmentCallback filteringSegmentCallback = new SingleServerInventoryView.FilteringSegmentCallback(callback, filter);
    segmentCallbacks.put(filteringSegmentCallback, exec);
    segmentPredicates.put(filteringSegmentCallback, filter);

    finalPredicate = Predicates.or(
        defaultFilter,
        Predicates.or(segmentPredicates.values())
    );
  }

  @Override
  public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
  {
    if (lifecycleLock.isStarted()) {
      throw new ISE("Lifecycle has already started.");
    }

    serverCallbacks.put(callback, exec);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    if (lifecycleLock.isStarted()) {
      throw new ISE("Lifecycle has already started.");
    }

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
  public Collection<DruidServer> getInventory()
  {
    return servers.values()
                  .stream()
                  .map(serverHolder -> serverHolder.druidServer)
                  .collect(Collectors.toList());
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

    inventoryInitializationLatch.countDown();

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

  private void serverAdded(DruidServer server)
  {
    DruidServerHolder holder = servers.computeIfAbsent(
        server.getName(),
        k -> new DruidServerHolder(server)
    );

    if (holder.druidServer == server) {
      holder.updateSegmentsListAsync();
    } else {
      log.info("Server[%s] already exists.", server.getName());
    }
  }

  private void serverRemoved(DruidServer server)
  {
    DruidServerHolder holder = servers.remove(server.getName());
    if (holder != null) {
      runServerCallbacks(holder.druidServer);
    } else {
      log.info("Server[%s] did not exist. Removal notification ignored.", server.getName());
    }
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
    private final DruidServer druidServer;

    private volatile SegmentChangeRequestHistory.Counter counter = null;

    private final HostAndPort serverHostAndPort;

    private final long serverHttpTimeout = config.getServerTimeout() + 1000;

    private final CountDownLatch initializationLatch = new CountDownLatch(1);

    private volatile long unstableStartTime = -1;
    private volatile int consecutiveFailedAttemptCount = 0;

    private final Runnable addToQueueRunnable;

    DruidServerHolder(DruidServer druidServer)
    {
      this.druidServer = druidServer;
      this.serverHostAndPort = HostAndPort.fromString(druidServer.getHost());

      this.addToQueueRunnable = () -> {
        queue.add(
            () -> {
              DruidServerHolder holder = servers.get(druidServer.getName());
              if (holder != null) {
                holder.updateSegmentsListAsync();
              }
            }
        );
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

    void updateSegmentsListAsync()
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
                      log.info(
                          "Server [%s] requested resetCounter for reason [%s].",
                          druidServer.getName(),
                          delta.getResetCause()
                      );
                      counter = null;
                      return;
                    }

                    if (counter == null) {
                      // means, on last request either server had asked us to reset the counter or it was very first
                      // request to the server.
                      Map<String, DataSegment> toRemove = Maps.newHashMap(druidServer.getSegments());

                      for (DataSegmentChangeRequest request : delta.getRequests()) {
                        if (request instanceof SegmentChangeRequestLoad) {
                          DataSegment segment = ((SegmentChangeRequestLoad) request).getSegment();
                          toRemove.remove(segment.getIdentifier());
                          addSegment(segment);
                        } else {
                          log.error(
                              "Server[%s] gave a non-load dataSegmentChangeRequest[%s]., Ignored.",
                              druidServer.getName(),
                              request
                          );
                        }
                      }

                      for (DataSegment segmentToRemove : toRemove.values()) {
                        removeSegment(segmentToRemove);
                      }

                    } else {
                      for (DataSegmentChangeRequest request : delta.getRequests()) {
                        if (request instanceof SegmentChangeRequestLoad) {
                          addSegment(((SegmentChangeRequestLoad) request).getSegment());
                        } else if (request instanceof SegmentChangeRequestDrop) {
                          removeSegment(((SegmentChangeRequestDrop) request).getSegment());
                        } else {
                          log.error(
                              "Server[%s] gave a non load/drop dataSegmentChangeRequest[%s], Ignored.",
                              druidServer.getName(),
                              request
                          );
                        }
                      }
                    }

                    counter = delta.getCounter();
                  }

                  initializationLatch.countDown();
                  consecutiveFailedAttemptCount = 0;
                }
                catch (Exception ex) {
                  String logMsg = StringUtils.nonStrictFormat(
                      "Error processing segment list response from server [%s]. Reason [%s]",
                      druidServer.getName(),
                      ex.getMessage()
                  );

                  if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
                    log.error(ex, logMsg);
                  } else {
                    log.info("Temporary Failure. %s", logMsg);
                    log.debug(ex, logMsg);
                  }
                }
                finally {
                  addNextSyncToWorkQueue();
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

                  if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
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
                }
                finally {
                  addNextSyncToWorkQueue();
                }
              }
            },
            executor
        );
      }
      catch (Throwable th) {
        try {
          String logMsg = StringUtils.nonStrictFormat(
              "Fatal error while fetching segment list from server [%s].", druidServer.getName()
          );

          if (incrementFailedAttemptAndCheckUnstabilityTimeout()) {
            log.makeAlert(th, logMsg).emit();
          } else {
            log.info("Temporary Failure. %s", logMsg);
            log.debug(th, logMsg);
          }
        }
        finally {
          addNextSyncToWorkQueue();
        }
      }
    }

    private void addSegment(final DataSegment segment)
    {
      if (finalPredicate.apply(Pair.of(druidServer.getMetadata(), segment))) {
        if (druidServer.getSegment(segment.getIdentifier()) == null) {
          druidServer.addDataSegment(segment);
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
        } else {
          log.warn(
              "Not adding or running callbacks for existing segment[%s] on server[%s]",
              segment.getIdentifier(),
              druidServer.getName()
          );
        }
      }
    }

    private void removeSegment(final DataSegment segment)
    {
      if (druidServer.getSegment(segment.getIdentifier()) != null) {
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
      } else {
        log.warn(
            "Not running cleanup or callbacks for non-existing segment[%s] on server[%s]",
            segment.getIdentifier(),
            druidServer.getName()
        );
      }
    }

    private void addNextSyncToWorkQueue()
    {
      if (consecutiveFailedAttemptCount > 0) {
        try {
          long sleepMillis = RetryUtils.nextRetrySleepMillis(consecutiveFailedAttemptCount);
          log.info("Scheduling next syncup in [%d] millis from server [%s].", sleepMillis, druidServer.getName());
          executor.schedule(
              addToQueueRunnable,
              sleepMillis,
              TimeUnit.MILLISECONDS
          );
        }
        catch (Exception ex) {
          log.makeAlert(
              ex,
              "WTF! Couldn't schedule next sync. Server[%s] is not being synced any more, restarting Druid process on that server might fix the issue.",
              druidServer.getName()
          ).emit();
        }
      } else {
        addToQueueRunnable.run();
      }
    }

    private boolean incrementFailedAttemptAndCheckUnstabilityTimeout()
    {
      if (consecutiveFailedAttemptCount > 0
          && (System.currentTimeMillis() - unstableStartTime) > config.getServerUnstabilityTimeout()) {
        return true;
      }

      if (consecutiveFailedAttemptCount++ == 0) {
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
