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
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.druid.concurrent.Execs;
import io.druid.curator.inventory.CuratorInventoryManager;
import io.druid.curator.inventory.CuratorInventoryManagerStrategy;
import io.druid.curator.inventory.InventoryManagerConfig;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.coordination.DataSegmentChangeCallback;
import io.druid.server.coordination.DataSegmentChangeHandler;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.SegmentChangeRequestHistory;
import io.druid.server.coordination.SegmentChangeRequestsSnapshot;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import javax.annotation.concurrent.GuardedBy;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class uses CuratorInventoryManager to listen for queryable server membership which serve segments(e.g. Historicals).
 * For each queryable server, it uses HTTP GET /druid/internal/v1/segments (see docs in SegmentListerResource.getSegments(..).
 */
public class HttpServerInventoryView implements ServerInventoryView, FilteredServerInventoryView
{
  private final EmittingLogger log = new EmittingLogger(HttpServerInventoryView.class);
  private final CuratorInventoryManager<DruidServer, Object> inventoryManager;
  private final AtomicBoolean started = new AtomicBoolean(false);

  private final ConcurrentMap<ServerCallback, Executor> serverCallbacks = new MapMaker().makeMap();
  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks = new MapMaker().makeMap();

  private final ConcurrentMap<SegmentCallback, Predicate<Pair<DruidServerMetadata, DataSegment>>> segmentPredicates = new MapMaker()
      .makeMap();
  private final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter;
  private volatile Predicate<Pair<DruidServerMetadata, DataSegment>> finalPredicate;

  // For each queryable server, a name -> DruidServerHolder entry is kept
  @GuardedBy("inventory")
  private final Map<String, DruidServerHolder> servers = new HashMap<>();

  private volatile ExecutorService executor;

  // a queue of queryable server names for which worker threads in executor initiate the segment list call i.e.
  // DruidServerHolder.updateSegmentsListAsync(..) which updates the segment list asynchronously and adds itself
  // to this queue again for next update.
  private final BlockingQueue<String> queue = new LinkedBlockingDeque<>();



  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final HttpServerInventoryViewConfig config;

  @Inject
  public HttpServerInventoryView(
      final @Json ObjectMapper jsonMapper,
      final @Smile ObjectMapper smileMapper,
      final @Global HttpClient httpClient,
      final CuratorFramework curator,
      final String announcementsPath,
      final Predicate<Pair<DruidServerMetadata, DataSegment>> defaultFilter,
      final HttpServerInventoryViewConfig config
  )
  {
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    this.inventoryManager = initCuratorInventoryManager(curator, announcementsPath, jsonMapper);
    this.defaultFilter = defaultFilter;
    this.finalPredicate = defaultFilter;
    this.config = config;
  }


  @LifecycleStart
  public void start() throws Exception
  {
    synchronized (started) {
      if (!started.get()) {
        log.info("Starting HttpServerInventoryView.");

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
                while (!Thread.currentThread().isInterrupted()) {
                  try {
                    String name = queue.take();

                    synchronized (servers) {
                      DruidServerHolder holder = servers.get(name);
                      if (holder != null) {
                        holder.updateSegmentsListAsync();
                      }
                    }
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

        inventoryManager.start();
        started.set(true);
        log.info("Started HttpServerInventoryView.");
      }
    }
  }

  @LifecycleStop
  public void stop() throws IOException
  {
    synchronized (started) {
      if (started.getAndSet(false)) {
        log.info("Stopping HttpServerInventoryView.");

        inventoryManager.stop();

        if (executor != null) {
          executor.shutdownNow();
          executor = null;
        }

        queue.clear();

        log.info("Stopped HttpServerInventoryView.");
      }
    }
  }

  private CuratorInventoryManager initCuratorInventoryManager(
      final CuratorFramework curator,
      final String announcementsPath,
      final ObjectMapper jsonMapper
  )
  {
    return new CuratorInventoryManager<>(
        curator,
        new InventoryManagerConfig()
        {
          @Override
          public String getContainerPath()
          {
            return announcementsPath;
          }

          @Override
          public String getInventoryPath()
          {
            return "/NON_EXISTENT_DUMMY_INVENTORY_PATH";
          }
        },
        Execs.singleThreaded("HttpServerInventoryView-Curator-%s"),
        new CuratorInventoryManagerStrategy<DruidServer, Object>()
        {
          @Override
          public DruidServer deserializeContainer(byte[] bytes)
          {
            try {
              return jsonMapper.readValue(bytes, DruidServer.class);
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }

          @Override
          public void newContainer(DruidServer container)
          {
            log.info("New Server[%s]", container);
            serverAddedOrUpdated(container);
          }

          @Override
          public void deadContainer(DruidServer deadContainer)
          {
            log.info("Server Disappeared[%s]", deadContainer);
            serverRemoved(deadContainer);
            runServerCallbacks(deadContainer);
          }

          @Override
          public DruidServer updateContainer(DruidServer oldContainer, DruidServer newContainer)
          {
            return serverUpdated(oldContainer, newContainer);
          }

          @Override
          public Object deserializeInventory(byte[] bytes)
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public DruidServer addInventory(
              final DruidServer container,
              String inventoryKey,
              final Object inventory
          )
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public DruidServer updateInventory(
              DruidServer container, String inventoryKey, Object inventory
          )
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public DruidServer removeInventory(final DruidServer container, String inventoryKey)
          {
            throw new ISE("no inventory should exist.");
          }

          @Override
          public void inventoryInitialized()
          {
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
        }
    );
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
  public void registerServerCallback(Executor exec, ServerCallback callback)
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
    synchronized (servers) {
      return servers.get(containerKey).druidServer;
    }
  }

  @Override
  public Iterable<DruidServer> getInventory()
  {
    synchronized (servers) {
      return Iterables.transform(
          servers.values(), new com.google.common.base.Function<DruidServerHolder, DruidServer>()
          {
            @Override
            public DruidServer apply(DruidServerHolder input)
            {
              return input.druidServer;
            }
          }
      );
    }
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
    for (final Map.Entry<ServerCallback, Executor> entry : serverCallbacks.entrySet()) {
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

  private DruidServer serverAddedOrUpdated(DruidServer server)
  {
    DruidServerHolder curr;
    DruidServerHolder newHolder;
    synchronized (servers) {
      curr = servers.get(server.getName());
      newHolder = curr == null ? new DruidServerHolder(server) : curr.updatedHolder(server);
      servers.put(server.getName(), newHolder);
    }

    //fetch list of segments
    if (curr == null) {
      try {
        Future<?> future = newHolder.updateSegmentsListAsync();
        future.get(config.getServerTimeout(), TimeUnit.MILLISECONDS);
      } catch (Throwable th) {
        log.error(th, "Failed to fetch segment list from %s , will be retried", server.getName());
      }
    }

    return newHolder.druidServer;
  }

  private void serverRemoved(DruidServer server)
  {
    synchronized (servers) {
      servers.remove(server.getName());
    }
  }

  public DruidServer serverUpdated(DruidServer oldServer, DruidServer newServer)
  {
    return serverAddedOrUpdated(newServer);
  }

  @Override
  public boolean isStarted()
  {
    return started.get();
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    synchronized (servers) {
      DruidServerHolder holder = servers.get(serverKey);
      return holder.druidServer.getSegment(segment.getIdentifier()) != null;
    }
  }

  private class DruidServerHolder
  {
    private final Object lock = new Object();

    @GuardedBy("lock")
    private final DruidServer druidServer;
    private final HostAndPort serverHostAndPort;

    @GuardedBy("lock")
    private volatile SegmentChangeRequestHistory.Counter counter = null;

    private final DataSegmentChangeHandler changeHandler;

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

    DruidServerHolder updatedHolder(DruidServer server)
    {
      synchronized (lock) {
        return new DruidServerHolder(server.addDataSegments(druidServer), counter) ;
      }
    }

    Future<?> updateSegmentsListAsync()
    {
      try {
        final String req;
        if (counter != null) {
          req = String.format(
              "/druid/internal/v1/segments?counter=%s&hash=%s&timeout=%s",
              counter.getCounter(),
              counter.getHash(),
              config.getServerTimeout()
          );
        } else {
          req = String.format(
              "/druid/internal/v1/segments?counter=-1&timeout=%s",
              config.getServerTimeout()
          );
        }
        URL url = new URL("http", serverHostAndPort.getHostText(), serverHostAndPort.getPort(), req);


        final AtomicInteger returnCode = new AtomicInteger();
        final AtomicReference<String> reasonString = new AtomicReference<>();

        SequenceInputStreamResponseHandler responseHandler = new SequenceInputStreamResponseHandler()
        {
          @Override
          public ClientResponse<InputStream> handleResponse(HttpResponse response)
          {
            returnCode.set(response.getStatus().getCode());
            reasonString.set(response.getStatus().getReasonPhrase());
            return super.handleResponse(response);
          }
        };

        if (log.isDebugEnabled()) {
          log.debug("Sending segment list fetch request to [%s] on URL [%s]", druidServer.getName(), url);
        }

        ListenableFuture<InputStream> future = httpClient.go(
            new Request(HttpMethod.GET, url)
                .addHeader(
                    HttpHeaders.Names.ACCEPT,
                    SmileMediaTypes.APPLICATION_JACKSON_SMILE
                )
                .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
            responseHandler,
            new Duration(config.getServerTimeout() + 1000)
        );

        if (log.isDebugEnabled()) {
          log.debug("Sent segment list fetch request to [%s]", druidServer.getName());
        }

        Futures.addCallback(
            future,
            new FutureCallback<InputStream>()
            {
              @Override
              public void onSuccess(InputStream stream)
              {
                try {
                  if (returnCode.get() == HttpServletResponse.SC_NO_CONTENT) {
                    if (log.isDebugEnabled()) {
                      log.debug("Received NO CONTENT from [%s]", druidServer.getName());
                    }
                    return;
                  } else if (returnCode.get() != HttpServletResponse.SC_OK) {
                    onFailure(null);
                    return;
                  }

                  if (log.isDebugEnabled()) {
                    log.debug("Received segment list response from [%s]", druidServer.getName());
                  }

                  SegmentChangeRequestsSnapshot delta = smileMapper.readValue(
                      stream,
                      SegmentChangeRequestsSnapshot.class
                  );

                  if (log.isDebugEnabled()) {
                    log.debug("Finished reading segment list response from [%s]", druidServer.getName());
                  }

                  synchronized (lock) {
                    if (counter == null) {
                      druidServer.removeAllSegments();
                    }

                    for (DataSegmentChangeRequest request : delta.getRequests()) {
                      request.go(changeHandler, null);
                    }
                    counter = delta.getCounter();
                  }
                } catch (Exception ex) {
                  log.error(ex, "error processing segment list response from server [%s]", druidServer.getName());
                } finally {
                  queue.add(druidServer.getName());
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                try {
                  if (t != null) {
                    log.error(
                        t,
                        "failed to fetch segment list from server [%s]. Return code [%s], Reason: [%s]",
                        druidServer.getName(),
                        returnCode.get(),
                        reasonString.get()
                    );
                  } else {
                    log.error(
                        "failed to fetch segment list from server [%s]. Return code [%s], Reason: [%s]",
                        druidServer.getName(),
                        returnCode.get(),
                        reasonString.get()
                    );
                  }

                  if (returnCode.get() == HttpServletResponse.SC_BAD_REQUEST) {
                    synchronized (lock) {
                      counter = null;
                    }
                  }
                } finally {
                  queue.add(druidServer.getName());
                }
              }
            },
            executor
        );

        return future;
      } catch (Throwable th) {
        queue.add(druidServer.getName());
        log.makeAlert(th, "Fatal error while fetching segment list from server [%s].", druidServer.getName()).emit();

        // sleep for a bit so that retry does not happen immediately.
        try {
          Thread.sleep(5000);
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }

        throw Throwables.propagate(th);
      }
    }
  }
}
