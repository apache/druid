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

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.druid.audit.AuditInfo;
import io.druid.common.config.JacksonConfigManager;
import io.druid.concurrent.Execs;
import io.druid.concurrent.LifecycleLock;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StreamUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.query.lookup.LookupModule;
import io.druid.server.listener.announcer.ListenerDiscoverer;
import io.druid.server.listener.resource.ListenerResource;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class LookupCoordinatorManager
{
  //key used in druid-0.9.x with config manager
  public static final String OLD_LOOKUP_CONFIG_KEY = "lookups";

  public static final String LOOKUP_CONFIG_KEY = "lookupsConfig";
  public static final String LOOKUP_LISTEN_ANNOUNCE_KEY = "lookups";
  private static final EmittingLogger LOG = new EmittingLogger(LookupCoordinatorManager.class);

  private final static Function<HostAndPort, URL> HOST_TO_URL = new Function<HostAndPort, URL>()
  {
    @Nullable
    @Override
    public URL apply(HostAndPort input)
    {
      if (input == null) {
        LOG.warn("null entry in lookups");
        return null;
      }
      try {
        return getLookupsURL(input);
      }
      catch (MalformedURLException e) {
        LOG.warn(e, "Skipping node. Malformed URL from `%s`", input);
        return null;
      }
    }
  };

  private final ListenerDiscoverer listenerDiscoverer;
  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final JacksonConfigManager configManager;
  private final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig;
  private final AtomicReference<Map<HostAndPort, LookupsStateWithMap>> knownOldState = new AtomicReference<>();

  // Updated by config watching service
  private AtomicReference<Map<String, Map<String, LookupExtractorFactoryMapContainer>>> lookupMapConfigRef;

  @VisibleForTesting
  final LifecycleLock lifecycleLock = new LifecycleLock();

  private ListeningScheduledExecutorService executorService;
  private ListenableScheduledFuture<?> backgroundManagerFuture;
  private CountDownLatch backgroundManagerExitedLatch;

  @Inject
  public LookupCoordinatorManager(
      final @Global HttpClient httpClient,
      final ListenerDiscoverer listenerDiscoverer,
      final @Smile ObjectMapper smileMapper,
      final JacksonConfigManager configManager,
      final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig
  )
  {
    this.listenerDiscoverer = listenerDiscoverer;
    this.configManager = configManager;
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    this.lookupCoordinatorManagerConfig = lookupCoordinatorManagerConfig;
  }

  public boolean updateLookup(
      final String tier,
      final String lookupName,
      LookupExtractorFactoryMapContainer spec,
      final AuditInfo auditInfo
  )
  {
    return updateLookups(
        ImmutableMap.<String, Map<String, LookupExtractorFactoryMapContainer>>of(tier, ImmutableMap.of(lookupName, spec)),
        auditInfo
    );
  }

  public boolean updateLookups(final Map<String, Map<String, LookupExtractorFactoryMapContainer>> updateSpec, AuditInfo auditInfo)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");

    if (updateSpec.isEmpty() && lookupMapConfigRef.get() != null) {
      return true;
    }

    //ensure all the lookups specs have version specified. ideally this should be done in the LookupExtractorFactoryMapContainer
    //constructor but that allows null to enable backward compatibility with 0.9.x lookup specs
    for (final String tier : updateSpec.keySet()) {
      Map<String, LookupExtractorFactoryMapContainer> lookups = updateSpec.get(tier);
      for (Map.Entry<String, LookupExtractorFactoryMapContainer> e : lookups.entrySet()) {
        Preconditions.checkNotNull(
            e.getValue().getVersion(),
            String.format("lookup [%s]:[%s] does not have version.", tier, e.getKey())
        );
      }
    }

    synchronized(this) {
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> priorSpec = getKnownLookups();
      if (priorSpec == null && !updateSpec.isEmpty()) {
        // To prevent accidentally erasing configs if we haven't updated our cache of the values
        throw new ISE("Not initialized. If this is the first lookup, post an empty map to initialize");
      }
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> updatedSpec;

      // Only add or update here, don't delete.
      if (priorSpec == null) {
        // all new
        updatedSpec = updateSpec;
      } else {
        // Needs update
        updatedSpec = new HashMap<>(priorSpec);
        for (final String tier : updateSpec.keySet()) {
          final Map<String, LookupExtractorFactoryMapContainer> priorTierSpec = priorSpec.get(tier);
          final Map<String, LookupExtractorFactoryMapContainer> updateTierSpec = updateSpec.get(tier);
          if (priorTierSpec == null) {
            // New tier
            updatedSpec.put(tier, updateTierSpec);
          } else {
            // Update existing tier
            final Map<String, LookupExtractorFactoryMapContainer> updatedTierSpec = new HashMap<>(priorTierSpec);

            for (Map.Entry<String, LookupExtractorFactoryMapContainer> e : updateTierSpec.entrySet()) {
              if (updatedTierSpec.containsKey(e.getKey()) && !e.getValue().replaces(updatedTierSpec.get(e.getKey()))) {
                throw new IAE(
                    "given update for lookup [%s]:[%s] can't replace existing spec [%s].",
                    tier,
                    e.getKey(),
                    updatedTierSpec.get(e.getKey())
                );
              }
            }
            updatedTierSpec.putAll(updateTierSpec);
            updatedSpec.put(tier, updatedTierSpec);
          }
        }
      }
      return configManager.set(LOOKUP_CONFIG_KEY, updatedSpec, auditInfo);
    }
  }

  public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");
    return lookupMapConfigRef.get();
  }

  public boolean deleteLookup(final String tier, final String lookup, AuditInfo auditInfo)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");

    synchronized(this) {
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> priorSpec = getKnownLookups();
      if (priorSpec == null) {
        LOG.warn("Requested delete lookup [%s]/[%s]. But no lookups exist!", tier, lookup);
        return false;
      }
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> updateSpec = new HashMap<>(priorSpec);
      final Map<String, LookupExtractorFactoryMapContainer> priorTierSpec = updateSpec.get(tier);
      if (priorTierSpec == null) {
        LOG.warn("Requested delete of lookup [%s]/[%s] but tier does not exist!", tier, lookup);
        return false;
      }

      if (!priorTierSpec.containsKey(lookup)) {
        LOG.warn("Requested delete of lookup [%s]/[%s] but lookup does not exist!", tier, lookup);
        return false;
      }

      final Map<String, LookupExtractorFactoryMapContainer> updateTierSpec = new HashMap<>(priorTierSpec);
      updateTierSpec.remove(lookup);
      updateSpec.put(tier, updateTierSpec);
      return configManager.set(LOOKUP_CONFIG_KEY, updateSpec, auditInfo);
    }
  }

  public Collection<String> discoverTiers()
  {
    try {
      return listenerDiscoverer.discoverChildren(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Try to find a lookupName spec for the specified lookupName.
   *
   * @param lookupName The lookupName to look for
   *
   * @return The lookupName spec if found or null if not found or if no lookups at all are found
   */
  @Nullable
  public LookupExtractorFactoryMapContainer getLookup(final String tier, final String lookupName)
  {
    final Map<String, Map<String, LookupExtractorFactoryMapContainer>> prior = getKnownLookups();
    if (prior == null) {
      LOG.warn("Requested tier [%s] lookupName [%s]. But no lookups exist!", tier, lookupName);
      return null;
    }
    final Map<String, LookupExtractorFactoryMapContainer> tierLookups = prior.get(tier);
    if (tierLookups == null) {
      LOG.warn("Tier [%s] does not exist", tier);
      return null;
    }
    return tierLookups.get(lookupName);
  }

  // start() and stop() are synchronized so that they never run in parallel in case of ZK acting funny and
  // coordinator becomes leader and drops leadership in quick succession.
  public void start()
  {
    synchronized(lifecycleLock) {
      if (!lifecycleLock.canStart()) {
        throw new ISE("LookupCoordinatorManager can't start.");
      }

      try {
        LOG.debug("Starting.");

        if (executorService != null &&
            !executorService.awaitTermination(
                lookupCoordinatorManagerConfig.getHostTimeout().getMillis() * 10,
                TimeUnit.MILLISECONDS
            )) {
          throw new ISE("WTF! LookupCoordinatorManager executor from last start() hasn't finished. Failed to Start.");
        }

        executorService = MoreExecutors.listeningDecorator(
            Executors.newScheduledThreadPool(
                lookupCoordinatorManagerConfig.getThreadPoolSize(),
                Execs.makeThreadFactory("LookupCoordinatorManager--%s")
            )
        );

        //Note: this call is idempotent, so multiple start() would not cause any problems.
        lookupMapConfigRef = configManager.watch(
            LOOKUP_CONFIG_KEY,
            new TypeReference<Map<String, Map<String, LookupExtractorFactoryMapContainer>>>()
            {
            },
            null
        );

        // backward compatibility with 0.9.x
        if (lookupMapConfigRef.get() == null) {
          Map<String, Map<String, Map<String, Object>>> oldLookups = configManager.watch(
              OLD_LOOKUP_CONFIG_KEY,
              new TypeReference<Map<String, Map<String, Map<String, Object>>>>()
              {
              },
              null
          ).get();

          if (oldLookups != null) {
            Map<String, Map<String, LookupExtractorFactoryMapContainer>> converted = new HashMap<>();
            for (String tier : oldLookups.keySet()) {
              Map<String, Map<String, Object>> oldTierLookups = oldLookups.get(tier);
              if (oldLookups != null && !oldLookups.isEmpty()) {
                Map<String, LookupExtractorFactoryMapContainer> convertedTierLookups = new HashMap<>();
                for (Map.Entry<String, Map<String, Object>> e : oldTierLookups.entrySet()) {
                  convertedTierLookups.put(e.getKey(), new LookupExtractorFactoryMapContainer(null, e.getValue()));
                }
                converted.put(tier, convertedTierLookups);
              }
            }
            configManager.set(
                LOOKUP_CONFIG_KEY,
                converted,
                new AuditInfo("autoConversion", "autoConversion", "127.0.0.1")
            );
          }
        }

        this.backgroundManagerExitedLatch = new CountDownLatch(1);
        this.backgroundManagerFuture = executorService.scheduleWithFixedDelay(
            new Runnable()
            {
              @Override
              public void run()
              {
                lookupMgmtLoop();
              }
            },
            2000,
            lookupCoordinatorManagerConfig.getPeriod(),
            TimeUnit.MILLISECONDS
        );
        Futures.addCallback(
            backgroundManagerFuture, new FutureCallback()
            {
              @Override
              public void onSuccess(@Nullable Object result)
              {
                backgroundManagerExitedLatch.countDown();
                LOG.debug("Exited background lookup manager");
              }

              @Override
              public void onFailure(Throwable t)
              {
                backgroundManagerExitedLatch.countDown();
                if (backgroundManagerFuture.isCancelled()) {
                  LOG.debug("Exited background lookup manager due to cancellation.");
                } else {
                  LOG.makeAlert(t, "Background lookup manager exited with error!").emit();
                }
              }
            }
        );

        LOG.debug("Started");
      }
      catch (Exception ex) {
        LOG.makeAlert(ex, "Got Exception while start()").emit();
      }
      finally {
        //so that subsequent stop() would happen.
        lifecycleLock.started();
        lifecycleLock.exitStart();
      }
    }
  }

  public synchronized void stop()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStop()) {
        throw new ISE("LookupCoordinatorManager can't stop.");
      }

      try {
        LOG.debug("Stopping");

        if (backgroundManagerFuture != null && !backgroundManagerFuture.cancel(true)) {
          LOG.warn("Background lookup manager thread could not be cancelled");
        }

        if (executorService != null) {
          executorService.shutdownNow();
        }

        LOG.debug("Stopped");
      }
      catch (Exception ex) {
        LOG.makeAlert(ex, "Got Exception while stop()").emit();
      }
      finally {
        //so that next start() would happen.
        lifecycleLock.exitStop();
        lifecycleLock.reset();
      }
    }
  }

  private void lookupMgmtLoop()
  {
    // Sanity check for if we are shutting down
    if (Thread.currentThread().isInterrupted() || !lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      LOG.info("Not updating lookups because process was interrupted or not finished starting yet.");
      return;
    }

    final Map<String, Map<String, LookupExtractorFactoryMapContainer>> allLookupTiers = lookupMapConfigRef.get();

    if (allLookupTiers == null) {

      if (LOG.isDebugEnabled()) {
        LOG.debug("Not updating lookups because no data exists");
      }
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting lookup sync for on all nodes.");
    }

    final Map<HostAndPort, LookupsStateWithMap> currState = new HashMap<>();
    final Random rand = new Random();

    try {
      List<ListenableFuture<?>> futures = new ArrayList<>();
      for (String tier : allLookupTiers.keySet()) {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Starting lookup mgmt for tier [%s].", tier);
        }

        final Map<String, LookupExtractorFactoryMapContainer> tierLookups = allLookupTiers.get(tier);
        for (final HostAndPort node : listenerDiscoverer.getNodes(LookupModule.getTierListenerPath(tier))) {

          if (LOG.isDebugEnabled()) {
            LOG.debug("Starting lookup mgmt for tier [%s] and host [%s:%s].", tier, node.getHostText(), node.getPort());
          }

          futures.add(
              executorService.submit(
                  new Runnable()
                  {
                    @Override
                    public void run()
                    {
                      try {

                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Starting lookup sync for node [%s].", node);
                        }

                        LookupsStateWithMap lookupsState = knownOldState.get() != null ? knownOldState.get().get(node) : null;
                        if (lookupsState == null
                            || !lookupsState.getToLoad().isEmpty()
                            || !lookupsState.getToDrop().isEmpty()
                            || rand.nextBoolean()) {
                          lookupsState = getLookupStateForNode(node);

                          if (LOG.isDebugEnabled()) {
                            LOG.debug("Received lookups state from node [%s].", node);
                          }
                        }
                        currState.put(node, lookupsState);

                        Map<String, LookupExtractorFactoryMapContainer> toLoad = new HashMap<>();
                        for (Map.Entry<String, LookupExtractorFactoryMapContainer> e : tierLookups.entrySet()) {
                          String name = e.getKey();
                          LookupExtractorFactoryMapContainer lookupToBe = e.getValue();

                          LookupExtractorFactoryMapContainer current = lookupsState.getToLoad().get(name);
                          if (current == null) {
                            current = lookupsState.getCurrent().get(name);
                          }

                          if (current == null || lookupToBe.replaces(current)) {
                            toLoad.put(name, lookupToBe);
                          }
                        }

                        Set<String> toDrop = new HashSet<>();
                        toDrop.addAll(lookupsState.getCurrent().keySet());
                        toDrop.addAll(lookupsState.getToLoad().keySet());
                        toDrop = Sets.difference(toDrop, lookupsState.getToDrop());
                        toDrop = Sets.difference(toDrop, tierLookups.keySet());

                        if (!toLoad.isEmpty() || !toDrop.isEmpty()) {
                          currState.put(node, updateNode(node, new LookupsStateWithMap(null, toLoad, toDrop)));

                          if (LOG.isDebugEnabled()) {
                            LOG.debug(
                                "Sent lookup toAdd[%d] and toDrop[%d] updates to node [%s].",
                                toLoad.size(),
                                toDrop.size(),
                                node
                            );
                          }
                        }

                        if (LOG.isDebugEnabled()) {
                          LOG.debug("Finished lookup sync for node [%s].", node);
                        }
                      }
                      catch (Exception ex) {
                        LOG.makeAlert(ex, "Failed to manage lookups on node [%s].", node).emit();
                      }
                    }
                  }
              )
          );
        }
      }

      final ListenableFuture allFuture = Futures.allAsList(futures);
      try {
        allFuture.get(lookupCoordinatorManagerConfig.getAllHostTimeout().getMillis(), TimeUnit.MILLISECONDS);
        knownOldState.set(currState);
      }
      catch (InterruptedException ex) {
        allFuture.cancel(true);
        Thread.currentThread().interrupt();
        throw ex;
      } catch (Exception ex) {
        allFuture.cancel(true);
        throw ex;
      }

    } catch (Exception ex) {
      LOG.makeAlert(ex, "Failed to finish lookup management loop.").emit();
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Finished lookup sync for on all nodes.");
    }
  }

  @VisibleForTesting
  LookupsStateWithMap updateNode(
      HostAndPort node,
      LookupsStateWithMap lookupsUpdate
  )
      throws IOException, InterruptedException, ExecutionException
  {
    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);

    final URL url = getLookupsUpdateURL(node);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Sending lookups load/drop request to [%s]. Request [%s]", url, lookupsUpdate);
    }

    try (final InputStream result = httpClient.go(
        new Request(HttpMethod.POST, url)
            .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
            .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
            .setContent(smileMapper.writeValueAsBytes(lookupsUpdate)),
        makeResponseHandler(returnCode, reasonString),
        lookupCoordinatorManagerConfig.getHostTimeout()
    ).get()) {
      if (httpStatusIsSuccess(returnCode.get())) {
        try {
          final LookupsStateWithMap response = smileMapper.readValue(result, LookupsStateWithMap.class);
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Update on [%s], Status: %s reason: [%s], Response [%s].", url, returnCode.get(), reasonString.get(),
                response
            );
          }
          return response;
        } catch (IOException ex) {
          throw new IOException(
              String.format("Failed to parse update response from [%s]. response [%s]", url, result),
              ex
          );
        }
      } else {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          StreamUtils.copyAndClose(result, baos);
        }
        catch (IOException e2) {
          LOG.warn(e2, "Error reading response");
        }

        throw new IOException(
            String.format(
                "Bad update request to [%s] : [%d] : [%s]  Response: [%s]",
                url,
                returnCode.get(),
                reasonString.get(),
                StringUtils.fromUtf8(baos.toByteArray())
            )
        );
      }
    }
  }

  @VisibleForTesting
  LookupsStateWithMap getLookupStateForNode(
      HostAndPort node
  ) throws IOException, InterruptedException, ExecutionException
  {
    final URL url = getLookupsURL(node);
    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting lookups from [%s]", url);
    }

    try (final InputStream result = httpClient.go(
        new Request(HttpMethod.GET, url)
            .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
        makeResponseHandler(returnCode, reasonString),
        lookupCoordinatorManagerConfig.getHostTimeout()
    ).get()) {
      if (returnCode.get() == 200) {
        try {
          final LookupsStateWithMap response = smileMapper.readValue(result, LookupsStateWithMap.class);
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Get on [%s], Status: %s reason: [%s], Response [%s].", url, returnCode.get(), reasonString.get(),
                response
            );
          }
          return response;
        } catch(IOException ex) {
          throw new IOException(
              String.format(
                  "Failed to parser GET lookups response from [%s]. response [%s].",
                  url,
                  result
              ),
              ex
          );
        }
      } else {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          StreamUtils.copyAndClose(result, baos);
        }
        catch (IOException ex) {
          LOG.warn(ex, "Error reading response from GET on url [%s]", url);
        }

        throw new IOException(
            String.format(
                "GET request failed to [%s] : [%d] : [%s]  Response: [%s]",
                url,
                returnCode.get(),
                reasonString.get(),
                StringUtils.fromUtf8(baos.toByteArray())
            )
        );
      }
    }
  }

  static URL getLookupsURL(HostAndPort druidNode) throws MalformedURLException
  {
    return new URL(
        "http",
        druidNode.getHostText(),
        druidNode.getPortOrDefault(-1),
        ListenerResource.BASE_PATH + "/" + LOOKUP_LISTEN_ANNOUNCE_KEY
    );
  }

  static URL getLookupsUpdateURL(HostAndPort druidNode) throws MalformedURLException
  {
    return new URL(
        "http",
        druidNode.getHostText(),
        druidNode.getPortOrDefault(-1),
        ListenerResource.BASE_PATH + "/" + LOOKUP_LISTEN_ANNOUNCE_KEY + "/" + "updates"
    );
  }

  private static boolean httpStatusIsSuccess(int statusCode)
  {
    return statusCode >= 200 && statusCode < 300;
  }

  @VisibleForTesting
  boolean backgroundManagerIsRunning()
  {
    ListenableScheduledFuture backgroundManagerFuture = this.backgroundManagerFuture;
    return backgroundManagerFuture != null && !backgroundManagerFuture.isDone();
  }

  @VisibleForTesting
  boolean waitForBackgroundTermination(long timeout) throws InterruptedException
  {
    return backgroundManagerExitedLatch.await(timeout, TimeUnit.MILLISECONDS);
  }

  @VisibleForTesting
  HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
      final AtomicInteger returnCode,
      final AtomicReference<String> reasonString
  )
  {
    return new SequenceInputStreamResponseHandler()
    {
      @Override
      public ClientResponse<InputStream> handleResponse(HttpResponse response)
      {
        returnCode.set(response.getStatus().getCode());
        reasonString.set(response.getStatus().getReasonPhrase());
        return super.handleResponse(response);
      }
    };
  }
}
