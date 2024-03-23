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

package org.apache.druid.server.lookup.cache;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
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
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StreamUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.SequenceInputStreamResponseHandler;
import org.apache.druid.query.lookup.LookupsState;
import org.apache.druid.server.http.HostAndPortWithScheme;
import org.apache.druid.server.listener.resource.ListenerResource;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Managers {@link org.apache.druid.query.lookup.LookupExtractorFactoryContainer} specifications, distributing them
 * to {@link org.apache.druid.query.lookup.LookupReferencesManager} around the cluster by monitoring the lookup
 * announce path for servers and utilizing their {@link org.apache.druid.query.lookup.LookupListeningResource} API
 * to load, drop, and update lookups around the cluster.
 */
public class LookupCoordinatorManager
{
  //key used in druid-0.10.0 with config manager
  public static final String OLD_LOOKUP_CONFIG_KEY = "lookups";

  public static final String LOOKUP_CONFIG_KEY = "lookupsConfig";
  public static final String LOOKUP_LISTEN_ANNOUNCE_KEY = "lookups";

  private static final String LOOKUP_BASE_REQUEST_PATH = ListenerResource.BASE_PATH
                                                         + "/"
                                                         + LOOKUP_LISTEN_ANNOUNCE_KEY;
  private static final String LOOKUP_UPDATE_REQUEST_PATH = LOOKUP_BASE_REQUEST_PATH + "/" + "updates";

  private static final TypeReference<LookupsState<LookupExtractorFactoryMapContainer>> LOOKUPS_STATE_TYPE_REFERENCE =
      new TypeReference<LookupsState<LookupExtractorFactoryMapContainer>>()
      {
      };

  private static final EmittingLogger LOG = new EmittingLogger(LookupCoordinatorManager.class);

  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private LookupNodeDiscovery lookupNodeDiscovery;

  private final JacksonConfigManager configManager;
  private final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig;
  private final LookupsCommunicator lookupsCommunicator;

  // Known lookup state across various cluster nodes is managed in the reference here. On each lookup management loop
  // state is rediscovered and updated in the ref here. If some lookup nodes have disappeared since last lookup
  // management loop, then they get discarded automatically.
  @VisibleForTesting
  final AtomicReference<Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>>> knownOldState =
      new AtomicReference<>(ImmutableMap.of());

  // Updated by config watching service
  private AtomicReference<Map<String, Map<String, LookupExtractorFactoryMapContainer>>> lookupMapConfigRef;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private ListeningScheduledExecutorService executorService;
  private ListenableScheduledFuture<?> backgroundManagerFuture;
  private CountDownLatch backgroundManagerExitedLatch;

  @Inject
  public LookupCoordinatorManager(
      final @EscalatedGlobal HttpClient httpClient,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final @Smile ObjectMapper smileMapper,
      final JacksonConfigManager configManager,
      final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig
  )
  {
    this(
        druidNodeDiscoveryProvider,
        configManager,
        lookupCoordinatorManagerConfig,
        new LookupsCommunicator(
            httpClient,
            lookupCoordinatorManagerConfig,
            smileMapper
        ),
        null
    );
  }

  @VisibleForTesting
  LookupCoordinatorManager(
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final JacksonConfigManager configManager,
      final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig,
      final LookupsCommunicator lookupsCommunicator,
      final LookupNodeDiscovery lookupNodeDiscovery
  )
  {
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.configManager = configManager;
    this.lookupCoordinatorManagerConfig = lookupCoordinatorManagerConfig;
    this.lookupsCommunicator = lookupsCommunicator;
    this.lookupNodeDiscovery = lookupNodeDiscovery;
  }

  public boolean updateLookup(
      final String tier,
      final String lookupName,
      LookupExtractorFactoryMapContainer spec,
      final AuditInfo auditInfo
  )
  {
    return updateLookups(
        ImmutableMap.of(tier, ImmutableMap.of(lookupName, spec)),
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
    //constructor but that allows null to enable backward compatibility with 0.10.0 lookup specs
    for (final Map.Entry<String, Map<String, LookupExtractorFactoryMapContainer>> tierEntry : updateSpec.entrySet()) {
      for (Map.Entry<String, LookupExtractorFactoryMapContainer> e : tierEntry.getValue().entrySet()) {
        Preconditions.checkNotNull(
            e.getValue().getVersion(),
            "lookup [%s]:[%s] does not have version.", tierEntry.getKey(), e.getKey()
        );
      }
    }

    synchronized (this) {
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
        for (final Map.Entry<String, Map<String, LookupExtractorFactoryMapContainer>> tierEntry : updateSpec.entrySet()) {
          final String tier = tierEntry.getKey();
          final Map<String, LookupExtractorFactoryMapContainer> updateTierSpec = tierEntry.getValue();
          final Map<String, LookupExtractorFactoryMapContainer> priorTierSpec = priorSpec.get(tier);

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
      return configManager.set(LOOKUP_CONFIG_KEY, updatedSpec, auditInfo).isOk();
    }
  }

  public Map<String, Map<String, LookupExtractorFactoryMapContainer>> getKnownLookups()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");
    return lookupMapConfigRef.get();
  }

  public boolean deleteTier(final String tier, AuditInfo auditInfo)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");

    synchronized (this) {
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> priorSpec = getKnownLookups();
      if (priorSpec == null) {
        LOG.warn("Requested delete tier [%s]. But no lookups exist!", tier);
        return false;
      }
      final Map<String, Map<String, LookupExtractorFactoryMapContainer>> updateSpec = new HashMap<>(priorSpec);

      if (updateSpec.remove(tier) == null) {
        LOG.warn("Requested delete of tier [%s] that does not exist!", tier);
        return false;
      }

      return configManager.set(LOOKUP_CONFIG_KEY, updateSpec, auditInfo).isOk();
    }
  }

  public boolean deleteLookup(final String tier, final String lookup, AuditInfo auditInfo)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");

    synchronized (this) {
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

      if (updateTierSpec.isEmpty()) {
        updateSpec.remove(tier);
      } else {
        updateSpec.put(tier, updateTierSpec);
      }
      return configManager.set(LOOKUP_CONFIG_KEY, updateSpec, auditInfo).isOk();
    }
  }

  public Set<String> discoverTiers()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");
    return lookupNodeDiscovery.getAllTiers();
  }

  public Collection<HostAndPort> discoverNodesInTier(String tier)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");
    return Collections2.transform(
        lookupNodeDiscovery.getNodesInTier(tier),
        new Function<HostAndPortWithScheme, HostAndPort>()
        {
          @Override
          public HostAndPort apply(HostAndPortWithScheme input)
          {
            return input.getHostAndPort();
          }
        }
    );
  }

  public Map<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> getLastKnownLookupsStateOnNodes()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(5, TimeUnit.SECONDS), "not started");
    return knownOldState.get();
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

  public boolean isStarted()
  {
    return lifecycleLock.isStarted();
  }

  @VisibleForTesting
  boolean awaitStarted(long waitTimeMs)
  {
    return lifecycleLock.awaitStarted(waitTimeMs, TimeUnit.MILLISECONDS);
  }

  // start() and stop() are synchronized so that they never run in parallel in case of ZK acting funny or druid bug and
  // coordinator becomes leader and drops leadership in quick succession.
  public void start()
  {
    synchronized (lifecycleLock) {
      if (!lifecycleLock.canStart()) {
        throw new ISE("LookupCoordinatorManager can't start.");
      }

      try {
        LOG.debug("Starting.");

        if (lookupNodeDiscovery == null) {
          lookupNodeDiscovery = new LookupNodeDiscovery(druidNodeDiscoveryProvider);
        }

        //first ensure that previous executorService from last cycle of start/stop has finished completely.
        //so that we don't have multiple live executorService instances lying around doing lookup management.
        if (executorService != null &&
            !executorService.awaitTermination(
                lookupCoordinatorManagerConfig.getHostTimeout().getMillis() * 10,
                TimeUnit.MILLISECONDS
            )) {
          throw new ISE("LookupCoordinatorManager executor from last start() hasn't finished. Failed to Start.");
        }

        executorService = MoreExecutors.listeningDecorator(
            Executors.newScheduledThreadPool(
                lookupCoordinatorManagerConfig.getThreadPoolSize(),
                Execs.makeThreadFactory("LookupCoordinatorManager--%s")
            )
        );

        initializeLookupsConfigWatcher();

        this.backgroundManagerExitedLatch = new CountDownLatch(1);
        this.backgroundManagerFuture = executorService.scheduleWithFixedDelay(
            this::lookupManagementLoop,
            lookupCoordinatorManagerConfig.getInitialDelay(),
            lookupCoordinatorManagerConfig.getPeriod(),
            TimeUnit.MILLISECONDS
        );
        Futures.addCallback(
            backgroundManagerFuture,
            new FutureCallback<Object>()
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
            },
            MoreExecutors.directExecutor()
        );

        LOG.debug("Started");
      }
      catch (Exception ex) {
        LOG.makeAlert(ex, "Got Exception while start()").emit();
      }
      finally {
        //so that subsequent stop() would happen, even if start() failed with exception
        lifecycleLock.started();
        lifecycleLock.exitStart();
      }
    }
  }

  public void stop()
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

        // signal the executorService to shut down ASAP, if this coordinator becomes leader again
        // then start() would ensure that this executorService is finished before starting a
        // new one.
        if (executorService != null) {
          executorService.shutdownNow();
        }

        LOG.debug("Stopped");
      }
      catch (Exception ex) {
        LOG.makeAlert(ex, "Got Exception while stop()").emit();
      }
      finally {
        //so that subsequent start() would happen, even if stop() failed with exception
        lifecycleLock.exitStopAndReset();
      }
    }
  }

  private void initializeLookupsConfigWatcher()
  {
    //Note: this call is idempotent, so multiple start() would not cause any problems.
    lookupMapConfigRef = configManager.watch(
        LOOKUP_CONFIG_KEY,
        new TypeReference<Map<String, Map<String, LookupExtractorFactoryMapContainer>>>()
        {
        },
        null
    );

    // backward compatibility with 0.10.0
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
        oldLookups.forEach(
            (tier, oldTierLookups) -> {
              if (oldTierLookups != null && !oldTierLookups.isEmpty()) {
                converted.put(tier, convertTierLookups(oldTierLookups));
              }
            }
        );

        configManager.set(
            LOOKUP_CONFIG_KEY,
            converted,
            new AuditInfo("autoConversion", "autoConversion", "autoConversion", "127.0.0.1")
        );
      }
    }
  }

  private Map<String, LookupExtractorFactoryMapContainer> convertTierLookups(
      Map<String, Map<String, Object>> oldTierLookups
  )
  {
    Map<String, LookupExtractorFactoryMapContainer> convertedTierLookups = new HashMap<>();
    oldTierLookups.forEach(
        (lookup, lookupExtractorFactory) -> {
          convertedTierLookups.put(lookup, new LookupExtractorFactoryMapContainer(null, lookupExtractorFactory));
        }
    );
    return convertedTierLookups;
  }

  @VisibleForTesting
  void lookupManagementLoop()
  {
    // Sanity check for if we are shutting down
    if (Thread.currentThread().isInterrupted() || !lifecycleLock.awaitStarted(15, TimeUnit.SECONDS)) {
      LOG.info("Not updating lookups because process was interrupted or not finished starting yet.");
      return;
    }

    final Map<String, Map<String, LookupExtractorFactoryMapContainer>> allLookupTiers = lookupMapConfigRef.get();

    if (allLookupTiers == null) {
      LOG.info("Not updating lookups because no data exists");
      return;
    }

    LOG.debug("Starting lookup sync for on all nodes.");

    try {
      List<ListenableFuture<Map.Entry>> futures = new ArrayList<>();

      Set<String> discoveredLookupTiers = lookupNodeDiscovery.getAllTiers();

      // Check and Log warnings about lookups configured by user in DB but no nodes discovered to load those.
      for (String tierInDB : allLookupTiers.keySet()) {
        if (!discoveredLookupTiers.contains(tierInDB) &&
            !allLookupTiers.getOrDefault(tierInDB, ImmutableMap.of()).isEmpty()) {
          LOG.warn("Found lookups for tier [%s] in DB, but no nodes discovered for it", tierInDB);
        }
      }

      for (String tier : discoveredLookupTiers) {

        LOG.debug("Starting lookup mgmt for tier [%s].", tier);

        final Map<String, LookupExtractorFactoryMapContainer> tierLookups = allLookupTiers.getOrDefault(tier, ImmutableMap.of());
        for (final HostAndPortWithScheme node : lookupNodeDiscovery.getNodesInTier(tier)) {

          LOG.debug(
              "Starting lookup mgmt for tier [%s] and host [%s:%s:%s].",
              tier,
              node.getScheme(),
              node.getHostText(),
              node.getPort()
          );

          futures.add(
              executorService.submit(
                  () -> {
                    try {
                      return new AbstractMap.SimpleImmutableEntry<>(node.getHostAndPort(), doLookupManagementOnNode(node, tierLookups));
                    }
                    catch (InterruptedException ex) {
                      LOG.warn(ex, "lookup management on node [%s:%s:%s] interrupted.", node.getScheme(), node.getHostText(), node.getPort());
                      return null;
                    }
                    catch (Exception ex) {
                      LOG.makeAlert(
                          ex,
                          "Failed to finish lookup management on node [%s:%s:%s]",
                          node.getScheme(),
                          node.getHostText(),
                          node.getPort()
                      ).emit();
                      return null;
                    }
                  }
              )
          );
        }
      }

      final ListenableFuture<List<Map.Entry>> allFuture = Futures.allAsList(futures);
      try {
        ImmutableMap.Builder<HostAndPort, LookupsState<LookupExtractorFactoryMapContainer>> stateBuilder = ImmutableMap.builder();
        allFuture.get(lookupCoordinatorManagerConfig.getAllHostTimeout().getMillis(), TimeUnit.MILLISECONDS)
                 .stream()
                 .filter(Objects::nonNull)
                 .forEach(stateBuilder::put);
        knownOldState.set(stateBuilder.build());
      }
      catch (InterruptedException ex) {
        allFuture.cancel(true);
        Thread.currentThread().interrupt();
        throw ex;
      }
      catch (Exception ex) {
        allFuture.cancel(true);
        throw ex;
      }

    }
    catch (Exception ex) {
      LOG.makeAlert(ex, "Failed to finish lookup management loop.").emit();
    }

    LOG.debug("Finished lookup sync for on all nodes.");
  }

  private LookupsState<LookupExtractorFactoryMapContainer> doLookupManagementOnNode(
      HostAndPortWithScheme node,
      Map<String, LookupExtractorFactoryMapContainer> nodeTierLookupsToBe
  ) throws IOException, InterruptedException, ExecutionException
  {
    LOG.debug("Starting lookup sync for node [%s].", node);

    LookupsState<LookupExtractorFactoryMapContainer> currLookupsStateOnNode = lookupsCommunicator.getLookupStateForNode(
        node
    );
    LOG.debug("Received lookups state from node [%s].", node);


    // Compare currLookupsStateOnNode with nodeTierLookupsToBe to find what are the lookups
    // we need to further ask node to load/drop
    Map<String, LookupExtractorFactoryMapContainer> toLoad = getToBeLoadedOnNode(
        currLookupsStateOnNode,
        nodeTierLookupsToBe
    );
    Set<String> toDrop = getToBeDroppedFromNode(currLookupsStateOnNode, nodeTierLookupsToBe);

    if (!toLoad.isEmpty() || !toDrop.isEmpty()) {
      // Send POST request to the node asking to load and drop the lookups necessary.
      // no need to send "current" in the LookupsStateWithMap , that is not required
      currLookupsStateOnNode = lookupsCommunicator.updateNode(node, new LookupsState<>(null, toLoad, toDrop));

      LOG.debug(
          "Sent lookup toAdd[%s] and toDrop[%s] updates to node [%s].",
          toLoad.keySet(),
          toDrop,
          node
      );
    }

    LOG.debug("Finished lookup sync for node [%s].", node);
    return currLookupsStateOnNode;
  }

  // Returns the Map<lookup-name, lookup-spec> that needs to be loaded by the node and it does not know about
  // those already.
  // It is assumed that currLookupsStateOnNode "toLoad" and "toDrop" are disjoint.
  @VisibleForTesting
  Map<String, LookupExtractorFactoryMapContainer> getToBeLoadedOnNode(
      LookupsState<LookupExtractorFactoryMapContainer> currLookupsStateOnNode,
      Map<String, LookupExtractorFactoryMapContainer> nodeTierLookupsToBe
  )
  {
    Map<String, LookupExtractorFactoryMapContainer> toLoad = new HashMap<>();
    for (Map.Entry<String, LookupExtractorFactoryMapContainer> e : nodeTierLookupsToBe.entrySet()) {
      String name = e.getKey();
      LookupExtractorFactoryMapContainer lookupToBe = e.getValue();

      // get it from the current pending notices list on the node
      LookupExtractorFactoryMapContainer current = currLookupsStateOnNode.getToLoad().get(name);

      if (current == null) {
        //ok, not on pending list, get from currently loaded lookups on node
        current = currLookupsStateOnNode.getCurrent().get(name);
      }

      if (current == null || //lookup is neither pending nor already loaded on the node OR
          currLookupsStateOnNode.getToDrop().contains(name) || //it is being dropped on the node OR
          lookupToBe.replaces(current) //lookup is already know to node, but lookupToBe overrides that
          ) {
        toLoad.put(name, lookupToBe);
      }
    }
    return toLoad;
  }

  // Returns Set<lookup-name> that should be dropped from the node which has them already either in pending to load
  // state or loaded
  // It is assumed that currLookupsStateOnNode "toLoad" and "toDrop" are disjoint.
  @VisibleForTesting
  Set<String> getToBeDroppedFromNode(
      LookupsState<LookupExtractorFactoryMapContainer> currLookupsStateOnNode,
      Map<String, LookupExtractorFactoryMapContainer> nodeTierLookupsToBe
  )
  {
    Set<String> toDrop = new HashSet<>();

    // {currently loading/loaded on the node} - {currently pending deletion on node} - {lookups node should actually have}
    toDrop.addAll(currLookupsStateOnNode.getCurrent().keySet());
    toDrop.addAll(currLookupsStateOnNode.getToLoad().keySet());
    toDrop = Sets.difference(toDrop, currLookupsStateOnNode.getToDrop());
    toDrop = Sets.difference(toDrop, nodeTierLookupsToBe.keySet());
    return toDrop;
  }

  static URL getLookupsURL(HostAndPortWithScheme druidNode) throws MalformedURLException
  {
    return new URL(
        druidNode.getScheme(),
        druidNode.getHostText(),
        druidNode.getPortOrDefault(-1),
        LOOKUP_BASE_REQUEST_PATH
    );
  }

  static URL getLookupsUpdateURL(HostAndPortWithScheme druidNode) throws MalformedURLException
  {
    return new URL(
        druidNode.getScheme(),
        druidNode.getHostText(),
        druidNode.getPortOrDefault(-1),
        LOOKUP_UPDATE_REQUEST_PATH
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
  public static class LookupsCommunicator
  {
    private final HttpClient httpClient;
    private final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig;
    private final ObjectMapper smileMapper;

    public LookupsCommunicator(
        HttpClient httpClient,
        LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig,
        ObjectMapper smileMapper
    )
    {
      this.httpClient = httpClient;
      this.lookupCoordinatorManagerConfig = lookupCoordinatorManagerConfig;
      this.smileMapper = smileMapper;
    }

    public LookupsState<LookupExtractorFactoryMapContainer> updateNode(
        HostAndPortWithScheme node,
        LookupsState<LookupExtractorFactoryMapContainer> lookupsUpdate
    )
        throws IOException, InterruptedException, ExecutionException
    {
      final AtomicInteger returnCode = new AtomicInteger(0);
      final AtomicReference<String> reasonString = new AtomicReference<>(null);

      final URL url = getLookupsUpdateURL(node);

      LOG.debug("Sending lookups load/drop request to [%s]. Request [%s]", url, lookupsUpdate);

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
            final LookupsState<LookupExtractorFactoryMapContainer> response = smileMapper.readValue(
                result,
                LOOKUPS_STATE_TYPE_REFERENCE
            );
            LOG.debug(
                "Update on [%s], Status: %s reason: [%s], Response [%s].", url, returnCode.get(), reasonString.get(),
                response
            );
            return response;
          }
          catch (IOException ex) {
            throw new IOE(ex, "Failed to parse update response from [%s]. response [%s]", url, result);
          }
        } else {
          final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            StreamUtils.copyAndClose(result, baos);
          }
          catch (IOException e2) {
            LOG.warn(e2, "Error reading response");
          }

          throw new IOE(
              "Bad update request to [%s] : [%d] : [%s]  Response: [%s]",
              url,
              returnCode.get(),
              reasonString.get(),
              StringUtils.fromUtf8(baos.toByteArray())
          );
        }
      }
    }

    public LookupsState<LookupExtractorFactoryMapContainer> getLookupStateForNode(
        HostAndPortWithScheme node
    ) throws IOException, InterruptedException, ExecutionException
    {
      final URL url = getLookupsURL(node);
      final AtomicInteger returnCode = new AtomicInteger(0);
      final AtomicReference<String> reasonString = new AtomicReference<>(null);

      LOG.debug("Getting lookups from [%s]", url);

      try (final InputStream result = httpClient.go(
          new Request(HttpMethod.GET, url)
              .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
          makeResponseHandler(returnCode, reasonString),
          lookupCoordinatorManagerConfig.getHostTimeout()
      ).get()) {
        if (returnCode.get() == HttpURLConnection.HTTP_OK) {
          try {
            final LookupsState<LookupExtractorFactoryMapContainer> response = smileMapper.readValue(
                result,
                LOOKUPS_STATE_TYPE_REFERENCE
            );
            LOG.debug(
                "Get on [%s], Status: [%s] reason: [%s], Response [%s].",
                url,
                returnCode.get(),
                reasonString.get(),
                response
            );
            return response;
          }
          catch (IOException ex) {
            throw new IOE(ex, "Failed to parser GET lookups response from [%s]. response [%s].", url, result);
          }
        } else {
          final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          try {
            StreamUtils.copyAndClose(result, baos);
          }
          catch (IOException ex) {
            LOG.warn(ex, "Error reading response from GET on url [%s]", url);
          }

          throw new IOE(
              "GET request failed to [%s] : [%d] : [%s]  Response: [%s]",
              url,
              returnCode.get(),
              reasonString.get(),
              StringUtils.fromUtf8(baos.toByteArray())
          );
        }
      }
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
        public ClientResponse<InputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
        {
          returnCode.set(response.getStatus().getCode());
          reasonString.set(response.getStatus().getReasonPhrase());
          return super.handleResponse(response, trafficCop);
        }
      };
    }
  }
}
