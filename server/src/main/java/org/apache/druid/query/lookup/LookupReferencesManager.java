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

package org.apache.druid.query.lookup;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;

/**
 * This class provide a basic {@link LookupExtractorFactory} references manager. It allows basic operations fetching,
 * listing, adding and deleting of {@link LookupExtractor} objects, and can take periodic snap shot of the loaded lookup
 * extractor specifications in order to bootstrap nodes after restart.
 *
 * It also implements {@link LookupExtractorFactoryContainerProvider}, to supply queries and indexing transformations
 * with a reference to a {@link LookupExtractorFactoryContainer}. This class is a companion of
 * {@link org.apache.druid.server.lookup.cache.LookupCoordinatorManager}, which communicates with
 * {@link LookupReferencesManager} through {@link LookupListeningResource}.
 */
@ManageLifecycle
public class LookupReferencesManager implements LookupExtractorFactoryContainerProvider
{
  private static final EmittingLogger LOG = new EmittingLogger(LookupReferencesManager.class);

  private static final TypeReference<Map<String, Object>> LOOKUPS_ALL_GENERIC_REFERENCE =
      new TypeReference<Map<String, Object>>()
      {
      };

  // Lookups state (loaded/to-be-loaded/to-be-dropped etc) is managed by immutable LookupUpdateState instance.
  // Any update to state is done by creating updated LookupUpdateState instance and atomically setting that
  // into the ref here.
  // this allows getAllLookupsState() to provide a consistent view without using locks.
  @VisibleForTesting
  final AtomicReference<LookupUpdateState> stateRef = new AtomicReference<>();

  @VisibleForTesting
  final LookupSnapshotTaker lookupSnapshotTaker;

  @VisibleForTesting
  final LifecycleLock lifecycleLock = new LifecycleLock();

  @VisibleForTesting
  Thread mainThread;

  //for unit testing only
  private final boolean testMode;

  private final DruidLeaderClient druidLeaderClient;

  private final ObjectMapper jsonMapper;

  private final LookupListeningAnnouncerConfig lookupListeningAnnouncerConfig;

  private final LookupConfig lookupConfig;

  private ExecutorService lookupUpdateExecutorService;

  @Inject
  public LookupReferencesManager(
      LookupConfig lookupConfig,
      @Json ObjectMapper objectMapper,
      @Coordinator DruidLeaderClient druidLeaderClient,
      LookupListeningAnnouncerConfig lookupListeningAnnouncerConfig
  )
  {
    this(lookupConfig, objectMapper, druidLeaderClient, lookupListeningAnnouncerConfig, false);
  }

  @VisibleForTesting
  LookupReferencesManager(
      LookupConfig lookupConfig,
      ObjectMapper objectMapper,
      DruidLeaderClient druidLeaderClient,
      LookupListeningAnnouncerConfig lookupListeningAnnouncerConfig,
      boolean testMode
  )
  {
    if (Strings.isNullOrEmpty(lookupConfig.getSnapshotWorkingDir())) {
      this.lookupSnapshotTaker = null;
    } else {
      this.lookupSnapshotTaker = new LookupSnapshotTaker(objectMapper, lookupConfig.getSnapshotWorkingDir());
    }
    this.druidLeaderClient = druidLeaderClient;
    this.jsonMapper = objectMapper;
    this.lookupListeningAnnouncerConfig = lookupListeningAnnouncerConfig;
    this.lookupConfig = lookupConfig;
    this.testMode = testMode;
    this.lookupUpdateExecutorService = Execs.multiThreaded(
        lookupConfig.getNumLookupLoadingThreads(),
        "LookupExtractorFactoryContainerProvider-Update-%s"
    );
  }

  @LifecycleStart
  public void start() throws IOException
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      LOG.debug("LookupExtractorFactoryContainerProvider starting.");
      if (!Strings.isNullOrEmpty(lookupConfig.getSnapshotWorkingDir())) {
        FileUtils.mkdirp(new File(lookupConfig.getSnapshotWorkingDir()));
      }
      loadAllLookupsAndInitStateRef();
      if (!testMode) {
        mainThread = Execs.makeThread(
            "LookupExtractorFactoryContainerProvider-MainThread",
            () -> {
              try {
                if (!lifecycleLock.awaitStarted()) {
                  LOG.error("Lifecycle not started, lookup update notices will not be handled.");
                  return;
                }

                while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                  try {
                    handlePendingNotices();
                    LockSupport.parkNanos(LookupReferencesManager.this, TimeUnit.MINUTES.toNanos(1));
                  }
                  catch (Throwable t) {
                    LOG.makeAlert(t, "Error occurred while lookup notice handling.").emit();
                  }
                }
              }
              catch (Throwable t) {
                LOG.error(t, "Error while waiting for lifecycle start. lookup updates notices will not be handled");
              }
              finally {
                LOG.info("Lookup Management loop exited. Lookup notices are not handled anymore.");
              }
            },
            true
        );

        mainThread.start();
      }

      LOG.debug("LookupExtractorFactoryContainerProvider started.");
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @VisibleForTesting
  void handlePendingNotices()
  {
    if (stateRef.get().pendingNotices.isEmpty()) {
      return;
    }

    @SuppressWarnings("ArgumentParameterSwap")
    LookupUpdateState swappedState = atomicallyUpdateStateRef(
        oldState -> new LookupUpdateState(oldState.lookupMap, ImmutableList.of(), oldState.pendingNotices)
    );

    Map<String, LookupExtractorFactoryContainer> lookupMap = new HashMap<>(swappedState.lookupMap);
    for (Notice notice : swappedState.noticesBeingHandled) {
      try {
        notice.handle(lookupMap, this);
      }
      catch (Exception ex) {
        LOG.error(ex, "Exception occurred while handling lookup notice [%s].", notice);
        LOG.makeAlert("Exception occurred while handling lookup notice, with message [%s].", ex.getMessage()).emit();
      }
    }

    takeSnapshot(lookupMap);

    ImmutableMap<String, LookupExtractorFactoryContainer> immutableLookupMap = ImmutableMap.copyOf(lookupMap);

    atomicallyUpdateStateRef(
        oldState -> new LookupUpdateState(immutableLookupMap, oldState.pendingNotices, ImmutableList.of())
    );
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    LOG.debug("LookupExtractorFactoryContainerProvider is stopping.");

    if (!testMode) {
      mainThread.interrupt();

      try {
        mainThread.join();
      }
      catch (InterruptedException ex) {
        throw new ISE("failed to stop, mainThread couldn't finish.");
      }
    }

    for (Map.Entry<String, LookupExtractorFactoryContainer> e : stateRef.get().lookupMap.entrySet()) {
      try {
        if (e.getValue().getLookupExtractorFactory().close()) {
          LOG.info("Closed lookup [%s].", e.getKey());
        } else {
          LOG.error("Failed to close lookup [%s].", e.getKey());
        }
      }
      catch (Exception ex) {
        LOG.error(ex, "Failed to close lookup [%s].", e.getKey());
      }
    }
    lookupUpdateExecutorService.shutdown();
    LOG.debug("LookupExtractorFactoryContainerProvider is stopped.");
  }


  public void add(String lookupName, LookupExtractorFactoryContainer lookupExtractorFactoryContainer)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    addNotice(new LoadNotice(lookupName, lookupExtractorFactoryContainer, lookupConfig.getLookupStartRetries()));
  }

  public void remove(String lookupName, LookupExtractorFactoryContainer loadedContainer)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    addNotice(new DropNotice(lookupName, loadedContainer));
  }

  private void addNotice(Notice notice)
  {
    atomicallyUpdateStateRef(
        oldState -> {
          if (oldState.pendingNotices.size() > 10000) { //don't let pendingNotices grow indefinitely
            throw new ISE("There are too many [%d] pendingNotices.", oldState.pendingNotices.size());
          }

          ImmutableList.Builder<Notice> builder = ImmutableList.builder();
          builder.addAll(oldState.pendingNotices);
          builder.add(notice);

          return new LookupUpdateState(oldState.lookupMap, builder.build(), oldState.noticesBeingHandled);
        }
    );
    LockSupport.unpark(mainThread);
  }

  public void submitAsyncLookupTask(Runnable task)
  {
    lookupUpdateExecutorService.submit(task);
  }

  @Override
  public Optional<LookupExtractorFactoryContainer> get(String lookupName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return Optional.ofNullable(stateRef.get().lookupMap.get(lookupName));
  }

  @Override
  public Set<String> getAllLookupNames()
  {
    if (stateRef.get() == null) {
      return Collections.emptySet();
    }
    return stateRef.get().lookupMap.keySet();
  }

  // Note that this should ensure that "toLoad" and "toDrop" are disjoint.
  LookupsState<LookupExtractorFactoryContainer> getAllLookupsState()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    LookupUpdateState lookupUpdateState = stateRef.get();

    Map<String, LookupExtractorFactoryContainer> lookupsToLoad = new HashMap<>();
    Set<String> lookupsToDrop = new HashSet<>();

    updateToLoadAndDrop(lookupUpdateState.noticesBeingHandled, lookupsToLoad, lookupsToDrop);
    updateToLoadAndDrop(lookupUpdateState.pendingNotices, lookupsToLoad, lookupsToDrop);

    return new LookupsState<>(lookupUpdateState.lookupMap, lookupsToLoad, lookupsToDrop);
  }

  private void updateToLoadAndDrop(
      List<Notice> notices,
      Map<String, LookupExtractorFactoryContainer> lookupsToLoad,
      Set<String> lookupsToDrop
  )
  {
    for (Notice notice : notices) {
      if (notice instanceof LoadNotice) {
        LoadNotice loadNotice = (LoadNotice) notice;
        lookupsToLoad.put(loadNotice.lookupName, loadNotice.lookupExtractorFactoryContainer);
        lookupsToDrop.remove(loadNotice.lookupName);
      } else if (notice instanceof DropNotice) {
        DropNotice dropNotice = (DropNotice) notice;
        lookupsToDrop.add(dropNotice.lookupName);
        lookupsToLoad.remove(dropNotice.lookupName);
      } else {
        throw new ISE("Unknown Notice type [%s].", notice.getClass().getName());
      }
    }
  }

  private void takeSnapshot(Map<String, LookupExtractorFactoryContainer> lookupMap)
  {
    if (lookupSnapshotTaker != null) {
      lookupSnapshotTaker.takeSnapshot(lookupListeningAnnouncerConfig.getLookupTier(), getLookupBeanList(lookupMap));
    }
  }

  private void loadAllLookupsAndInitStateRef()
  {
    List<LookupBean> lookupBeanList = getLookupsList();
    if (lookupBeanList != null) {
      startLookups(lookupBeanList);
    } else {
      LOG.debug("No lookups to be loaded at this point.");
      stateRef.set(new LookupUpdateState(ImmutableMap.of(), ImmutableList.of(), ImmutableList.of()));
    }
  }

  /**
   * Gets the lookup list from coordinator or from snapshot.
   */
  @Nullable
  private List<LookupBean> getLookupsList()
  {
    List<LookupBean> lookupBeanList;
    if (lookupConfig.getEnableLookupSyncOnStartup()) {
      lookupBeanList = getLookupListFromCoordinator(lookupListeningAnnouncerConfig.getLookupTier());
      if (lookupBeanList == null) {
        LOG.info("Coordinator is unavailable. Loading saved snapshot instead");
        lookupBeanList = getLookupListFromSnapshot();
      }
    } else {
      lookupBeanList = getLookupListFromSnapshot();
    }
    return lookupBeanList;
  }

  /**
   * Returns a list of lookups from the coordinator if the coordinator is available. If it's not available, returns null.
   *
   * @param tier lookup tier name
   *
   * @return list of LookupBean objects, or null
   */
  @Nullable
  private List<LookupBean> getLookupListFromCoordinator(String tier)
  {
    try {
      MutableBoolean firstAttempt = new MutableBoolean(true);
      Map<String, LookupExtractorFactoryContainer> lookupMap = RetryUtils.retry(
          () -> {
            if (firstAttempt.isTrue()) {
              firstAttempt.setValue(false);
            } else if (lookupConfig.getCoordinatorRetryDelay() > 0) {
              // Adding any configured extra time in addition to the retry wait. In RetryUtils, retry wait starts from
              // a few seconds, that is likely not enough to coordinator to be back to healthy state, e. g. if it
              // experiences 30-second GC pause. Default is 1 minute
              Thread.sleep(lookupConfig.getCoordinatorRetryDelay());
            }
            return tryGetLookupListFromCoordinator(tier);
          },
          e -> true,
          lookupConfig.getCoordinatorFetchRetries()
      );
      if (lookupMap != null) {
        List<LookupBean> lookupBeanList = new ArrayList<>();
        lookupMap.forEach((k, v) -> lookupBeanList.add(new LookupBean(k, null, v)));
        return lookupBeanList;
      } else {
        return null;
      }
    }
    catch (Exception e) {
      LOG.error(e, "Error while trying to get lookup list from coordinator for tier[%s]", tier);
      return null;
    }
  }

  @Nullable
  private Map<String, LookupExtractorFactoryContainer> tryGetLookupListFromCoordinator(String tier)
      throws IOException, InterruptedException
  {
    final StringFullResponseHolder response = fetchLookupsForTier(tier);
    if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)) {
      LOG.warn("No lookups found for tier [%s], response [%s]", tier, response);
      return null;
    } else if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new IOE(
          "Error while fetching lookup code from Coordinator with status[%s] and content[%s]",
          response.getStatus(),
          response.getContent()
      );
    }

    // Older version of getSpecificTier returns a list of lookup names.
    // Lookup loading is performed via snapshot if older version is present.
    // This check is only for backward compatibility and should be removed in a future release
    if (response.getContent().startsWith("[")) {
      LOG.info(
          "Failed to retrieve lookup information from coordinator, " +
          "because coordinator appears to be running on older Druid version. " +
          "Attempting to load lookups using snapshot instead"
      );
      return null;
    } else {
      Map<String, Object> lookupNameToGenericConfig =
          jsonMapper.readValue(response.getContent(), LOOKUPS_ALL_GENERIC_REFERENCE);
      return LookupUtils.tryConvertObjectMapToLookupConfigMap(
          lookupNameToGenericConfig,
          jsonMapper
      );
    }
  }

  /**
   * Returns a list of lookups from the snapshot if the lookupsnapshottaker is configured. If it's not available,
   * returns null.
   *
   * @return list of LookupBean objects, or null
   */
  @Nullable
  private List<LookupBean> getLookupListFromSnapshot()
  {
    if (lookupSnapshotTaker != null) {
      return lookupSnapshotTaker.pullExistingSnapshot(lookupListeningAnnouncerConfig.getLookupTier());
    }
    return null;
  }

  private List<LookupBean> getLookupBeanList(Map<String, LookupExtractorFactoryContainer> lookupMap)
  {
    List<LookupBean> lookups = new ArrayList<>(lookupMap.size());
    for (Map.Entry<String, LookupExtractorFactoryContainer> e : lookupMap.entrySet()) {
      lookups.add(new LookupBean(e.getKey(), null, e.getValue()));
    }
    return lookups;
  }

  private void startLookups(final List<LookupBean> lookupBeanList)
  {
    final ImmutableMap.Builder<String, LookupExtractorFactoryContainer> builder = ImmutableMap.builder();
    final ExecutorService executorService = Execs.multiThreaded(
        lookupConfig.getNumLookupLoadingThreads(),
        "LookupExtractorFactoryContainerProvider-Startup-%s"
    );
    final CompletionService<Map.Entry<String, LookupExtractorFactoryContainer>> completionService =
        new ExecutorCompletionService<>(executorService);
    final List<LookupBean> remainingLookups = new ArrayList<>(lookupBeanList);
    try {
      LOG.info("Starting lookup loading process.");
      for (int i = 0; i < lookupConfig.getLookupStartRetries() && !remainingLookups.isEmpty(); i++) {
        LOG.info("Round of attempts #%d, [%d] lookups", i + 1, remainingLookups.size());
        final Map<String, LookupExtractorFactoryContainer> successfulLookups =
            startLookups(remainingLookups, completionService);
        builder.putAll(successfulLookups);
        remainingLookups.removeIf(l -> successfulLookups.containsKey(l.getName()));
      }
      if (!remainingLookups.isEmpty()) {
        LOG.warn(
            "Failed to start the following lookups after [%d] attempts: [%s]",
            lookupConfig.getLookupStartRetries(),
            remainingLookups
        );
      }
      stateRef.set(new LookupUpdateState(builder.build(), ImmutableList.of(), ImmutableList.of()));
    }
    catch (InterruptedException | RuntimeException e) {
      LOG.error(e, "Failed to finish lookup load process.");
    }
    finally {
      executorService.shutdownNow();
    }
  }

  /**
   * @return a map with successful lookups
   */
  private Map<String, LookupExtractorFactoryContainer> startLookups(
      List<LookupBean> lookupBeans,
      CompletionService<Map.Entry<String, LookupExtractorFactoryContainer>> completionService
  ) throws InterruptedException
  {
    for (LookupBean lookupBean : lookupBeans) {
      completionService.submit(() -> startLookup(lookupBean));
    }
    Map<String, LookupExtractorFactoryContainer> successfulLookups = new HashMap<>();
    for (int i = 0; i < lookupBeans.size(); i++) {
      Future<Map.Entry<String, LookupExtractorFactoryContainer>> completedFuture = completionService.take();
      try {
        Map.Entry<String, LookupExtractorFactoryContainer> lookupResult = completedFuture.get();
        if (lookupResult != null) {
          successfulLookups.put(lookupResult.getKey(), lookupResult.getValue());
        }
      }
      catch (ExecutionException e) {
        LOG.error(e.getCause(), "Exception while starting a lookup");
        // not adding to successfulLookups
      }
    }
    return successfulLookups;
  }

  @Nullable
  private Map.Entry<String, LookupExtractorFactoryContainer> startLookup(LookupBean lookupBean)
  {
    LookupExtractorFactoryContainer container = lookupBean.getContainer();
    LOG.info("Starting lookup [%s]:[%s]", lookupBean.getName(), container);
    try {
      if (container.getLookupExtractorFactory().start()) {
        LOG.info("Started lookup [%s]:[%s]", lookupBean.getName(), container);
        return new AbstractMap.SimpleImmutableEntry<>(lookupBean.getName(), container);
      } else {
        LOG.error("Failed to start lookup [%s]:[%s]", lookupBean.getName(), container);
        return null;
      }
    }
    catch (RuntimeException e) {
      throw new RE(e, "Failed to start lookup [%s]:[%s]", lookupBean.getName(), container);
    }
  }

  private LookupUpdateState atomicallyUpdateStateRef(Function<LookupUpdateState, LookupUpdateState> fn)
  {
    while (true) {
      LookupUpdateState old = stateRef.get();
      LookupUpdateState newState = fn.apply(old);
      if (stateRef.compareAndSet(old, newState)) {
        return newState;
      }
    }
  }

  private StringFullResponseHolder fetchLookupsForTier(String tier) throws InterruptedException, IOException
  {
    return druidLeaderClient.go(
        druidLeaderClient.makeRequest(
            HttpMethod.GET,
            StringUtils.format("/druid/coordinator/v1/lookups/config/%s?detailed=true", tier)
        )
    );
  }
  private void dropContainer(LookupExtractorFactoryContainer container, String lookupName)
  {
    if (container != null) {
      LOG.debug("Removed lookup [%s] with spec [%s].", lookupName, container);

      if (!container.getLookupExtractorFactory().destroy()) {
        throw new ISE(
            "destroy method returned false for lookup [%s]:[%s]",
            lookupName,
            container
        );
      }
    }
  }
  @VisibleForTesting
  interface Notice
  {
    void handle(Map<String, LookupExtractorFactoryContainer> lookupMap, LookupReferencesManager manager) throws Exception;
  }

  private static class LoadNotice implements Notice
  {
    private final String lookupName;
    private final LookupExtractorFactoryContainer lookupExtractorFactoryContainer;
    private final int startRetries;

    LoadNotice(String lookupName, LookupExtractorFactoryContainer lookupExtractorFactoryContainer, int startRetries)
    {
      this.lookupName = lookupName;
      this.lookupExtractorFactoryContainer = lookupExtractorFactoryContainer;
      this.startRetries = startRetries;
    }

    @Override
    public void handle(Map<String, LookupExtractorFactoryContainer> lookupMap, LookupReferencesManager manager)
        throws Exception
    {
      LookupExtractorFactoryContainer old = lookupMap.get(lookupName);
      if (old != null && !lookupExtractorFactoryContainer.replaces(old)) {
        LOG.warn(
            "got notice to load lookup [%s] that can't replace existing [%s].",
            lookupExtractorFactoryContainer,
            old
        );
        return;
      }

      RetryUtils.retry(
          () -> {
            if (!lookupExtractorFactoryContainer.getLookupExtractorFactory().start()) {
              throw new ISE(
                  "start method returned false for lookup [%s]:[%s]",
                  lookupName,
                  lookupExtractorFactoryContainer
              );
            }
            return null;
          },
          e -> true,
          startRetries
      );
      /*
       if new container is initailized then add it to manager to start serving immediately.
       if old container is null then it is fresh load, we can skip waiting for initialization and add the container to registry first. Esp for MSQ workers.
       */
      if (old == null || lookupExtractorFactoryContainer.getLookupExtractorFactory().isInitialized()) {
        old = lookupMap.put(lookupName, lookupExtractorFactoryContainer);
        LOG.debug("Loaded lookup [%s] with spec [%s].", lookupName, lookupExtractorFactoryContainer);
        manager.dropContainer(old, lookupName);
        return;
      }
      manager.submitAsyncLookupTask(() -> {
        try {
        /*
        Retry startRetries times and wait for first cache to load for new container,
        if loaded then kill old container and start serving from new one.
        If new lookupExtractorFactoryContainer has errors in loading, kill the new container and do not remove the old container
         */
          RetryUtils.retry(
              () -> {
                lookupExtractorFactoryContainer.getLookupExtractorFactory().awaitInitialization();
                return null;
              }, e -> true,
              startRetries
          );
          if (lookupExtractorFactoryContainer.getLookupExtractorFactory().isInitialized()) {
            // send load notice with cache loaded container
            manager.add(lookupName, lookupExtractorFactoryContainer);
          } else {
            // skip loading new container as it is failed after 3 attempts
            manager.dropContainer(lookupExtractorFactoryContainer, lookupName);
          }
        }
        catch (Exception e) {
          // drop new failed container and continue serving old one
          LOG.error(
              e,
              "Exception in updating the namespace %s, continue serving from old container and killing new container ",
              lookupExtractorFactoryContainer
          );
          manager.dropContainer(lookupExtractorFactoryContainer, lookupName);
        }
      });
    }
    @Override
    public String toString()
    {
      return "LoadNotice{" +
             "lookupName='" + lookupName + '\'' +
             ", lookupExtractorFactoryContainer=" + lookupExtractorFactoryContainer +
             '}';
    }
  }

  private static class DropNotice implements Notice
  {
    private final String lookupName;
    private final LookupExtractorFactoryContainer loadedContainer;

    /**
     * @param lookupName      Name of the lookup to drop
     * @param loadedContainer Container ref to newly loaded container, this is mandatory in the update lookup call, it can be null in purely drop call.
     */
    DropNotice(String lookupName, @Nullable LookupExtractorFactoryContainer loadedContainer)
    {
      this.lookupName = lookupName;
      this.loadedContainer = loadedContainer;
    }

    @Override
    public void handle(Map<String, LookupExtractorFactoryContainer> lookupMap, LookupReferencesManager manager)
    {
      if (loadedContainer != null && !loadedContainer.getLookupExtractorFactory().isInitialized()) {
        final LookupExtractorFactoryContainer containterToDrop = lookupMap.get(lookupName);
        manager.submitAsyncLookupTask(() -> {
          try {
            loadedContainer.getLookupExtractorFactory().awaitInitialization();
            manager.dropContainer(containterToDrop, lookupName);
          }
          catch (InterruptedException | TimeoutException e) {
            // do nothing as loadedContainer is dropped by LoadNotice handler eventually if cache is not loaded
          }
        });
        return;
      }
      final LookupExtractorFactoryContainer lookupExtractorFactoryContainer = lookupMap.remove(lookupName);
      manager.dropContainer(lookupExtractorFactoryContainer, lookupName);
    }

    @Override
    public String toString()
    {
      return "DropNotice{" +
             "lookupName='" + lookupName + '\'' +
             '}';
    }
  }

  private static class LookupUpdateState
  {
    private final ImmutableMap<String, LookupExtractorFactoryContainer> lookupMap;
    private final ImmutableList<Notice> pendingNotices;
    private final ImmutableList<Notice> noticesBeingHandled;

    LookupUpdateState(
        ImmutableMap<String, LookupExtractorFactoryContainer> lookupMap,
        ImmutableList<Notice> pendingNotices,
        ImmutableList<Notice> noticesBeingHandled
    )
    {
      this.lookupMap = lookupMap;
      this.pendingNotices = pendingNotices;
      this.noticesBeingHandled = noticesBeingHandled;
    }
  }
}
