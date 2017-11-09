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

package io.druid.query.lookup;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.client.coordinator.Coordinator;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DruidLeaderClient;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class provide a basic {@link LookupExtractorFactory} references manager.
 * It allows basic operations fetching, listing, adding and deleting of {@link LookupExtractor} objects
 * It is be used by queries to fetch the lookup reference.
 * It is used by Lookup configuration manager to add/remove or list lookups configuration via HTTP or other protocols.
 * It does periodic snap shot of the list of lookup in order to bootstrap nodes after restart.
 */
@ManageLifecycle
public class LookupReferencesManager
{
  private static final EmittingLogger LOG = new EmittingLogger(LookupReferencesManager.class);

  private static final TypeReference<Map<String, LookupExtractorFactoryContainer>> LOOKUPS_ALL_REFERENCE =
      new TypeReference<Map<String, LookupExtractorFactoryContainer>>()
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
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      LOG.info("LookupReferencesManager is starting.");
      loadAllLookupsAndInitStateRef();
      if (!testMode) {
        mainThread = Execs.makeThread(
            "LookupReferencesManager-MainThread",
            () -> {
              try {
                if (!lifecycleLock.awaitStarted()) {
                  LOG.error("WTF! lifecycle not started, lookup update notices will not be handled.");
                  return;
                }

                while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
                  try {
                    handlePendingNotices();
                    LockSupport.parkNanos(LookupReferencesManager.this, TimeUnit.MINUTES.toNanos(1));
                  }
                  catch (Throwable t) {
                    LOG.makeAlert(t, "Error occured while lookup notice handling.").emit();
                  }
                }
              }
              catch (Throwable t) {
                LOG.error(t, "Error while waiting for lifecycle start. lookup updates notices will not be handled");
              }
              finally {
                LOG.info("Lookup Management loop exited, Lookup notices are not handled anymore.");
              }
            },
            true
        );

        mainThread.start();
      }

      LOG.info("LookupReferencesManager is started.");
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
        notice.handle(lookupMap);
      }
      catch (Exception ex) {
        LOG.error(ex, "Exception occured while handling lookup notice [%s].", notice);
        LOG.makeAlert("Exception occured while handling lookup notice, with message [%s].", ex.getMessage()).emit();
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

    LOG.info("LookupReferencesManager is stopping.");

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
        LOG.info("Closing lookup [%s]", e.getKey());
        if (!e.getValue().getLookupExtractorFactory().close()) {
          LOG.error("Failed to close lookup [%s].", e.getKey());
        }
      }
      catch (Exception ex) {
        LOG.error(ex, "Failed to close lookup [%s].", e.getKey());
      }
    }

    LOG.info("LookupReferencesManager is stopped.");
  }

  public void add(String lookupName, LookupExtractorFactoryContainer lookupExtractorFactoryContainer)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    addNotice(new LoadNotice(lookupName, lookupExtractorFactoryContainer, lookupConfig.getLookupStartRetries()));
  }

  public void remove(String lookupName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    addNotice(new DropNotice(lookupName));
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

          return new LookupUpdateState(
              oldState.lookupMap, builder.build(), oldState.noticesBeingHandled

          );
        }
    );
    LockSupport.unpark(mainThread);
  }

  @Nullable
  public LookupExtractorFactoryContainer get(String lookupName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return stateRef.get().lookupMap.get(lookupName);
  }

  // Note that this should ensure that "toLoad" and "toDrop" are disjoint.
  public LookupsState<LookupExtractorFactoryContainer> getAllLookupsState()
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
      lookupSnapshotTaker.takeSnapshot(getLookupBeanList(lookupMap));
    }
  }

  private void loadAllLookupsAndInitStateRef()
  {
    List<LookupBean> lookupBeanList = getLookupsList();
    if (lookupBeanList != null) {
      startLookups(lookupBeanList);
    } else {
      LOG.info("No lookups to be loaded at this point");
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
      String tier = lookupListeningAnnouncerConfig.getLookupTier();
      lookupBeanList = getLookupListFromCoordinator(tier);
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
            } else {
              // Adding an extra minute in addition to the retry wait. In RetryUtils, retry wait starts from a few
              // seconds, that is likely not enough to coordinator to be back to healthy state, e. g. if it experiences
              // 30-second GC pause.
              Thread.sleep(60_000);
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
  private Map<String, LookupExtractorFactoryContainer> tryGetLookupListFromCoordinator(String tier) throws Exception
  {
    final FullResponseHolder response = fetchLookupsForTier(tier);
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
      return jsonMapper.readValue(response.getContent(), LOOKUPS_ALL_REFERENCE);
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
      return lookupSnapshotTaker.pullExistingSnapshot();
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
    ImmutableMap.Builder<String, LookupExtractorFactoryContainer> builder = ImmutableMap.builder();
    ExecutorService executorService = Execs.multiThreaded(
        lookupConfig.getNumLookupLoadingThreads(),
        "LookupReferencesManager-Startup-%s"
    );
    CompletionService<Map.Entry<String, LookupExtractorFactoryContainer>> completionService =
        new ExecutorCompletionService<>(executorService);
    try {
      LOG.info("Starting lookup loading process");
      List<LookupBean> remainingLookups = lookupBeanList;
      for (int i = 0; i < lookupConfig.getLookupStartRetries(); i++) {
        LOG.info("Round of attempts #%d, [%d] lookups", i + 1, remainingLookups.size());
        Map<String, LookupExtractorFactoryContainer> successfulLookups =
            startLookups(remainingLookups, completionService);
        builder.putAll(successfulLookups);
        List<LookupBean> failedLookups = remainingLookups
            .stream()
            .filter(l -> !successfulLookups.containsKey(l.getName()))
            .collect(Collectors.toList());
        if (failedLookups.isEmpty()) {
          break;
        } else {
          // next round
          remainingLookups = failedLookups;
        }
      }
      LOG.info(
          "Failed to start the following lookups after [%d] attempts: [%s]",
          lookupConfig.getLookupStartRetries(),
          remainingLookups
      );
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

  private FullResponseHolder fetchLookupsForTier(String tier)
      throws ExecutionException, InterruptedException, IOException
  {
    return druidLeaderClient.go(
        druidLeaderClient.makeRequest(
            HttpMethod.GET,
            StringUtils.format("/druid/coordinator/v1/lookups/config/%s?detailed=true", tier)
        )
    );
  }

  @VisibleForTesting
  interface Notice
  {
    void handle(Map<String, LookupExtractorFactoryContainer> lookupMap) throws Exception;
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
    public void handle(Map<String, LookupExtractorFactoryContainer> lookupMap) throws Exception
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

      old = lookupMap.put(lookupName, lookupExtractorFactoryContainer);

      LOG.debug("Loaded lookup [%s] with spec [%s].", lookupName, lookupExtractorFactoryContainer);

      if (old != null) {
        if (!old.getLookupExtractorFactory().close()) {
          throw new ISE("close method returned false for lookup [%s]:[%s]", lookupName, old);
        }
      }
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

    DropNotice(String lookupName)
    {
      this.lookupName = lookupName;
    }

    @Override
    public void handle(Map<String, LookupExtractorFactoryContainer> lookupMap)
    {
      final LookupExtractorFactoryContainer lookupExtractorFactoryContainer = lookupMap.remove(lookupName);

      if (lookupExtractorFactoryContainer != null) {
        LOG.debug("Removed lookup [%s] with spec [%s].", lookupName, lookupExtractorFactoryContainer);

        if (!lookupExtractorFactoryContainer.getLookupExtractorFactory().close()) {
          throw new ISE(
              "close method returned false for lookup [%s]:[%s]",
              lookupName,
              lookupExtractorFactoryContainer
          );
        }
      }
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
