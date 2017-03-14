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


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.LifecycleLock;
import io.druid.concurrent.Threads;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

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

  private final Map<String, LookupExtractorFactoryContainer> lookupMap = new ConcurrentHashMap<>();

  @VisibleForTesting
  final BlockingQueue<Notice> queue = new LinkedBlockingDeque<>(10000);

  @VisibleForTesting
  final LookupSnapshotTaker lookupSnapshotTaker;

  @VisibleForTesting
  final LifecycleLock lifecycleLock = new LifecycleLock();

  @VisibleForTesting
  Thread mainThread;

  //for unit testing only
  private final boolean testMode;

  @Inject
  public LookupReferencesManager(LookupConfig lookupConfig, @Json ObjectMapper objectMapper)
  {
    this(lookupConfig, objectMapper, false);
  }

  @VisibleForTesting
  LookupReferencesManager(LookupConfig lookupConfig, ObjectMapper objectMapper, boolean testMode)
  {
    if (Strings.isNullOrEmpty(lookupConfig.getSnapshotWorkingDir())) {
      this.lookupSnapshotTaker = null;
    } else {
      this.lookupSnapshotTaker = new LookupSnapshotTaker(objectMapper, lookupConfig.getSnapshotWorkingDir());
    }

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

      loadSnapshot();

      if (!testMode) {
        mainThread = Threads.createThread(
            "LookupReferencesManager-MainThread",
            new Runnable()
            {
              @Override
              public void run()
              {
                try {

                  if (!lifecycleLock.awaitStarted()) {
                    LOG.error("WTF! lifecycle not started, lookup update notices will not be handled.");
                    return;
                  }

                  while (!Thread.interrupted()) {
                    try {
                      queue.take().handle();
                    }
                    catch (InterruptedException ex) {
                      LOG.warn(ex, "interrupted, going down... lookups are not managed anymore");
                      Thread.currentThread().interrupt();
                    }
                    catch (Exception ex) {
                      LOG.makeAlert(ex, "Exception occured while lookup notice handling.").emit();
                    }
                    catch (Throwable t) {
                      LOG.makeAlert(t, "Fatal error occured while lookup notice handling.").emit();
                      throw t;
                    }
                  }
                }
                catch (Throwable t) {
                  LOG.error(t, "Error while waiting for lifecycle start. lookup updates notices will not be handled");
                }
                finally {
                  LOG.info("Lookup Management loop exited, Lookup notices are not handled anymore.");
                }
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

    for (Map.Entry<String, LookupExtractorFactoryContainer> e : lookupMap.entrySet()) {
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

    lookupMap.clear();

    LOG.info("LookupReferencesManager is stopped.");
  }

  public void add(String lookupName, LookupExtractorFactoryContainer lookupExtractorFactoryContainer)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    try {
      if (!queue.offer(new LoadNotice(lookupName, lookupExtractorFactoryContainer), 1, TimeUnit.MILLISECONDS)) {
        throw new ISE("notice queue add timedout to add [%s] lookup load notice", lookupName);
      }
    } catch (InterruptedException ex) {
      throw new ISE(ex, "failed to add [%s] lookup load notice", lookupName);
    }
  }

  public void remove(String lookupName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    try {
      if (!queue.offer(new DropNotice(lookupName), 1, TimeUnit.MILLISECONDS)) {
        throw new ISE("notice queue add timedout to add [%s] lookup drop notice", lookupName);
      }
    } catch (InterruptedException ex) {
      throw new ISE(ex, "failed to add [%s] lookup drop notice", lookupName);
    }
  }

  @Nullable
  public LookupExtractorFactoryContainer get(String lookupName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return lookupMap.get(lookupName);
  }

  public LookupsState getAllLookupsState()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    Map<String, LookupExtractorFactoryContainer> lookupsToLoad = new HashMap<>();
    Set<String> lookupsToDrop = new HashSet<>();

    for (Notice notice : queue) {
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

    return new LookupsState(Maps.newHashMap(lookupMap), lookupsToLoad, lookupsToDrop);
  }

  private void takeSnapshot()
  {
    if (lookupSnapshotTaker != null) {
      List<LookupBean> lookups = new ArrayList<>(lookupMap.size());
      for (Map.Entry<String, LookupExtractorFactoryContainer> e : lookupMap.entrySet()) {
        lookups.add(new LookupBean(e.getKey(), null, e.getValue()));
      }

      lookupSnapshotTaker.takeSnapshot(lookups);
    }
  }

  private void loadSnapshot()
  {
    if (lookupSnapshotTaker != null) {
      final List<LookupBean> lookupBeanList = lookupSnapshotTaker.pullExistingSnapshot();
      for (LookupBean lookupBean : lookupBeanList) {
        LookupExtractorFactoryContainer container = lookupBean.getContainer();

        if (container.getLookupExtractorFactory().start()) {
          lookupMap.put(lookupBean.getName(), container);
        } else {
          throw new ISE("Failed to start lookup [%s]:[%s]", lookupBean.getName(), container);
        }
      }
    }
  }

  @VisibleForTesting
  interface Notice
  {
    void handle();
  }

  private class LoadNotice implements Notice
  {
    String lookupName;
    LookupExtractorFactoryContainer lookupExtractorFactoryContainer;

    public LoadNotice(String lookupName, LookupExtractorFactoryContainer lookupExtractorFactoryContainer)
    {
      this.lookupName = lookupName;
      this.lookupExtractorFactoryContainer = lookupExtractorFactoryContainer;
    }

    @Override
    public void handle()
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

      if (!lookupExtractorFactoryContainer.getLookupExtractorFactory().start()) {
        throw new ISE("start method returned false for lookup [%s]:[%s]", lookupName, lookupExtractorFactoryContainer);
      }

      old = lookupMap.put(lookupName, lookupExtractorFactoryContainer);

      LOG.debug("Loaded lookup [%s] with spec [%s].", lookupName, lookupExtractorFactoryContainer);

      takeSnapshot();

      if (old != null) {
        if (!old.getLookupExtractorFactory().close()) {
          throw new ISE("close method returned false for lookup [%s]:[%s]", lookupName, old);
        }
      }
    }
  }

  private class DropNotice implements Notice
  {
    String lookupName;

    public DropNotice(String lookupName)
    {
      this.lookupName = lookupName;
    }

    @Override
    public void handle()
    {
      final LookupExtractorFactoryContainer lookupExtractorFactoryContainer = lookupMap.remove(lookupName);

      if (lookupExtractorFactoryContainer != null) {
        takeSnapshot();

        LOG.debug("Removed lookup [%s] with spec [%s].", lookupName, lookupExtractorFactoryContainer);

        if (!lookupExtractorFactoryContainer.getLookupExtractorFactory().close()) {
          throw new ISE("close method returned false for lookup [%s]:[%s]", lookupName, lookupExtractorFactoryContainer);
        }
      }
    }
  }
}
