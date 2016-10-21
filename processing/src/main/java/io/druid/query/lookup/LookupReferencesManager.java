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
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
  private static final Logger LOGGER = new Logger(LookupReferencesManager.class);
  private final ConcurrentMap<String, LookupExtractorFactory> lookupMap = new ConcurrentHashMap<>();
  // This is a lock against the state of the REFERENCE MANAGER (aka start/stop state), NOT of the lookup itself.
  private final ReadWriteLock startStopLock = new ReentrantReadWriteLock(true);
  private final AtomicBoolean started = new AtomicBoolean(false);

  private final LookupSnapshotTaker lookupSnapshotTaker;

  @Inject
  public LookupReferencesManager(LookupConfig lookupConfig, final @Json ObjectMapper objectMapper)
  {
    if (Strings.isNullOrEmpty(lookupConfig.getSnapshotWorkingDir())) {
      this.lookupSnapshotTaker = null;
    } else {
      this.lookupSnapshotTaker = new LookupSnapshotTaker(objectMapper, lookupConfig.getSnapshotWorkingDir());
    }
  }

  @LifecycleStart
  public void start()
  {
    startStopLock.writeLock().lock();
    try {
      if (!started.getAndSet(true)) {
        if (lookupSnapshotTaker != null) {
          final List<LookupBean> lookupBeanList = lookupSnapshotTaker.pullExistingSnapshot();
          for (LookupBean lookupBean : lookupBeanList) {
            this.put(lookupBean.name, lookupBean.factory);
          }
        }
        LOGGER.info("Started lookup factory references manager");
      }
    }
    finally {
      startStopLock.writeLock().unlock();
    }
  }

  @LifecycleStop
  public void stop()
  {
    startStopLock.writeLock().lock();
    try {
      if (started.getAndSet(false)) {
        if (lookupSnapshotTaker != null) {
          lookupSnapshotTaker.takeSnapshot(getAllAsList());
        }
        LOGGER.info("Stopping lookup factory references manager");
        for (String lookupName : lookupMap.keySet()) {
          lookupMap.remove(lookupName).close();
        }
      }
    }
    finally {
      startStopLock.writeLock().unlock();
    }
  }

  /**
   * @param lookupName             name of the lookupExtractorFactory object
   * @param lookupExtractorFactory {@link LookupExtractorFactory} implementation reference.
   *
   * @return true if the lookup is added otherwise false.
   *
   * @throws IllegalStateException If the manager is closed or if start of lookup returns false.
   */
  public boolean put(String lookupName, final LookupExtractorFactory lookupExtractorFactory)
  {
    try {
      startStopLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      assertStarted();
      if (lookupMap.containsKey(lookupName)) {
        LOGGER.warn("lookup [%s] is not add, another lookup with the same name already exist", lookupName);
        return false;
      }
      if (!lookupExtractorFactory.start()) {
        throw new ISE("start method returned false for lookup [%s]", lookupName);
      }
      final boolean noPrior = null == lookupMap.putIfAbsent(lookupName, lookupExtractorFactory);
      if (noPrior) {
        if (lookupSnapshotTaker != null) {
          lookupSnapshotTaker.takeSnapshot(getAllAsList());
        }
      } else {
        if (!lookupExtractorFactory.close()) {
          throw new ISE("Error closing [%s] on race condition", lookupName);
        }
      }
      return noPrior;
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  /**
   * @param lookups {@link Map<String, LookupExtractorFactory>} containing all the lookup as one batch.
   *
   * @throws IllegalStateException if the manager is closed or if {@link LookupExtractorFactory#start()} returns false
   */
  public void put(Map<String, LookupExtractorFactory> lookups)
  {
    Map<String, LookupExtractorFactory> failedExtractorFactoryMap = new HashMap<>();
    try {
      startStopLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      assertStarted();
      for (Map.Entry<String, LookupExtractorFactory> entry : lookups.entrySet()) {
        final String lookupName = entry.getKey();
        final LookupExtractorFactory lookupExtractorFactory = entry.getValue();
        if (lookupMap.containsKey(lookupName)) {
          // Fail early without bothering to start
          LOGGER.warn("lookup [%s] is not add, another lookup with the same name already exist", lookupName);
          continue;
        }
        if (!lookupExtractorFactory.start()) {
          failedExtractorFactoryMap.put(lookupName, lookupExtractorFactory);
          continue;
        }
        if (null != lookupMap.putIfAbsent(lookupName, lookupExtractorFactory)) {
          // handle race
          LOGGER.warn("lookup [%s] is not add, another lookup with the same name already exist", lookupName);
          if (!lookupExtractorFactory.close()) {
            LOGGER.error("Failed to properly close stale lookup [%s]", lookupExtractorFactory);
          }
          continue;
        }
        if (lookupSnapshotTaker != null) {
          lookupSnapshotTaker.takeSnapshot(getAllAsList());
        }
      }
      if (!failedExtractorFactoryMap.isEmpty()) {
        throw new ISE(
            "was not able to start the following lookup(s) [%s]",
            failedExtractorFactoryMap.keySet().toString()
        );
      }
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  /**
   * Add or update a lookup factory
   *
   * @param lookupName             The name of the lookup
   * @param lookupExtractorFactory The factory of the lookup
   *
   * @return True if the lookup was updated, false otherwise
   *
   * @throws IllegalStateException if start of the factory fails
   */
  public boolean updateIfNew(String lookupName, final LookupExtractorFactory lookupExtractorFactory)
  {
    boolean update = false;
    try {
      startStopLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      assertStarted();
      LookupExtractorFactory prior = lookupMap.get(lookupName);
      update = lookupExtractorFactory.replaces(prior);
      if (update) {
        if (!lookupExtractorFactory.start()) {
          throw new ISE("Could not start [%s]", lookupName);
        }
        boolean racy;
        do {
          if (prior == null) {
            racy = null != lookupMap.putIfAbsent(lookupName, lookupExtractorFactory);
          } else {
            racy = !lookupMap.replace(lookupName, prior, lookupExtractorFactory);
          }

          if (racy) {
            prior = lookupMap.get(lookupName);
            update = lookupExtractorFactory.replaces(prior);
          }
        } while (racy && update);

        if (prior != null && update) {
          if (!prior.close()) {
            LOGGER.error("Error closing [%s]:[%s]", lookupName, prior);
          }
        }

        if (!update) {
          // We started the lookup, failed a race, and now need to cleanup
          if (!lookupExtractorFactory.close()) {
            LOGGER.error("Error closing [%s]:[%s]", lookupExtractorFactory);
          }
        }
      }
    }
    finally {
      startStopLock.readLock().unlock();
    }
    return update;
  }

  /**
   * @param lookupName name of {@link LookupExtractorFactory} to delete from the reference registry.
   *                   this function does call the cleaning method {@link LookupExtractorFactory#close()}
   *
   * @return true only if {@code lookupName} is removed and the lookup correctly stopped
   */
  public boolean remove(String lookupName)
  {
    try {
      startStopLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      final LookupExtractorFactory lookupExtractorFactory = lookupMap.remove(lookupName);
      if (lookupExtractorFactory != null) {
        LOGGER.debug("Removed lookup [%s]", lookupName);
        if (lookupSnapshotTaker != null) {
          lookupSnapshotTaker.takeSnapshot(getAllAsList());
        }
        return lookupExtractorFactory.close();
      }
    }
    finally {
      startStopLock.readLock().unlock();
    }
    return false;
  }

  /**
   * @param lookupName key to fetch the reference of the object {@link LookupExtractor}
   *
   * @return reference of {@link LookupExtractorFactory} that correspond the {@code lookupName} or null if absent
   *
   * @throws IllegalStateException if the {@link LookupReferencesManager} is closed or did not start yet
   */
  @Nullable
  public LookupExtractorFactory get(String lookupName)
  {
    try {
      startStopLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      final LookupExtractorFactory lookupExtractorFactory = lookupMap.get(lookupName);
      assertStarted();
      return lookupExtractorFactory;
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  /**
   * @return Returns {@link Map} containing a copy of the current state.
   *
   * @throws ISE if the is is closed or did not start yet.
   */
  public Map<String, LookupExtractorFactory> getAll()
  {
    try {
      startStopLock.readLock().lockInterruptibly();
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
    try {
      assertStarted();
      return Maps.newHashMap(lookupMap);
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  private void assertStarted() throws ISE
  {
    if (isClosed()) {
      throw new ISE("lookup manager is closed");
    }
  }

  public boolean isClosed()
  {
    return !started.get();
  }

  private List<LookupBean> getAllAsList()
  {
    return Lists.newArrayList(
        Collections2.transform(
            lookupMap.entrySet(),
            new Function<Map.Entry<String, LookupExtractorFactory>, LookupBean>()
            {
              @Nullable
              @Override
              public LookupBean apply(
                  @Nullable
                      Map.Entry<String, LookupExtractorFactory> input
              )
              {
                final LookupBean lookupBean = new LookupBean();
                lookupBean.factory = input.getValue();
                lookupBean.name = input.getKey();
                return lookupBean;
              }
            }
        ));
  }
}
