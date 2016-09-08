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
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Json;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
  private final ConcurrentMap<String, LookupExtractorFactory> lookupMap = new ConcurrentHashMap();
  private final Object lock = new Object();
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
    synchronized (lock) {
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
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
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
    synchronized (lock) {
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
        if(lookupSnapshotTaker != null) {
          lookupSnapshotTaker.takeSnapshot(getAllAsList());
        }
      } else {
        if (!lookupExtractorFactory.close()) {
          throw new ISE("Error closing [%s] on race condition", lookupName);
        }
      }
      return noPrior;
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
    synchronized (lock) {
      assertStarted();
      for (Map.Entry<String, LookupExtractorFactory> entry : lookups.entrySet()) {
        final String lookupName = entry.getKey();
        final LookupExtractorFactory lookupExtractorFactory = entry.getValue();
        if (lookupMap.containsKey(lookupName)) {
          LOGGER.warn("lookup [%s] is not add, another lookup with the same name already exist", lookupName);
          continue;
        }
        if (!lookupExtractorFactory.start()) {
          failedExtractorFactoryMap.put(lookupName, lookupExtractorFactory);
          continue;
        }
        lookupMap.put(lookupName, lookupExtractorFactory);
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
    final boolean update;
    synchronized (lock) {
      assertStarted();
      final LookupExtractorFactory prior = lookupMap.get(lookupName);
      update = lookupExtractorFactory.replaces(prior);
      if (update) {
        if (!lookupExtractorFactory.start()) {
          throw new ISE("Could not start [%s]", lookupName);
        }
        lookupMap.put(lookupName, lookupExtractorFactory);
        if (prior != null) {
          if (!prior.close()) {
            LOGGER.error("Error closing [%s]:[%s]", lookupName, lookupExtractorFactory);
          }
        }
      }
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
    synchronized (lock) {
      final LookupExtractorFactory lookupExtractorFactory = lookupMap.remove(lookupName);
      if (lookupExtractorFactory != null) {
        LOGGER.debug("Removed lookup [%s]", lookupName);
        if (lookupSnapshotTaker != null) {
          lookupSnapshotTaker.takeSnapshot(getAllAsList());
        }
        return lookupExtractorFactory.close();
      }
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
    final LookupExtractorFactory lookupExtractorFactory = lookupMap.get(lookupName);
    assertStarted();
    return lookupExtractorFactory;
  }

  /**
   * @return Returns {@link Map} containing a copy of the current state.
   *
   * @throws ISE if the is is closed or did not start yet.
   */
  public Map<String, LookupExtractorFactory> getAll()
  {
    assertStarted();
    return Maps.newHashMap(lookupMap);
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
