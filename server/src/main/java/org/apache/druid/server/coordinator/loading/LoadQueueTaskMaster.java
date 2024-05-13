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

package org.apache.druid.server.coordinator.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordinator.config.HttpLoadQueuePeonConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides LoadQueuePeons
 */
public class LoadQueueTaskMaster
{
  private static final Logger log = new Logger(LoadQueueTaskMaster.class);

  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService peonExec;
  private final ExecutorService callbackExec;
  private final HttpLoadQueuePeonConfig config;
  private final HttpClient httpClient;

  @GuardedBy("this")
  private final AtomicBoolean isLeader = new AtomicBoolean(false);

  private final ConcurrentHashMap<String, LoadQueuePeon> loadManagementPeons = new ConcurrentHashMap<>();

  public LoadQueueTaskMaster(
      ObjectMapper jsonMapper,
      ScheduledExecutorService peonExec,
      ExecutorService callbackExec,
      HttpLoadQueuePeonConfig config,
      HttpClient httpClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.peonExec = peonExec;
    this.callbackExec = callbackExec;
    this.config = config;
    this.httpClient = httpClient;
  }

  private LoadQueuePeon createPeon(ImmutableDruidServer server)
  {
    return new HttpLoadQueuePeon(server.getURL(), jsonMapper, httpClient, config, peonExec, callbackExec);
  }

  public Map<String, LoadQueuePeon> getAllPeons()
  {
    return loadManagementPeons;
  }

  public LoadQueuePeon getPeonForServer(ImmutableDruidServer server)
  {
    return loadManagementPeons.get(server.getName());
  }

  /**
   * Creates a peon for each of the given servers, if it doesn't already exist and
   * removes peons for servers not present in the cluster anymore.
   * <p>
   * This method must not run concurrently with {@link #onLeaderStart()} and
   * {@link #onLeaderStop()} so that there are no stray peons if the Coordinator
   * is not leader anymore.
   */
  public synchronized void resetPeonsForNewServers(List<ImmutableDruidServer> currentServers)
  {
    if (!isLeader.get()) {
      return;
    }

    final Set<String> oldServers = Sets.newHashSet(loadManagementPeons.keySet());

    // Start peons for new servers
    for (ImmutableDruidServer server : currentServers) {
      loadManagementPeons.computeIfAbsent(server.getName(), serverName -> {
        LoadQueuePeon loadQueuePeon = createPeon(server);
        loadQueuePeon.start();
        log.debug("Created LoadQueuePeon for server[%s].", server.getName());
        return loadQueuePeon;
      });
    }

    // Remove peons for disappeared servers
    for (ImmutableDruidServer server : currentServers) {
      oldServers.remove(server.getName());
    }
    for (String name : oldServers) {
      log.debug("Removing LoadQueuePeon for disappeared server[%s].", name);
      LoadQueuePeon peon = loadManagementPeons.remove(name);
      peon.stop();
    }
  }

  public synchronized void onLeaderStart()
  {
    isLeader.set(true);
  }

  /**
   * Stops and removes all peons.
   */
  public synchronized void onLeaderStop()
  {
    isLeader.set(false);

    loadManagementPeons.values().forEach(LoadQueuePeon::stop);
    loadManagementPeons.clear();
  }

  public boolean isHttpLoading()
  {
    return true;
  }
}
