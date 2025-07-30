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

package org.apache.druid.testing;

import java.util.Map;

/**
 * Standard Druid commands.
 */
public enum DruidCommand
{
  /**
   * Command to run a Druid Coordinator which coordinates segment assignments,
   * balancing and centralized schema.
   */
  COORDINATOR(
      "coordinator",
      Map.of(
          "druid.coordinator.startDelay", "PT0.1S",
          "druid.coordinator.period", "PT0.5S",
          "druid.manager.segments.pollDuration", "PT0.1S"
      ),
      8081
  ),

  /**
   * Command to run a Druid Overlord which manages ingestion tasks and publishes
   * segment metadata to Metadata Store.
   */
  OVERLORD(
      "overlord",
      Map.of(
          "druid.indexer.storage.type", "metadata",
          "druid.indexer.queue.startDelay", "PT0S",
          "druid.indexer.queue.restartDelay", "PT0S",
          // Keep a small sync timeout so that Peons and Indexers are not stuck
          // handling a change request when Overlord has already shutdown
          "druid.indexer.runner.syncRequestTimeout", "PT1S"
      ),
      8090
  ),

  /**
   * Command to run a Druid Indexer which is a lightweight ingestion worker
   * that launches ingestion tasks as separate threads in the same JVM.
   * <p>
   * By default, this Indexer has a task capacity of 2.
   */
  INDEXER(
      "indexer",
      Map.of(
          "druid.lookup.enableLookupSyncOnStartup", "false",
          "druid.worker.capacity", "2",
          "druid.processing.buffer.sizeBytes", "50MiB",
          "druid.processing.numMergeBuffers", "2",
          "druid.processing.numThreads", "5"
      ),
      8091
  ),

  /**
   * Command to run a Druid MiddleManager which is an ingestion worker
   * that launches ingestion tasks as child processes.
   * <p>
   * By default, this MiddleManager has a task capacity of 2 running at exposed
   * ports 8100 and 8101.
   */
  MIDDLE_MANAGER(
      "middleManager",
      Map.of(
          "druid.lookup.enableLookupSyncOnStartup", "false",
          "druid.worker.capacity", "2",
          "druid.processing.buffer.sizeBytes", "50MiB",
          "druid.processing.numMergeBuffers", "2",
          "druid.processing.numThreads", "5"
      ),
      8091, 8100, 8101
  ),

  /**
   * Command to run Druid Historical service which hosts segment data and can
   * be queried by a Broker.
   */
  HISTORICAL(
      "historical",
      Map.of(
          "druid.segmentCache.locations", "[{\"path\":\"/opt/druid/var/segment-cache\",\"maxSize\":\"10M\"}]",
          "druid.processing.buffer.sizeBytes", "10MiB",
          "druid.processing.numMergeBuffers", "2",
          "druid.processing.numThreads", "5"
      ),
      8083
  ),

  /**
   * Command to run a Druid Broker which can handle all SQL and JSON queries
   * over HTTP and JDBC.
   */
  BROKER(
      "broker",
      Map.of(
          "druid.lookup.enableLookupSyncOnStartup", "false",
          "druid.processing.buffer.sizeBytes", "50MiB",
          "druid.processing.numMergeBuffers", "2",
          "druid.processing.numThreads", "5"
      ),
      8082
  ),

  /**
   * Command to run a Druid Router which routes queries to different Brokers
   * and serves the Druid Web-Console UI.
   */
  ROUTER(
      "router",
      Map.of("druid.router.managementProxy.enabled", "true"),
      8888
  ),

  /**
   * Command to run a test-only Druid node which collects events emitted by
   * other Druid services using {@code HttpPostEmitter}.
   */
  TEST_EVENT_COLLECTOR(
      "eventCollector",
      Map.of(),
      9301
  );

  private final String name;
  private final Integer[] exposedPorts;
  private final Map<String, String> defaultProperties;

  DruidCommand(String name, Map<String, String> defaultProperties, Integer... exposedPorts)
  {
    this.name = name;
    this.defaultProperties = defaultProperties;
    this.exposedPorts = exposedPorts;
  }

  public String getName()
  {
    return name;
  }

  public Integer[] getExposedPorts()
  {
    return exposedPorts;
  }

  public Map<String, String> getDefaultProperties()
  {
    return defaultProperties;
  }
}
