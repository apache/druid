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

package org.apache.druid.testing.embedded;

import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.LatchableEmitter;

/**
 * Provides a handle to the various objects used by an {@link EmbeddedDruidServer}
 * during an embedded cluster test. The returned references should be used for
 * read-only purposes and MUST NOT be mutated in any way.
 */
public interface ServerReferencesProvider
{
  /**
   * The hostname for this server.
   */
  DruidNode selfNode();

  /**
   * Client to make API calls to the leader Coordinator in the cluster.
   */
  CoordinatorClient leaderCoordinator();

  /**
   * Leader selector to elect and find the Coordinator leader.
   */
  DruidLeaderSelector coordinatorLeaderSelector();

  /**
   * Client to make API calls to the leader Overlord in the cluster.
   */
  OverlordClient leaderOverlord();

  /**
   * Leader selector to elect and find the Coordinator leader.
   */
  DruidLeaderSelector overlordLeaderSelector();

  /**
   * Client to submit queries to any Broker in the cluster.
   */
  BrokerClient anyBroker();

  /**
   * {@link LatchableEmitter} used by this server, if bound.
   */
  LatchableEmitter latchableEmitter();

  /**
   * Metadata storage coordinator to query and update segment metadata directly
   * in the metadata store.
   */
  IndexerMetadataStorageCoordinator segmentsMetadataStorage();

  /**
   * Provider for {@code DruidNodeDiscovery} for any node type.
   */
  DruidNodeDiscoveryProvider nodeDiscovery();

  /**
   * {@link HttpClient} used by this server to communicate with other Druid servers.
   */
  HttpClient escalatedHttpClient();
}
