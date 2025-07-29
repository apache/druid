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
import org.apache.druid.rpc.indexing.OverlordClient;

/**
 * Provides a handle to the various service clients being used by an
 * {@link EmbeddedDruidCluster}.
 */
public interface ClusterReferencesProvider
{
  /**
   * Client to make API calls to the leader Coordinator in the cluster.
   */
  CoordinatorClient leaderCoordinator();

  /**
   * Client to make API calls to the leader Overlord in the cluster.
   */
  OverlordClient leaderOverlord();

  /**
   * Client to submit queries to any Broker in the cluster.
   */
  BrokerClient anyBroker();

}
