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

package org.apache.druid.testing.embedded.k8s;

import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.docker.LatestImageDockerTest;
import org.apache.druid.testing.embedded.indexing.IngestionSmokeTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

/**
 * Runs some basic ingestion tests against latest image Druid containers running
 * on a K3s cluster.
 */
public class KubernetesClusterDockerTest extends IngestionSmokeTest implements LatestImageDockerTest
{
  @Override
  protected EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
  {
    final K3sDruidService brokerService = new K3sDruidService(DruidCommand.Server.BROKER)
        .addProperty("druid.sql.planner.metadataRefreshPeriod", "PT1s");

    // Create a K3s cluster with all the required services
    final K3sClusterResource k3sCluster = new K3sClusterResource()
        .usingDruidTestImage()
        .addService(new K3sDruidService(DruidCommand.Server.COORDINATOR))
        .addService(new K3sDruidService(DruidCommand.Server.OVERLORD))
        .addService(new K3sDruidService(DruidCommand.Server.HISTORICAL))
        .addService(new K3sDruidService(DruidCommand.Server.MIDDLE_MANAGER))
        .addService(new K3sDruidService(DruidCommand.Server.ROUTER))
        .addService(brokerService);

    // Add an EmbeddedOverlord and EmbeddedBroker to use their client and mapper bindings.
    overlord.addProperty("druid.plaintextPort", "7090");
    broker.addProperty("druid.plaintextPort", "7082");

    return cluster
        .useContainerFriendlyHostname()
        .useDefaultTimeoutForLatchableEmitter(60)
        .addResource(k3sCluster)
        .addServer(overlord)
        .addServer(broker)
        .addServer(eventCollector)
        .addCommonProperty(
            "druid.extensions.loadList",
            "[\"druid-s3-extensions\", \"druid-kafka-indexing-service\", \"postgresql-metadata-storage\"]"
        );
  }

  @BeforeEach
  public void verifyOverlordLeader()
  {
    // Verify that the EmbeddedOverlord is not leader i.e. the pod Overlord is leader
    Assertions.assertFalse(
        overlord.bindings().overlordLeaderSelector().isLeader()
    );
  }
}
