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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexing.IngestionSmokeTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

/**
 * Runs some basic ingestion tests using {@code DruidContainers} to verify the
 * functionality of latest Druid Docker images. The underlying cluster also uses
 * embedded servers either to provide visibility into the cluster state.
 */
public class IngestionDockerTest extends IngestionSmokeTest
{
  @Override
  protected EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
  {
    DruidContainerResource containerOverlord = new DruidContainerResource(DruidCommand.Server.OVERLORD)
        .usingTestImage();
    DruidContainerResource containerCoordinator = new DruidContainerResource(DruidCommand.Server.COORDINATOR)
        .usingTestImage();
    DruidContainerResource historical = new DruidContainerResource(DruidCommand.Server.HISTORICAL)
        .usingTestImage();
    DruidContainerResource router = new DruidContainerResource(DruidCommand.Server.ROUTER)
        .usingTestImage();
    DruidContainerResource middleManager = new DruidContainerResource(DruidCommand.Server.MIDDLE_MANAGER)
        .usingTestImage()
        .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    // Add an EmbeddedOverlord to the cluster to use its client and mapper bindings.
    // but ensure that it is not the leader.
    overlord.addProperty("druid.plaintextPort", "7090");

    return cluster
        .useDefaultTimeoutForLatchableEmitter(180)
        .useContainerFriendlyHostname()
        .addResource(containerOverlord)
        .addResource(containerCoordinator)
        .addResource(middleManager)
        .addResource(historical)
        .addResource(router)
        .addServer(overlord)
        .addServer(broker)
        .addServer(eventCollector)
        .addCommonProperty(
            "druid.extensions.loadList",
            "[\"druid-s3-extensions\", \"druid-kafka-indexing-service\","
            + "\"druid-multi-stage-query\", \"postgresql-metadata-storage\"]"
        );
  }

  @BeforeEach
  public void verifyOverlordLeader()
  {
    // Verify that the EmbeddedOverlord is not leader i.e. the container Overlord is leader
    Assertions.assertFalse(
        overlord.bindings().overlordLeaderSelector().isLeader()
    );
  }
}
