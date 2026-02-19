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
 * Base class for Kubernetes task runner tests. Subclasses configure whether to use
 * SharedInformers for caching.
 */
abstract class BaseKubernetesTaskRunnerDockerTest extends IngestionSmokeTest implements LatestImageDockerTest
{
  protected static final String MANIFEST_TEMPLATE = "manifests/druid-service-with-operator.yaml";

  /**
   * Subclasses override to enable/disable SharedInformer caching.
   */
  protected abstract boolean useSharedInformers();

  @Override
  protected EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
  {
    final K3sDruidService brokerService = new K3sDruidService(DruidCommand.Server.BROKER)
        .addProperty("druid.sql.planner.metadataRefreshPeriod", "PT1s")
        .usingPort(30082);

    final K3sDruidService overlordService = new K3sDruidService(DruidCommand.Server.OVERLORD)
        .addProperty("druid.indexer.runner.type", "k8s")
        .addProperty("druid.indexer.runner.namespace", "druid")
        .addProperty("druid.indexer.runner.capacity", "4")
        .addProperty("druid.indexer.runner.useK8sSharedInformers", String.valueOf(useSharedInformers()))
        .addProperty("druid.indexer.runner.k8sSharedInformerResyncPeriod", "PT1s")
        .usingPort(30090);

    final K3sClusterResource k3sCluster = new K3sClusterWithOperatorResource()
        .usingDruidTestImage()
        .usingDruidManifestTemplate(MANIFEST_TEMPLATE)
        .addService(new K3sDruidService(DruidCommand.Server.COORDINATOR).usingPort(30081))
        .addService(overlordService)
        .addService(new K3sDruidService(DruidCommand.Server.HISTORICAL).usingPort(30083))
        .addService(new K3sDruidService(DruidCommand.Server.ROUTER).usingPort(30088))
        .addService(brokerService);

    // Add an EmbeddedOverlord and EmbeddedBroker to use their client and mapper bindings.
    overlord.addProperty("druid.plaintextPort", "7090");
    broker.addProperty("druid.plaintextPort", "7082");
    
    return cluster
        .useContainerFriendlyHostname()
        .useDefaultTimeoutForLatchableEmitter(120)
        .addResource(k3sCluster)
        .addServer(overlord)
        .addServer(broker)
        .addServer(eventCollector)
        .addCommonProperty("druid.indexer.task.encapsulatedTask", "true")
        .addCommonProperty(
            "druid.extensions.loadList",
            "[\"druid-s3-extensions\", \"druid-kafka-indexing-service\","
            + "\"druid-multi-stage-query\", \"postgresql-metadata-storage\", \"druid-kubernetes-overlord-extensions\"]"
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
