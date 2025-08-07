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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.coordinator.CoordinatorServiceClient;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.indexing.SegmentUpdateResponse;
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.DruidContainer;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.IngestionSmokeTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

/**
 * Runs some basic ingestion tests using Coordinator and Overlord at version
 * {@link DruidContainer.Image#APACHE_31} and other services at current version
 * to test backward compatibility.
 * <p>
 * This test does not implement {@link LatestImageDockerTest} and runs in the
 * {@code mvn test} phase.
 */
public class IngestionBackwardCompatibilityDockerTest extends IngestionSmokeTest
{
  @Override
  protected EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
  {
    DruidContainerResource containerOverlord = new DruidContainerResource(DruidCommand.Server.OVERLORD)
        .usingImage(DruidContainer.Image.APACHE_31);
    DruidContainerResource containerCoordinator = new DruidContainerResource(DruidCommand.Server.COORDINATOR)
        .usingImage(DruidContainer.Image.APACHE_31);

    // Add an EmbeddedOverlord to the cluster to use its client and mapper bindings.
    // but ensure that it is not the leader.
    overlord.addProperty("druid.plaintextPort", "7090");

    return cluster
        .useContainerFriendlyHostname()
        .addResource(containerOverlord)
        .addResource(containerCoordinator)
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedRouter())
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

  @Override
  protected int markSegmentsAsUnused(String dataSource)
  {
    // For old Druid versions, use Coordinator to mark segments as unused
    final CoordinatorServiceClient coordinatorClient =
        overlord.bindings().getInstance(CoordinatorServiceClient.class);

    try {
      RequestBuilder req = new RequestBuilder(
          HttpMethod.DELETE,
          StringUtils.format("/druid/coordinator/v1/datasources/%s", dataSource)
      );
      BytesFullResponseHolder responseHolder = coordinatorClient.getServiceClient().request(
          req,
          new BytesFullResponseHandler()
      );

      final ObjectMapper mapper = overlord.bindings().jsonMapper();
      return mapper.readValue(responseHolder.getContent(), SegmentUpdateResponse.class).getNumChangedSegments();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
