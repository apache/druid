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

package org.apache.druid.testing.embedded.server;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.duty.DutyGroupStatus;
import org.apache.druid.server.http.CoordinatorDutyStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

public class CoordinatorPauseTest extends EmbeddedClusterTestBase
{
  private static final Duration COORDINATOR_DUTY_PERIOD = Duration.ofMillis(100);
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.coordinator.period", COORDINATOR_DUTY_PERIOD.toString());
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedBroker broker = new EmbeddedBroker();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addServer(overlord)
        .addServer(coordinator)
        .addServer(new EmbeddedIndexer())
        .addServer(broker)
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedRouter());
  }

  @Test
  public void test_segmentsAreNotLoaded_ifCoordinatorIsPaused() throws Exception
  {
    // Pause coordinator cycles
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(
            CoordinatorDynamicConfig.builder().withPauseCoordination(true).build()
        )
    );
    final DateTime pauseTime = DateTimes.nowUtc();

    // Perform some basic ingestion
    final Task task = MoreResources.Task.BASIC_INDEX
        .get()
        .dataSource(dataSource)
        .withId(IdUtils.getRandomId());
    cluster.callApi().runTask(task, overlord);

    // Verify that no coordinator duties have run even after 10 periods
    Thread.sleep(COORDINATOR_DUTY_PERIOD.toMillis() * 10);
    final CoordinatorDutyStatus status = cluster.callApi().serviceClient().onLeaderCoordinator(
        mapper -> new RequestBuilder(HttpMethod.GET, "/druid/coordinator/v1/duties"),
        new TypeReference<>() {}
    );
    Assertions.assertNotNull(status);

    final Optional<DutyGroupStatus> matchingDutyStatus = status.getDutyGroups().stream().filter(
        group -> group.getName().equals("HistoricalManagementDuties")
    ).findFirst();
    Assertions.assertTrue(matchingDutyStatus.isPresent());

    // Verify that the last run was before the pause and all segments are unavailable
    final DutyGroupStatus historicalDutyStatus = matchingDutyStatus.get();

    Assertions.assertTrue(historicalDutyStatus.getLastRunStart().isBefore(pauseTime));
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM sys.segments WHERE is_available = 0 AND datasource = '%s'",
        dataSource,
        "10"
    );

    // Un-pause the Coordinator
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(
            CoordinatorDynamicConfig.builder().build()
        )
    );

    // Verify that segments are finally loaded on the Historical
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM %s", dataSource, "10");
  }
}
