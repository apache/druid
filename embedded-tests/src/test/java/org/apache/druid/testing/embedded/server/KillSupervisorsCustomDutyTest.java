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
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.NoopSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.VersionedSupervisorSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class KillSupervisorsCustomDutyTest extends EmbeddedClusterTestBase
{
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator()
      .addProperty("druid.coordinator.kill.supervisor.on", "false")
      .addProperty("druid.coordinator.dutyGroups", "[\"cleanupMetadata\"]")
      .addProperty("druid.coordinator.cleanupMetadata.duties", "[\"killSupervisors\"]")
      .addProperty("druid.coordinator.cleanupMetadata.duty.killSupervisors.durationToRetain", "PT0M")
      .addProperty("druid.coordinator.cleanupMetadata.period", "PT0.1S");

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useLatchableEmitter()
        .addServer(coordinator)
        .addServer(new EmbeddedOverlord())
        .addServer(new EmbeddedBroker());
  }

  @Test
  public void test_customDuty_removesHistoryOfTerminatedSupervisor()
  {
    // Create a compaction supervisor
    final CompactionSupervisorSpec supervisor = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(dataSource).build(),
        false,
        null
    );
    cluster.callApi().postSupervisor(supervisor);

    // Verify that the history of the supervisor has 1 entry
    final List<VersionedSupervisorSpec> history = getSupervisorHistory(supervisor.getId());
    Assertions.assertEquals(1, history.size());

    final SupervisorSpec supervisorEntry = history.get(0).getSpec();
    Assertions.assertNotNull(supervisorEntry);
    Assertions.assertEquals(List.of(dataSource), supervisorEntry.getDataSources());
    Assertions.assertEquals(supervisor.getId(), supervisorEntry.getId());

    // Terminate the supervisor
    cluster.callApi().onLeaderOverlord(o -> o.terminateSupervisor(supervisor.getId()));

    // Verify that the history now has 2 entries and the latest entry is a tombstone
    final List<VersionedSupervisorSpec> historyAfterTermination = getSupervisorHistory(supervisor.getId());
    Assertions.assertEquals(2, historyAfterTermination.size());
    Assertions.assertInstanceOf(NoopSupervisorSpec.class, historyAfterTermination.get(0).getSpec());

    // Wait until the cleanup metric has been emitted
    coordinator.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("metadata/kill/supervisor/count")
                      .hasValueMatching(Matchers.greaterThanOrEqualTo(1L))
    );

    // Verify that the history now returns 404 Not Found
    MatcherAssert.assertThat(
        Assertions.assertThrows(
            RuntimeException.class,
            () -> getSupervisorHistory(supervisor.getId())
        ),
        ExceptionMatcher.of(RuntimeException.class).expectRootCause(
            ExceptionMatcher.of(HttpResponseException.class)
                            .expectMessageContains("404 Not Found")
                            .expectMessageContains(StringUtils.format("No history for [%s]", supervisor.getId()))
        )
    );
  }

  private List<VersionedSupervisorSpec> getSupervisorHistory(String supervisorId)
  {
    final String url = StringUtils.format(
        "/druid/indexer/v1/supervisor/%s/history",
        StringUtils.urlEncode(supervisorId)
    );
    return cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, url),
        new TypeReference<>() {}
    );
  }
}
