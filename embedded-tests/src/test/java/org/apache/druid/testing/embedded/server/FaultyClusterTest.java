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
import org.apache.druid.guice.ClusterTestingModule;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorReportPayload;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.testing.cluster.overlord.FaultyLagAggregator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.indexing.KafkaTestBase;
import org.hamcrest.Matchers;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;

/**
 * Integration test to verify induction of various faults in the cluster
 * using {@code ClusterTestingTaskConfig}.
 * <p>
 * Future tests can try to leverage the cluster testing config to verify cluster
 * scalability and stability.
 */
public class FaultyClusterTest extends KafkaTestBase
{
  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return super
        .createCluster()
        .addExtension(ClusterTestingModule.class)
        .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
        .addCommonProperty("druid.unsafe.cluster.testing", "true");
  }

  @Test
  public void test_overlord_skipsCleanupOfPendingSegments()
  {
    final Map<String, Object> taskContext = Map.of(
        "clusterTesting",
        Map.of("metadataConfig", Map.of("cleanupPendingSegments", false))
    );

    // Set up the topic and supervisor
    kafkaServer.createTopicWithPartitions(topic, 1);
    supervisorSpec = createSupervisor()
        .withIoConfig(io -> io.withTaskCount(1))
        .withContext(taskContext)
        .withId("supe_" + dataSource)
        .build(dataSource, topic);
    cluster.callApi().postSupervisor(supervisorSpec);

    publishRecords(topic, true);
    verifyDataAndTearDown(true);

    // Verify that pending segments are not cleaned up
    final List<PendingSegmentRecord> pendingSegments = overlord
        .bindings()
        .segmentsMetadataStorage()
        .getPendingSegments(dataSource, Intervals.ETERNITY);
    Assertions.assertFalse(pendingSegments.isEmpty());
  }

  @Test
  @Timeout(60)
  public void test_supervisor_reportsMagnifiedLag()
  {
    // Set up the topic and supervisor
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int lagMultiplier = 1_000_000;
    supervisorSpec = createSupervisor()
        .withIoConfig(
            io -> io
                .withTaskCount(2)
                .withLagAggregator(new FaultyLagAggregator(lagMultiplier))
        )
        .withContext(
            // Delay segment allocation so that tasks are not able to ingest anything
            Map.of(
                "clusterTesting",
                Map.of("taskActionClientConfig", Map.of("segmentAllocateDelay", "P1D"))
            )
        )
        .withTuningConfig(t -> t.withOffsetFetchPeriod(Period.millis(10)))
        .withId("supe_" + dataSource)
        .build(dataSource, topic);

    cluster.callApi().postSupervisor(supervisorSpec);

    // Publish records to build up some lag
    publishRecords(topic, true);

    // Wait for supervisor to report the expected lag
    final long expectedLag = (long) totalPublishedRecords * lagMultiplier;
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("ingest/kafka/lag")
                      .hasDimension(DruidMetrics.SUPERVISOR_ID, supervisorSpec.getId())
                      .hasValueMatching(Matchers.greaterThanOrEqualTo(expectedLag))
    );

    final String path = StringUtils.format("/druid/indexer/v1/supervisor/%s/status", supervisorSpec.getId());
    final SupervisorReport<KafkaSupervisorReportPayload> report = cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, path),
        new TypeReference<>() {}
    );

    Assertions.assertNotNull(report);
    final KafkaSupervisorReportPayload payload = report.getPayload();
    Assertions.assertNotNull(payload);

    Assertions.assertFalse(payload.isSuspended());
    Assertions.assertTrue(payload.getAggregateLag() >= expectedLag);

    // Kill ongoing tasks and suspend the supervisor
    for (TaskStatusPlus task : cluster.callApi().getTasks(dataSource, "running")) {
      cluster.callApi().onLeaderOverlord(o -> o.cancelTask(task.getId()));
    }
    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
  }
}
