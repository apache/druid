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

package org.apache.druid.testing.embedded.indexing;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.guice.ClusterTestingModule;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.testing.cluster.overlord.HttpLagAggregator;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.hamcrest.Matchers;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

/**
 * Work in progress.
 */
public class KafkaIndexAutoScalingTest extends StreamIndexTestBase
{
  private final KafkaResource kafkaResource = new KafkaResource();

  @Override
  protected StreamIngestResource<?> getStreamIngestResource()
  {
    return kafkaResource;
  }

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    return super
        .createCluster()
        .useDefaultTimeoutForLatchableEmitter(600)
        .addExtension(ClusterTestingModule.class)
        .addCommonProperty("druid.manager.segments.useIncrementalCache", "always")
        .addCommonProperty("druid.unsafe.cluster.testing", "true");
  }

  @Test
  public void test_supervisorWithAutoScaler_runsTasksSuccessfully_ifPublishIsSlow()
  {
    final String topic = EmbeddedClusterApis.createTestDatasourceName();
    kafkaResource.createTopicWithPartitions(topic, 4);

    final SupervisorSpec supervisor = createKafkaSupervisor(kafkaResource)
        .withTuningConfig(t -> t.withMaxRowsPerSegment(100))
        .withIoConfig(
            ioConfig -> ioConfig
                .withTaskCount(1)
                .withTaskDuration(Period.minutes(2))
                .withAutoScalerConfig(
                    new CostBasedAutoScalerConfig(
                        4,
                        1,
                        true, null, 1000L, null, 1000L, 1.0, 0.0, null, null, Duration.standardSeconds(2), null
                    )
                )
                .withLagAggregator(new HttpLagAggregator(null))
        )
        .build(dataSource, topic);
    cluster.callApi().postSupervisor(supervisor);

    int totalRecords = publish1kRecords(topic, false);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    // Force a scale-up by setting the lag to a high value
    final String updateLagPath = StringUtils.format("/druid-internal/v1/test/supervisor/%s/lag", supervisor.getId());
    cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.POST, updateLagPath).jsonContent(
            mapper,
            Map.of("maxLag", 2_000_000, "totalLag", 4_000_000, "avgLag", 1_000_000)
        ),
        null
    );

    // Wait for tasks to scale up
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/autoScaler/updatedCount")
                      .hasDimension(DruidMetrics.SUPERVISOR_ID, supervisor.getId())
                      .hasValueMatching(Matchers.equalTo(4L))
    );
    // Assertions.assertEquals(4, getCurrentTaskCount(supervisor.getId()));

    // Keep publish blocked
    // Ensure that there are no task failures
    // Do some more ingestion
    // Unblock publish

    totalRecords += publish1kRecords(topic, false);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    // Force a scale-down by setting the lag to a low value
    cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.POST, updateLagPath).jsonContent(
            mapper,
            Map.of("maxLag", 0, "totalLag", 0, "avgLag", 0)
        ),
        null
    );
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/autoScaler/updatedCount")
                      .hasDimension(DruidMetrics.SUPERVISOR_ID, supervisor.getId())
                      .hasValueMatching(Matchers.equalTo(1L))
    );
    // Assertions.assertEquals(1, getCurrentTaskCount(supervisor.getId()));

    totalRecords += publish1kRecords(topic, false);
    waitUntilPublishedRecordsAreIngested(totalRecords);

    cluster.callApi().postSupervisor(supervisor.createSuspendedSpec());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    Assertions.assertEquals("3000", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));

    final List<TaskStatusPlus> tasks = cluster.callApi().getTasks(dataSource, "complete");
    Assertions.assertTrue(tasks.stream().allMatch(task -> TaskState.SUCCESS == task.getStatusCode()));
  }

  private int getCurrentTaskCount(String supervisorId)
  {
    final String getSupervisorPath = StringUtils.format("/druid/indexer/v1/supervisor/%s", supervisorId);
    final KafkaSupervisorSpec supervisorSpec = cluster.callApi().serviceClient().onLeaderOverlord(
        mapper -> new RequestBuilder(HttpMethod.GET, getSupervisorPath),
        new TypeReference<>(){}
    );
    Assertions.assertNotNull(supervisorSpec);
    return supervisorSpec.getSpec().getIOConfig().getTaskCount();
  }
}
