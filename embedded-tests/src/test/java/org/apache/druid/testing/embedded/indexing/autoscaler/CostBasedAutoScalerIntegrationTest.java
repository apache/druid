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

package org.apache.druid.testing.embedded.indexing.autoscaler;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.embedded.indexing.StreamIndexTestBase;
import org.hamcrest.Matchers;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler.OPTIMAL_TASK_COUNT_METRIC;

/**
 * Integration test for {@link CostBasedAutoScaler}.
 * <p>
 * Tests the autoscaler's ability to compute optimal task counts based on partition count and cost metrics (lag and idle time).
 */
@SuppressWarnings("resource")
public class CostBasedAutoScalerIntegrationTest extends StreamIndexTestBase
{
  private static final int PARTITION_COUNT = 50;

  private String topic;
  private final KafkaResource kafkaServer = new KafkaResource();

  @Override
  protected StreamIngestResource<?> getStreamIngestResource()
  {
    return kafkaServer;
  }

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return super
        .createCluster()
        .useDefaultTimeoutForLatchableEmitter(600);
  }

  @BeforeEach
  public void createTopic()
  {
    topic = dataSource;
    kafkaServer.createTopicWithPartitions(topic, PARTITION_COUNT);
  }

  @Test
  @Timeout(45)
  public void test_autoScaler_computesOptimalTaskCountAndProduceScaleDown()
  {
    final int initialTaskCount = 10;

    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .enableTaskAutoScaler(true)
        .taskCountMin(1)
        .taskCountMax(100)
        .taskCountStart(initialTaskCount)
        .scaleActionPeriodMillis(1900)
        .minTriggerScaleActionFrequencyMillis(2000)
        // Weight configuration: strongly favor lag reduction over idle time
        .lagWeight(0.9)
        .idleWeight(0.1)
        .scaleDownDuringTaskRolloverOnly(false)
        .minScaleDownDelay(Duration.ZERO)
        .build();

    final KafkaSupervisorSpec spec = createKafkaSupervisorWithAutoScaler(autoScalerConfig, initialTaskCount);

    // Submit the supervisor
    Assertions.assertEquals(spec.getId(), cluster.callApi().postSupervisor(spec));

    // Wait for the supervisor to be healthy and running
    overlord.latchableEmitter()
            .waitForEvent(event -> event.hasMetricName("task/run/time")
                                        .hasDimension(DruidMetrics.DATASOURCE, dataSource));

    // Wait for autoscaler to emit optimalTaskCount metric indicating scale-down
    // We expect the optimal task count less than 6
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(OPTIMAL_TASK_COUNT_METRIC)
                      .hasValueMatching(Matchers.lessThanOrEqualTo(6L))
    );

    // Suspend the supervisor
    cluster.callApi().postSupervisor(spec.createSuspendedSpec());
  }

  @Test
  public void test_autoScaler_computesOptimalTaskCountAndProducesScaleUp()
  {
    // Start with a low task count (1 task for 50 partitions) and produce a large amount of data
    // to create lag pressure and low idle ratio, which should trigger a scale-up decision.
    // With the ideal idle range [0.2, 0.6], a single overloaded task will have idle < 0.2,
    // triggering the cost function to recommend more tasks.
    final int lowInitialTaskCount = 1;

    // Produce additional records to create a backlog / lag
    // This ensures tasks are busy processing (low idle ratio)
    Executors.newSingleThreadExecutor().submit(() -> {
      for (int i = 0; i < 500; ++i) {
        publish1kRecords(topic, true);
      }
    });

    // These values were carefully handpicked to allow that test to pass in a stable manner.
    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .enableTaskAutoScaler(true)
        .taskCountMin(1)
        .taskCountMax(50)
        .taskCountStart(lowInitialTaskCount)
        .scaleActionPeriodMillis(500)
        .minTriggerScaleActionFrequencyMillis(1000)
        .lagWeight(0.2)
        .idleWeight(0.8)
        .build();

    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisorWithAutoScaler(
        autoScalerConfig,
        lowInitialTaskCount
    );

    // Submit the supervisor
    Assertions.assertEquals(kafkaSupervisorSpec.getId(), cluster.callApi().postSupervisor(kafkaSupervisorSpec));

    // Wait for the supervisor to be healthy and running
    overlord.latchableEmitter()
            .waitForEvent(event -> event.hasMetricName("task/run/time")
                                        .hasDimension(DruidMetrics.DATASOURCE, dataSource));

    // With 50 partitions and high lag creating a low idle ratio (< 0.2),
    // the cost function must recommend scaling up to at least 2 tasks.
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(OPTIMAL_TASK_COUNT_METRIC)
                      .hasValueMatching(Matchers.greaterThan(1L))
    );

    // Suspend the supervisor
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  @Test
  public void test_autoScaler_scalesUpAndDown_withSlowPublish()
  {
    final String topic = EmbeddedClusterApis.createTestDatasourceName();
    kafkaServer.createTopicWithPartitions(topic, 4);

    // A small value of maxRowsPerSegment ensures that there are a large number
    // of segments to publish, thus slowing down publish actions
    final int maxRowsPerSegment = 100;

    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .taskCountMin(1)
        .taskCountMax(4)
        .lagWeight(1.0)
        .idleWeight(1.0)
        .enableTaskAutoScaler(true)
        .minTriggerScaleActionFrequencyMillis(10L)
        .scaleActionPeriodMillis(10L)
        .minScaleDownDelay(Duration.standardSeconds(1))
        .build();

    // taskDuration of 10s gives enough time to auto-scaler to fetch task metrics
    final SupervisorSpec supervisor = createKafkaSupervisor(kafkaServer)
        .withTuningConfig(t -> t.withMaxRowsPerSegment(maxRowsPerSegment))
        .withIoConfig(
            ioConfig -> ioConfig
                .withTaskCount(1)
                .withTaskDuration(Period.seconds(10))
                .withSupervisorRunPeriod(Period.millis(10))
                .withAutoScalerConfig(autoScalerConfig)
        )
        .build(dataSource, topic);
    cluster.callApi().postSupervisor(supervisor);

    // Ingest a large number of records to trigger a scale-up
    // 10k records = 100 segments to publish * 100 rows per segment
    int totalRecords = 0;
    for (int i = 0; i < 10; ++i) {
      totalRecords += publish1kRecords(topic, false);
    }

    // Wait for tasks to scale up
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/autoScaler/updatedCount")
                      .hasDimension(DruidMetrics.SUPERVISOR_ID, supervisor.getId())
                      .hasValueMatching(Matchers.equalTo(4L))
    );
    Assertions.assertEquals(4, getCurrentTaskCount(supervisor.getId()));
    waitUntilPublishedRecordsAreIngested(totalRecords);

    // Let the tasks work through the lag.
    // Do not publish any more records so that the idleness causes scale-down
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/autoScaler/updatedCount")
                      .hasDimension(DruidMetrics.SUPERVISOR_ID, supervisor.getId())
                      .hasValueMatching(Matchers.equalTo(1L))
    );
    Assertions.assertEquals(1, getCurrentTaskCount(supervisor.getId()));

    cluster.callApi().postSupervisor(supervisor.createSuspendedSpec());
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    Assertions.assertEquals("10000", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));

    final List<TaskStatusPlus> tasks = cluster.callApi().getTasks(dataSource, "complete");
    Assertions.assertFalse(tasks.isEmpty());

    // Ensure that there are no task failures due to auto-scaling
    final String expectedErrorOnShutdown = "Killing task for graceful shutdown";
    final Map<String, String> taskIdToError = new HashMap<>();
    for (TaskStatusPlus task : tasks) {
      if (task.getStatusCode() == TaskState.FAILED && !expectedErrorOnShutdown.equals(task.getErrorMsg())) {
        taskIdToError.put(task.getId(), task.getErrorMsg());
      }
    }
    Assertions.assertTrue(
        taskIdToError.isEmpty(),
        StringUtils.format(
            "[%d / %d] tasks have failed with errors: %s",
            taskIdToError.size(), tasks.size(), taskIdToError
        )
    );
  }

  @Test
  void test_scaleDownDuringTaskRollover()
  {
    final int initialTaskCount = 10;

    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .enableTaskAutoScaler(true)
        .taskCountMin(1)
        .taskCountMax(10)
        .scaleActionPeriodMillis(100)
        .minTriggerScaleActionFrequencyMillis(100)
        // High idle weight ensures scale-down when tasks are mostly idle (little data to process)
        .lagWeight(0.1)
        .idleWeight(0.9)
        .scaleDownDuringTaskRolloverOnly(true)
        // Do not slow scale-downs
        .minScaleDownDelay(Duration.ZERO)
        .build();

    final KafkaSupervisorSpec spec = createKafkaSupervisorWithAutoScaler(autoScalerConfig, initialTaskCount);

    // Submit the supervisor
    Assertions.assertEquals(spec.getId(), cluster.callApi().postSupervisor(spec));

    // Wait for at least one task running for the datasource managed by the supervisor.
    overlord.latchableEmitter().waitForEvent(e -> e.hasMetricName("task/run/time")
                                                   .hasDimension(DruidMetrics.DATASOURCE, dataSource));

    // Wait for autoscaler to emit metric indicating scale-down, it should be just less than the current task count.
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/autoScaler/updatedCount")
                      .hasDimension(DruidMetrics.SUPERVISOR_ID, spec.getId())
                      .hasValueMatching(Matchers.lessThan((long) initialTaskCount))
    );

    // After rollover, verify that the running task count has decreased
    // The autoscaler should have recommended fewer tasks due to high idle time
    final int postRolloverRunningTasks = getCurrentTaskCount(spec.getId());

    Assertions.assertTrue(
        postRolloverRunningTasks < initialTaskCount,
        StringUtils.format(
            "Expected running task count to decrease after rollover. Initial: %d, After rollover: %d",
            initialTaskCount,
            postRolloverRunningTasks
        )
    );

    // Suspend the supervisor to clean up
    cluster.callApi().postSupervisor(spec.createSuspendedSpec());
  }

  private KafkaSupervisorSpec createKafkaSupervisorWithAutoScaler(
      CostBasedAutoScalerConfig autoScalerConfig,
      int taskCount
  )
  {
    return createKafkaSupervisor(kafkaServer)
        .withIoConfig(
            ioConfig -> ioConfig
                .withTaskCount(taskCount)
                .withTaskDuration(Period.seconds(1))
                .withAutoScalerConfig(autoScalerConfig)
        )
        .build(dataSource, topic);
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
