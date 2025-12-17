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

import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Seconds;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler.OPTIMAL_TASK_COUNT_METRIC;

@SuppressWarnings("SameParameterValue")
public class TaskScaleDownRolloverTest extends EmbeddedClusterTestBase
{
  private static final String TOPIC = EmbeddedClusterApis.createTestDatasourceName();
  private static final String EVENT_TEMPLATE = "{\"timestamp\":\"%s\",\"dimension\":\"value%d\",\"metric\":%d}";
  private static final int PARTITION_COUNT = 50;

  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private KafkaResource kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();

    kafkaServer = new KafkaResource()
    {
      @Override
      public void start()
      {
        super.start();
        createTopicWithPartitions(TOPIC, PARTITION_COUNT);
        produceRecordsToKafka(500, 1);
      }

      @Override
      public void stop()
      {
        deleteTopic(TOPIC);
        super.stop();
      }
    };

    // Increase worker capacity to handle more tasks
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.worker.capacity", "60");

    overlord.addProperty("druid.indexer.task.default.context", "{\"useConcurrentLocks\": true}")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    cluster.useLatchableEmitter()
           .useDefaultTimeoutForLatchableEmitter(20)
           .addServer(new EmbeddedCoordinator())
           .addServer(overlord)
           .addServer(indexer)
           .addServer(new EmbeddedRouter())
           .addServer(new EmbeddedBroker())
           .addServer(new EmbeddedHistorical())
           .addExtension(KafkaIndexTaskModule.class)
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT1s")
           .addResource(kafkaServer);

    return cluster;
  }

  /**
   * Tests that scale down happen during task rollover via checkTaskDuration().
   *
   * <p>Test flow:</p>
   * <ol>
   *   <li>Start supervisor with 20 tasks and 50 partitions, minimal data (500 records)</li>
   *   <li>Wait for initial tasks to start running</li>
   *   <li>Wait for the first task rollover to complete (task duration is 10 seconds)</li>
   *   <li>Verify that after rollover, fewer tasks are running due to cost-based autoscaler (no ingestion at all)</li>
   * </ol>
   *
   * <p>Scale down during rollover is triggered in {@code SeekableStreamSupervisor.checkTaskDuration()}
   * when all task groups have rolled over and the autoscaler recommends a lower task count.</p>
   */
  @SuppressWarnings("resource")
  @Test
  @Timeout(300)
  void test_scaleDownDuringTaskRollover()
  {
    final String superId = dataSource + "_super";
    final int initialTaskCount = 10;

    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .enableTaskAutoScaler(true)
        .taskCountMin(1)
        .taskCountMax(10)
        .taskCountStart(initialTaskCount)
        .scaleActionPeriodMillis(2000)
        .minTriggerScaleActionFrequencyMillis(2000)
        // High idle weight ensures scale-down when tasks are mostly idle (little data to process)
        .lagWeight(0.1)
        .idleWeight(0.9)
        .build();

    final KafkaSupervisorSpec spec = createKafkaSupervisorWithAutoScaler(superId, autoScalerConfig, initialTaskCount);

    // Submit the supervisor
    Assertions.assertEquals(superId, cluster.callApi().postSupervisor(spec));

    // Wait for at least one task running for the datasource managed by the supervisor.
    overlord.latchableEmitter().waitForEvent(e -> e.hasMetricName("task/run/time")
                                                   .hasDimension(DruidMetrics.DATASOURCE, dataSource));

    // Wait for autoscaler to emit metric indicating scale-down, it should be just less than the current task count.
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(OPTIMAL_TASK_COUNT_METRIC)
                      .hasValueMatching(Matchers.lessThan((long) initialTaskCount)));

    // Wait for tasks to complete (first rollover)
    overlord.latchableEmitter().waitForEvent(e -> e.hasMetricName("task/action/success/count"));

    // Wait for the task running for the datasource managed by a supervisor.
    overlord.latchableEmitter().waitForEvent(e -> e.hasMetricName("task/run/time")
                                                   .hasDimension(DruidMetrics.DATASOURCE, dataSource));

    // After rollover, verify that the running task count has decreased
    // The autoscaler should have recommended fewer tasks due to high idle time
    final int postRolloverRunningTasks = cluster.callApi().getTaskCount("running", dataSource);

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
      String supervisorId,
      CostBasedAutoScalerConfig autoScalerConfig,
      int taskCount
  )
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null)))
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(100))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskCount(taskCount)
                .withTaskDuration(Seconds.parseSeconds("PT10S").toPeriod())
                .withAutoScalerConfig(autoScalerConfig)
        )
        .withId(supervisorId)
        .build(dataSource, TOPIC);
  }

  private void produceRecordsToKafka(int recordCount, int iterations)
  {
    int recordCountPerSlice = recordCount / iterations;
    int counter = 0;
    for (int i = 0; i < iterations; i++) {
      DateTime timestamp = DateTime.now(DateTimeZone.UTC);
      List<ProducerRecord<byte[], byte[]>> records = IntStream
          .range(counter, counter + recordCountPerSlice)
          .mapToObj(k -> new ProducerRecord<byte[], byte[]>(
                        TOPIC,
                        k % PARTITION_COUNT,
                        null,
                        StringUtils.format(EVENT_TEMPLATE, timestamp, k, k)
                                   .getBytes(StandardCharsets.UTF_8)
                    )
          )
          .collect(Collectors.toList());

      kafkaServer.produceRecordsToTopic(records);
      try {
        Thread.sleep(100L);
        counter += recordCountPerSlice;
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
