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

import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScalerConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.indexing.MoreResources;
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
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler.OPTIMAL_TASK_COUNT_METRIC;

/**
 * Integration test for {@link CostBasedAutoScaler}.
 * <p>
 * Tests the autoscaler's ability to compute optimal task counts based on partition count and cost metrics (lag and idle time).
 */
public class CostBasedAutoScalerIntegrationTest extends EmbeddedClusterTestBase
{
  private static final String TOPIC = EmbeddedClusterApis.createTestDatasourceName();
  private static final String EVENT_TEMPLATE = "{\"timestamp\":\"%s\",\"dimension\":\"value%d\",\"metric\":%d}";
  private static final int PARTITION_COUNT = 50;

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
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
            .addProperty("druid.manager.segments.useIncrementalCache", "ifSynced")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "ifSynced");

    cluster.useLatchableEmitter()
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(historical)
           .addExtension(KafkaIndexTaskModule.class)
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.5s")
           .addResource(kafkaServer)
           .addServer(new EmbeddedRouter());

    return cluster;
  }

  @Test
  @Timeout(45)
  public void test_autoScaler_computesOptimalTaskCountAndProduceScaleDown()
  {
    final String superId = dataSource + "_super";
    final int initialTaskCount = 10;

    // Produce some amount of data to Kafka, to trigger a 'scale down' decision to 4 tasks.

    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .enableTaskAutoScaler(true)
        .taskCountMin(1)
        .taskCountMax(100)
        .taskCountStart(initialTaskCount)
        .scaleActionPeriodMillis(1500)
        .minTriggerScaleActionFrequencyMillis(3000)
        // Weight configuration: strongly favor lag reduction over idle time
        .lagWeight(0.9)
        .idleWeight(0.1)
        .build();

    final KafkaSupervisorSpec spec = createKafkaSupervisorWithAutoScaler(superId, autoScalerConfig, initialTaskCount);

    // Submit the supervisor
    Assertions.assertEquals(superId, cluster.callApi().postSupervisor(spec));

    // Wait for the supervisor to be healthy and running
    overlord.latchableEmitter().waitForEvent(event -> event.hasMetricName("task/run/time"));

    // Wait for autoscaler to emit optimalTaskCount metric indicating scale-down
    // We expect the optimal task count to 4
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(OPTIMAL_TASK_COUNT_METRIC)
                      .hasValueMatching(Matchers.equalTo(6L))
    );

    // Suspend the supervisor
    cluster.callApi().postSupervisor(spec.createSuspendedSpec());
  }

  @Test
  public void test_autoScaler_computesOptimalTaskCountAndProducesScaleUp()
  {
    final String superId = dataSource + "_super_scaleup";

    // Start with a low task count (1 task for 50 partitions) and produce a large amount of data
    // to create lag pressure and low idle ratio, which should trigger a scale-up decision.
    // With the ideal idle range [0.2, 0.6], a single overloaded task will have idle < 0.2,
    // triggering the cost function to recommend more tasks.
    final int lowInitialTaskCount = 1;

    // Produce additional records to create a backlog / lag
    // This ensures tasks are busy processing (low idle ratio)
    Executors.newSingleThreadExecutor().submit(() -> produceRecordsToKafka(500_000, 20));

    // These values were carefully handpicked to allow that test to pass in a stable manner.
    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .enableTaskAutoScaler(true)
        .taskCountMin(1)
        .taskCountMax(50)
        .taskCountStart(lowInitialTaskCount)
        .scaleActionPeriodMillis(500)
        .minTriggerScaleActionFrequencyMillis(1000)
        // Weight configuration: favor lag as the primary signal for scale-up scenarios
        // High lag with low idle (overloaded) will trigger scale-up
        .lagWeight(0.2)
        .idleWeight(0.8)
        .build();

    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisorWithAutoScaler(
        superId,
        autoScalerConfig,
        lowInitialTaskCount
    );

    // Submit the supervisor
    Assertions.assertEquals(superId, cluster.callApi().postSupervisor(kafkaSupervisorSpec));

    // Wait for the supervisor to be healthy and running
    overlord.latchableEmitter().waitForEvent(event -> event.hasMetricName("task/run/time"));

    // First, wait for any optimalTaskCount metric to verify the autoscaler is running
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(OPTIMAL_TASK_COUNT_METRIC)
    );

    // Wait for autoscaler to emit optimalTaskCount metric indicating scale-up
    // We expect the optimal task count to be greater than the initial 1 task
    // With 50 partitions and high lag creating a low idle ratio (< 0.2),
    // the cost function should recommend scaling up to at least 2 tasks.
    // We actually expect 3.
    overlord.latchableEmitter().waitForEvent(
        e -> {
          EventMap eventMap = e.toMap();
          Object metricCandidate = e.toMap().get("metric");
          if (!(metricCandidate instanceof String)) {
            return false;
          }

          String metric = (String) metricCandidate;
          if (!OPTIMAL_TASK_COUNT_METRIC.equals(metric)) {
            return false;
          }

          return ((Long) eventMap.get("value")) > 1L;
        }, 30_000
    );

    // Suspend the supervisor
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
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
                .withTaskDuration(Seconds.THREE.toPeriod())
                .withAutoScalerConfig(autoScalerConfig)
        )
        .withId(supervisorId)
        .build(dataSource, TOPIC);
  }
}
