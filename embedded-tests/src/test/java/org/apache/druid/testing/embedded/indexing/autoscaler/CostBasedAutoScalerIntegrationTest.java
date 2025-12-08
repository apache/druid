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
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
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
import org.apache.druid.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor.AUTOSCALER_REQUIRED_TASKS_METRIC;

/**
 * Integration test for {@link org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler}.
 * <p>
 * Tests the autoscaler's ability to compute optimal task counts based
 * on partition count and cost metrics (lag and idle time).
 */
public class CostBasedAutoScalerIntegrationTest extends EmbeddedClusterTestBase
{
  private static final String TOPIC = EmbeddedClusterApis.createTestDatasourceName();
  private static final String EVENT_TEMPLATE = "{\"timestamp\":\"%s\",\"dimension\":\"value%d\",\"metric\":%d}";
  ;
  private static final int PARTITION_COUNT = 100;
  private static final int INITIAL_TASK_COUNT = 25;

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

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(LatchableEmitterModule.class)
           .useDefaultTimeoutForLatchableEmitter(300)
           .addCommonProperty("druid.emitter", "latching")
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addResource(kafkaServer)
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(historical)
           .addServer(new EmbeddedRouter());

    return cluster;
  }

  @Disabled
  @Test
  @Timeout(45)
  public void test_autoScaler_computesOptimalTaskCountAndProduceScaleDown()
  {
    final String supervisorId = dataSource + "_supe";

    // Produce some amount of data to kafka, to trigger a 'scale down' decision to 17 tasks.
    produceRecordsToKafka(50);

    final CostBasedAutoScalerConfig autoScalerConfig = CostBasedAutoScalerConfig
        .builder()
        .enableTaskAutoScaler(true)
        .taskCountMin(1)
        .taskCountMax(100)
        .taskCountStart(INITIAL_TASK_COUNT)
        .metricsCollectionIntervalMillis(3000)
        .metricsCollectionRangeMillis(2000)
        .scaleActionStartDelayMillis(3000)
        .scaleActionPeriodMillis(2000)
        .minTriggerScaleActionFrequencyMillis(3000)
        // Weight configuration: strongly favor lag reduction over idle time
        .lagWeight(0.9)
        .idleWeight(0.1)
        .build();

    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisorWithAutoScaler(
        supervisorId,
        autoScalerConfig
    );

    // Submit the supervisor
    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );

    // Wait for the supervisor to be healthy and running
    waitForSupervisorRunning(supervisorId);

    // Wait for autoscaler to emit optimalTaskCount metric indicating scale-up
    // We expect the optimal task count to be either 34 or 50.
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName(AUTOSCALER_REQUIRED_TASKS_METRIC)
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValueMatching(Matchers.equalTo(17L))
    );

    // Suspend the supervisor
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  private void waitForSupervisorRunning(String supervisorId)
  {
    int maxAttempts = 120;
    int attempt = 0;
    while (attempt < maxAttempts) {
      SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisorId);
      if (status != null && "RUNNING".equals(status.getState()) && status.isHealthy()) {
        return;
      }
      attempt++;
      try {
        Thread.sleep(3000);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    throw new RuntimeException("Supervisor did not reach RUNNING state within timeout");
  }

  private void produceRecordsToKafka(int recordCount)
  {
    DateTime timestamp = DateTime.now(DateTimeZone.UTC);
    List<ProducerRecord<byte[], byte[]>> records = IntStream
        .range(0, recordCount)
        .mapToObj(i -> new ProducerRecord<byte[], byte[]>(
                      TOPIC,
                      i % PARTITION_COUNT,
                      null,
                      StringUtils.format(EVENT_TEMPLATE, timestamp, i, i)
                                 .getBytes(StandardCharsets.UTF_8)
                  )
        )
        .collect(Collectors.toList());

    kafkaServer.produceRecordsToTopic(records);
  }

  private KafkaSupervisorSpec createKafkaSupervisorWithAutoScaler(
      String supervisorId,
      CostBasedAutoScalerConfig autoScalerConfig
  )
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null)))
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1000))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskCount(INITIAL_TASK_COUNT)
                .withAutoScalerConfig(autoScalerConfig)
        )
        .withId(supervisorId)
        .build(dataSource, TOPIC);
  }
}
