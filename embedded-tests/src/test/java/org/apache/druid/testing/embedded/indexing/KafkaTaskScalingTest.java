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
import org.apache.druid.indexing.kafka.supervisor.LagBasedAutoScalerConfigBuilder;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Embedded test to verify task scaling behaviour of {@code KafkaSupervisor} ingesting from a custom kafka topic.
 */
@SuppressWarnings("resource")
public class KafkaTaskScalingTest extends EmbeddedClusterTestBase
{
  private static final String TOPIC = EmbeddedClusterApis.createTestDatasourceName();

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
        createTopicWithPartitions(TOPIC, 10);
      }

      @Override
      public void stop()
      {
        deleteTopic(TOPIC);
        super.stop();
      }
    };

    indexer.addProperty("druid.worker.capacity", "10");
    overlord.addProperty("druid.server.http.numThreads", "50");
    cluster.addExtension(KafkaIndexTaskModule.class)
           .addResource(kafkaServer)
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(historical)
           .addServer(new EmbeddedRouter())
           .useLatchableEmitter();

    return cluster;
  }

  @Test
  @Timeout(10)
  public void test_supervisorTasksFinish_withNoDataAndShortTaskDuration()
  {
    final int taskCount = 3;

    final String supervisorId = dataSource + "_short_tasks";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createSupervisorSpec(supervisorId, taskCount, null, false);

    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );

    try {
      Thread.sleep(2000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      Assertions.fail("Test interrupted");
    }

    final String successTaskCountResult = cluster.runSql(
        "SELECT COUNT(*) FROM sys.tasks WHERE datasource = '%s' AND status = 'SUCCESS'",
        dataSource
    );
    final int successfulTasks = Integer.parseInt(successTaskCountResult);

    Assertions.assertEquals(
        taskCount,
        successfulTasks,
        String.format("Expected all %d tasks to succeed, but only %d succeeded", taskCount, successfulTasks)
    );

    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  @Test
  @Timeout(20)
  public void test_supervisorTasksDontFinish_withPersistentTasks()
  {
    final int taskCount = 3;

    final String supervisorId = dataSource + "_persistent_short_tasks";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createSupervisorSpec(supervisorId, taskCount, null, true);

    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );

    try {
      Thread.sleep(10000);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      Assertions.fail("Test interrupted");
    }

    final String runningTaskCountResult = cluster.runSql(
        "SELECT COUNT(*) FROM sys.tasks WHERE datasource = '%s' AND status = 'RUNNING'",
        dataSource
    );
    final int runningTasks = Integer.parseInt(runningTaskCountResult);

    Assertions.assertEquals(
        taskCount,
        runningTasks,
        String.format("Expected all %d tasks to be running, but only %d were found running", taskCount, runningTasks)
    );

    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  @Test
  @Timeout(20)
  public void test_supervisorTasksScalesIn_withPersistentTasksAndAutoScaler()
  {
    final int initialTaskCount = 3;
    final int taskCountMin = 1;

    AutoScalerConfig autoScalerConfig = new LagBasedAutoScalerConfigBuilder()
        .withLagCollectionIntervalMillis(500)
        .withLagCollectionRangeMillis(1000)
        .withEnableTaskAutoScaler(true)
        .withScaleActionPeriodMillis(5000)
        .withScaleActionStartDelayMillis(5000)
        .withScaleOutThreshold(10000)
        .withScaleInThreshold(1)
        .withTaskCountMin(taskCountMin)
        .withTriggerScaleOutFractionThreshold(0.9)
        .withTriggerScaleInFractionThreshold(0.001)
        .withTaskCountMax(initialTaskCount)
        .withTaskCountStart(initialTaskCount)
        .withScaleOutStep(0)
        .withScaleInStep(1)
        .withMinTriggerScaleActionFrequencyMillis(1000)
        .withStopTaskCountRatio(1.0)
        .build();

    String supervisorId = dataSource + "_persistent_autoscale_tasks";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createSupervisorSpec(
        supervisorId,
        initialTaskCount,
        autoScalerConfig,
        true
    );

    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );

    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/autoScaler/scaleActionTime"),
        agg -> agg.hasCountAtLeast(2)
    );

    try {
      Thread.sleep(2000); // Wait for a few seconds for the tasks to scale in
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      Assertions.fail("Test interrupted");
    }

    final String runningTaskCountResult = cluster.runSql(
        "SELECT COUNT(*) FROM sys.tasks WHERE datasource = '%s' AND status = 'RUNNING'",
        dataSource
    );

    final int runningTasks = Integer.parseInt(runningTaskCountResult);

    Assertions.assertEquals(
        taskCountMin,
        runningTasks,
        String.format("Expected all %d tasks to be running, but only %d were found running", taskCountMin, runningTasks)
    );
  }

  private KafkaSupervisorSpec createSupervisorSpec(
      String supervisorId,
      int taskCount,
      AutoScalerConfig autoScalerConfig,
      boolean usePersistentTasks
  )
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null)))
        .withTuningConfig(tuningConfig -> tuningConfig
            .withMaxRowsPerSegment(1000)
            .withWorkerThreads(10))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskCount(taskCount)
                .withTaskDuration(Period.millis(500))
                .withAutoScalerConfig(autoScalerConfig)
        )
        .withId(supervisorId)
        .withUsePersistentTasks(usePersistentTasks)
        .build(dataSource, TOPIC);
  }
}
