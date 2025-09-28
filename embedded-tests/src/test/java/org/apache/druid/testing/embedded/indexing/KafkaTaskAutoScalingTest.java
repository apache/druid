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
import org.apache.druid.guice.ClusterTestingModule;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.LagBasedAutoScalerConfigBuilder;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.testing.cluster.overlord.FaultyLagAggregator;
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
public class KafkaTaskAutoScalingTest extends EmbeddedClusterTestBase
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
    overlord.addProperty("druid.server.http.numThreads", "50")
            .addProperty("druid.unsafe.cluster.testing", "true");

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(ClusterTestingModule.class)
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
  @Timeout(50)
  public void test_supervisorTasksScalesOutAndScalesIn_withPersistentTasksAndAutoScaler() throws Exception
  {
    final int initialTaskCount = 1;
    final int taskCountMax = 3;
    final int taskCountMin = 1;

    AutoScalerConfig autoScalerConfig = new LagBasedAutoScalerConfigBuilder()
        .withLagCollectionIntervalMillis(500)
        .withLagCollectionRangeMillis(1000)
        .withEnableTaskAutoScaler(true)
        .withScaleActionPeriodMillis(2000)
        .withScaleActionStartDelayMillis(1000)
        .withScaleOutThreshold(100)
        .withScaleInThreshold(1)
        .withTaskCountMin(taskCountMin)
        .withTaskCountMax(taskCountMax)
        .withTaskCountStart(initialTaskCount)
        .withTriggerScaleOutFractionThreshold(0.001)
        .withTriggerScaleInFractionThreshold(0.01)
        .withScaleOutStep(1)
        .withScaleInStep(1)
        .withMinTriggerScaleActionFrequencyMillis(3000)
        .withStopTaskCountRatio(1.0)
        .build();

    String supervisorId = dataSource + "_scale_out_and_in_to_zero";
    String controllerId = "artificial-lag-controller";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createSupervisorSpecWithControlledLag(
        supervisorId,
        controllerId,
        initialTaskCount,
        autoScalerConfig,
        true
    );

    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );

    FaultyLagAggregator.injectLag(controllerId, 100000L);

    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/autoScaler/scaleActionTime"),
        agg -> agg.hasCountAtLeast(2)
    );

    Thread.sleep(5000);

    String runningTaskCountResult = cluster.runSql(
        "SELECT COUNT(*) FROM sys.tasks WHERE datasource = '%s' AND status = 'RUNNING'",
        dataSource
    );
    int runningTasks = Integer.parseInt(runningTaskCountResult);

    Assertions.assertEquals(
        taskCountMax,
        runningTasks,
        String.format("Expected %d tasks to be running after scale up, but found %d", taskCountMax, runningTasks)
    );

    FaultyLagAggregator.injectLag(controllerId, 0L);

    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/autoScaler/scaleActionTime"),
        agg -> agg.hasCountAtLeast(2)
    );
    Thread.sleep(10000);

    // ensure everything has shutdown.
    runningTaskCountResult = cluster.runSql(
        "SELECT COUNT(*) FROM sys.tasks WHERE datasource = '%s' AND status = 'RUNNING'",
        dataSource
    );
    String successTaskCountResult = cluster.runSql(
        "SELECT COUNT(*) FROM sys.tasks WHERE datasource = '%s' AND status = 'SUCCESS'",
        dataSource
    );
    runningTasks = Integer.parseInt(runningTaskCountResult);
    final int successTasks = Integer.parseInt(successTaskCountResult);

    Assertions.assertEquals(
        taskCountMin,
        runningTasks,
        String.format("Expected %d task to be running after scale down, but found %d", runningTasks, taskCountMin)
    );

    int shutDownTasksExpected = taskCountMax - taskCountMin;

    Assertions.assertTrue(
        successTasks >= shutDownTasksExpected,
        String.format("Expected at least %d task to be successfully completed, but found %d", shutDownTasksExpected, successTasks)
    );

    // Cleanup
    FaultyLagAggregator.clearAllInjectedLag();
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  private KafkaSupervisorSpec createSupervisorSpecWithControlledLag(
      String supervisorId,
      String controllerId,
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
                .withTaskDuration(Period.millis(30000))
                .withAutoScalerConfig(autoScalerConfig)
                .withLagAggregator(new FaultyLagAggregator(1, controllerId))
        )
        .withId(supervisorId)
        .withUsePersistentTasks(usePersistentTasks)
        .build(dataSource, TOPIC);
  }
}
