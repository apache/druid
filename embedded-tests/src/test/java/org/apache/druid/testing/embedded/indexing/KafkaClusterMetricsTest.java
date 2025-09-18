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

import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.emitter.kafka.KafkaEmitter;
import org.apache.druid.emitter.kafka.KafkaEmitterModule;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.LagBasedAutoScalerConfigBuilder;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.rpc.UpdateResponse;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.InlineSchemaDataSourceCompactionConfig;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedDruidServer;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Embedded test to emit cluster metrics using a {@link KafkaEmitter} and then
 * ingest them back into the cluster with a {@code KafkaSupervisor}.
 */
@SuppressWarnings("resource")
public class KafkaClusterMetricsTest extends EmbeddedClusterTestBase
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
        cluster.addCommonProperty("druid.emitter.kafka.bootstrap.servers", kafkaServer.getBootstrapServerUrl());
        cluster.addCommonProperty("druid.emitter.kafka.metric.topic", TOPIC);
        cluster.addCommonProperty("druid.emitter.kafka.alert.topic", TOPIC);
      }

      @Override
      public void stop()
      {
        deleteTopic(TOPIC);
        super.stop();
      }
    };

    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
           .addProperty("druid.server.http.numThreads", "30")
           .addProperty("druid.worker.capacity", "10");
    overlord.addProperty("druid.indexer.task.default.context", "{\"useConcurrentLocks\": true}")
            .addProperty("druid.server.http.numThreads", "50")
            .addProperty("druid.manager.segments.useIncrementalCache", "ifSynced")
            .addProperty("druid.manager.segments.pollDuration", "PT0.1s")
            .addProperty("druid.manager.segments.killUnused.enabled", "true")
            .addProperty("druid.manager.segments.killUnused.bufferPeriod", "PT0.1s")
            .addProperty("druid.manager.segments.killUnused.dutyPeriod", "PT1s");
    coordinator.addProperty("druid.manager.segments.useIncrementalCache", "ifSynced");
    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(KafkaEmitterModule.class)
           .addExtension(LatchableEmitterModule.class)
           .addCommonProperty("druid.emitter", "composing")
           .addCommonProperty("druid.emitter.composing.emitters", "[\"latching\",\"kafka\"]")
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT0.1s")
           .addCommonProperty(
               "druid.monitoring.monitors",
               "[\"org.apache.druid.java.util.metrics.JvmMonitor\","
               + "\"org.apache.druid.server.metrics.TaskCountStatsMonitor\"]"
           )
           .addResource(kafkaServer)
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(historical)
           .addServer(new EmbeddedRouter());

    return cluster;
  }

  @Test
  @Timeout(20)
  public void test_ingest10kRows_ofSelfClusterMetrics_andVerifyValues()
  {
    final int maxRowsPerSegment = 1000;
    final int expectedSegmentsHandedOff = 10;

    final int taskCount = 5;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(
        supervisorId,
        taskCount,
        maxRowsPerSegment,
        null
    );

    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );

    // Wait for segments to be handed off
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, List.of(dataSource)),
        agg -> agg.hasSumAtLeast(expectedSegmentsHandedOff)
    );

    // Verify number of segments and total number of rows in the datasource
    final int numSegments = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM sys.segments WHERE datasource = '%s'", dataSource)
    );
    Assertions.assertTrue(numSegments >= expectedSegmentsHandedOff);

    final int numRows = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
    Assertions.assertTrue(numRows >= expectedSegmentsHandedOff * maxRowsPerSegment);

    verifyIngestedMetricCountMatchesEmittedCount("jvm/pool/committed", coordinator);
    verifyIngestedMetricCountMatchesEmittedCount("coordinator/time", coordinator);

    // Suspend the supervisor
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  @Test
  @Timeout(60)
  public void test_ingest50kRows_ofSelfClusterMetricsWithScaleOuts_andVerifyValues()
  {
    final int maxRowsPerSegment = 1000;
    final int expectedSegmentsHandedOff = 50;

    final int taskCount = 1;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    AutoScalerConfig autoScalerConfig = new LagBasedAutoScalerConfigBuilder()
        .withLagCollectionIntervalMillis(100)
        .withLagCollectionRangeMillis(100)
        .withEnableTaskAutoScaler(true)
        .withScaleActionPeriodMillis(5000)
        .withScaleActionStartDelayMillis(10000)
        .withScaleOutThreshold(0)
        .withScaleInThreshold(10000)
        .withTriggerScaleOutFractionThreshold(0.001)
        .withTriggerScaleInFractionThreshold(0.1)
        .withTaskCountMax(3)
        .withTaskCountMin(taskCount)
        .withScaleOutStep(1)
        .withScaleInStep(0)
        .withMinTriggerScaleActionFrequencyMillis(5000)
        .withStopTaskCountRatio(1.0)
        .build();

    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(
        supervisorId,
        taskCount,
        maxRowsPerSegment,
        autoScalerConfig,
        true
    );

    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/autoScaler/scaleActionTime"),
        agg -> agg.hasSumAtLeast(2)
    );

    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count"),
        agg -> agg.hasSumAtLeast(expectedSegmentsHandedOff)
    );

    final int numSegments = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM sys.segments WHERE datasource = '%s'", dataSource)
    );
    Assertions.assertTrue(numSegments >= expectedSegmentsHandedOff);

    final int numRows = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
    Assertions.assertTrue(numRows >= expectedSegmentsHandedOff * maxRowsPerSegment);

    verifyIngestedMetricCountMatchesEmittedCount("jvm/pool/committed", coordinator);
    verifyIngestedMetricCountMatchesEmittedCount("coordinator/time", coordinator);

    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  @Test
  @Timeout(120)
  public void test_ingestClusterMetrics_withConcurrentCompactionSupervisor_andSkipKillOfUnusedSegments()
  {
    final int maxRowsPerSegment = 500;
    final int compactedMaxRowsPerSegment = 5000;

    final int taskCount = 2;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(
        supervisorId,
        taskCount,
        maxRowsPerSegment,
        null
    );
    cluster.callApi().postSupervisor(kafkaSupervisorSpec);

    // Wait for a task to succeed
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/success/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    // Wait for some segments to be published
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/txn/success")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Enable compaction supervisors on the Overlord
    final ClusterCompactionConfig originalCompactionConfig = cluster.callApi().onLeaderOverlord(
        OverlordClient::getClusterCompactionConfig
    );

    final ClusterCompactionConfig updatedCompactionConfig
        = new ClusterCompactionConfig(1.0, 10, null, true, null);
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(updatedCompactionConfig)
    );
    Assertions.assertTrue(updateResponse.isSuccess());

    // Submit a compaction supervisor for this datasource
    final CompactionSupervisorSpec compactionSupervisorSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withMaxRowsPerSegment(compactedMaxRowsPerSegment)
            .withTaskContext(Map.of("useConcurrentLocks", true))
            .build(),
        false,
        null
    );
    cluster.callApi().postSupervisor(compactionSupervisorSpec);

    // Wait until some compaction tasks have finished
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.TASK_TYPE, "compact")
                      .hasDimension(DruidMetrics.TASK_STATUS, "SUCCESS"),
        agg -> agg.hasCountAtLeast(2)
    );

    // Verify that some segments have been upgraded due to Concurrent Append and Replace
    final Set<String> allUsedSegmentsIds = overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.INCLUDING_OVERSHADOWED)
        .stream()
        .map(s -> s.getId().toString())
        .collect(Collectors.toSet());
    final Map<String, String> upgradedFromSegmentIds = overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveUpgradedFromSegmentIds(dataSource, allUsedSegmentsIds);
    Assertions.assertFalse(upgradedFromSegmentIds.isEmpty());

    // Update Coordinator dynamic config to mark segments as unused as soon as they become overshadowed
    final CoordinatorDynamicConfig originalCoordinatorDynamicConfig = cluster.callApi().onLeaderCoordinator(
        CoordinatorClient::getCoordinatorDynamicConfig
    );
    final CoordinatorDynamicConfig updatedCoordinatorDynamicConfig
        = CoordinatorDynamicConfig.builder()
                                  .withMarkSegmentAsUnusedDelayMillis(10L)
                                  .build(originalCoordinatorDynamicConfig);
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(updatedCoordinatorDynamicConfig)
    );

    // Wait for some segments to become unused and be eligible for kill
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/kill/unusedIntervals/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );

    // Verify that the segments are skipped since the interval is still being appended to
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/kill/skippedIntervals/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );

    // Revert the cluster compaction config and coordinator dynamic config
    cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(originalCompactionConfig)
    );
    cluster.callApi().onLeaderCoordinator(
        c -> c.updateCoordinatorDynamicConfig(originalCoordinatorDynamicConfig)
    );

    // Suspend the supervisors
    cluster.callApi().postSupervisor(compactionSupervisorSpec.createSuspendedSpec());
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  @Test
  @Timeout(60)
  public void test_ingest50kRows_ofSelfClusterMetricsWithScaleIns_andVerifyValues()
  {
    final int maxRowsPerSegment = 1000;
    final int expectedSegmentsHandedOff = 50;

    final int initialTaskCount = 3;

    // Submit and start a supervisor with scale-in configuration
    final String supervisorId = dataSource + "_supe";
    AutoScalerConfig autoScalerConfig = new LagBasedAutoScalerConfigBuilder()
        .withLagCollectionIntervalMillis(500)
        .withLagCollectionRangeMillis(1000)
        .withEnableTaskAutoScaler(true)
        .withScaleActionPeriodMillis(10000)
        .withScaleActionStartDelayMillis(5000)
        .withScaleOutThreshold(10000)
        .withScaleInThreshold(1)
        .withTriggerScaleOutFractionThreshold(0.9)
        .withTriggerScaleInFractionThreshold(0.001)
        .withTaskCountMax(initialTaskCount)
        .withTaskCountStart(initialTaskCount)
        .withScaleOutStep(0)
        .withScaleInStep(1)
        .withMinTriggerScaleActionFrequencyMillis(10000)
        .withStopTaskCountRatio(1.0)
        .build();

    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(
        supervisorId,
        initialTaskCount,
        maxRowsPerSegment,
        autoScalerConfig,
        true
    );

    Assertions.assertEquals(
        supervisorId,
        cluster.callApi().postSupervisor(kafkaSupervisorSpec)
    );

    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/autoScaler/scaleActionTime"),
        agg -> agg.hasSumAtLeast(2)
    );

    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count"),
        agg -> agg.hasSumAtLeast(expectedSegmentsHandedOff)
    );

    final int numSegments = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM sys.segments WHERE datasource = '%s'", dataSource)
    );
    Assertions.assertTrue(numSegments >= expectedSegmentsHandedOff);

    final int numRows = Integer.parseInt(
        cluster.runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
    Assertions.assertTrue(numRows >= expectedSegmentsHandedOff * maxRowsPerSegment);

    verifyIngestedMetricCountMatchesEmittedCount("jvm/pool/committed", coordinator);
    verifyIngestedMetricCountMatchesEmittedCount("coordinator/time", coordinator);

    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  @Test
  @Timeout(120)
  public void test_ingestClusterMetrics_compactionSkipsLockedIntervals()
  {
    final int maxRowsPerSegment = 500;
    final int compactedMaxRowsPerSegment = 5000;

    final int taskCount = 2;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(
        supervisorId,
        taskCount,
        maxRowsPerSegment,
        null
    );
    cluster.callApi().postSupervisor(kafkaSupervisorSpec);

    // Wait for some segments to be published
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/txn/success")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Enable compaction supervisors on the Overlord
    final ClusterCompactionConfig originalCompactionConfig = cluster.callApi().onLeaderOverlord(
        OverlordClient::getClusterCompactionConfig
    );

    final ClusterCompactionConfig updatedCompactionConfig
        = new ClusterCompactionConfig(1.0, 10, null, true, null);
    final UpdateResponse updateResponse = cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(updatedCompactionConfig)
    );
    Assertions.assertTrue(updateResponse.isSuccess());

    // Submit a compaction supervisor for this datasource
    final CompactionSupervisorSpec compactionSupervisorSpec = new CompactionSupervisorSpec(
        InlineSchemaDataSourceCompactionConfig
            .builder()
            .forDataSource(dataSource)
            .withSkipOffsetFromLatest(Period.seconds(0))
            .withMaxRowsPerSegment(compactedMaxRowsPerSegment)
            .withTaskContext(Map.of("useConcurrentLocks", false))
            .build(),
        false,
        null
    );
    cluster.callApi().postSupervisor(compactionSupervisorSpec);

    // Wait until some skipped metrics have been emitted
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("interval/skipCompact/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );

    // Revert the cluster compaction config
    cluster.callApi().onLeaderOverlord(
        o -> o.updateClusterCompactionConfig(originalCompactionConfig)
    );

    // Suspend the supervisors
    cluster.callApi().postSupervisor(compactionSupervisorSpec.createSuspendedSpec());
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
  }

  /**
   * SELECTs the total count of the given metric in the {@link #dataSource} and
   * verifies it against the metrics actually emitted by the server.
   */
  private void verifyIngestedMetricCountMatchesEmittedCount(String metricName, EmbeddedDruidServer<?> server)
  {
    // Get the value of the metric from the datasource
    final DruidNode selfNode = server.bindings().selfNode();
    final int expectedValueForSegmentsAssigned = (int) Double.parseDouble(
        cluster.runSql(
            "SELECT COUNT(*) FROM %s WHERE metric = '%s' AND host = '%s' AND service = '%s'",
            dataSource, metricName, selfNode.getHostAndPort(), selfNode.getServiceName()
        )
    );
    Assertions.assertTrue(expectedValueForSegmentsAssigned > 0);

    // Verify the number of metrics actually emitted from this server
    server.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName(metricName),
        agg -> agg.hasCountAtLeast(expectedValueForSegmentsAssigned)
    );
  }

  private KafkaSupervisorSpec createKafkaSupervisor(
      String supervisorId,
      int taskCount,
      int maxRowsPerSegment,
      AutoScalerConfig autoScalerConfig
  )
  {
    return createKafkaSupervisor(supervisorId, taskCount, maxRowsPerSegment, autoScalerConfig, false);
  }

  private KafkaSupervisorSpec createKafkaSupervisor(
      String supervisorId,
      int taskCount,
      int maxRowsPerSegment,
      AutoScalerConfig autoScalerConfig,
      boolean usePerpetuallyRunningTasks
  )
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null)))
        .withTuningConfig(tuningConfig -> tuningConfig
            .withMaxRowsPerSegment(maxRowsPerSegment)
            .withWorkerThreads(10)
            .withReleaseLocksOnHandoff(true))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskCount(taskCount)
                .withAutoScalerConfig(autoScalerConfig)
        )
        .withId(supervisorId)
        .withUsePerpetuallyRunningTasks(usePerpetuallyRunningTasks)
        .build(dataSource, TOPIC);
  }
}
