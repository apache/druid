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

package org.apache.druid.simulate.indexing;

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.emitter.kafka.KafkaEmitter;
import org.apache.druid.emitter.kafka.KafkaEmitterModule;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.EmbeddedKafkaServer;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.simulate.EmbeddedBroker;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
import org.apache.druid.testing.simulate.EmbeddedDruidServer;
import org.apache.druid.testing.simulate.EmbeddedHistorical;
import org.apache.druid.testing.simulate.EmbeddedIndexer;
import org.apache.druid.testing.simulate.EmbeddedOverlord;
import org.apache.druid.testing.simulate.EmbeddedRouter;
import org.apache.druid.testing.simulate.emitter.LatchableEmitterModule;
import org.apache.druid.testing.simulate.junit5.IndexingSimulationTestBase;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Simulation test to emit cluster metrics using a {@link KafkaEmitter} and then
 * ingest them back into the cluster with a Kafka supervisor.
 */
public class KafkaIngestSelfMetricsSimTest extends IndexingSimulationTestBase
{
  private static final String TOPIC = IndexingSimulationTestBase.createTestDataourceName();

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private EmbeddedKafkaServer kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();

    kafkaServer = new EmbeddedKafkaServer(cluster.getZookeeper(), cluster.getTestFolder(), Map.of())
    {
      @Override
      public void start() throws IOException
      {
        super.start();
        createTopicWithPartitions(TOPIC, 2);
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

    coordinator.addProperty("druid.coordinator.loadqueuepeon.http.batchSize", "100");
    historical.addProperty("druid.segmentCache.numLoadingThreads", "10");
    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(KafkaEmitterModule.class)
           .addExtension(LatchableEmitterModule.class)
           .addCommonProperty("druid.emitter", "composing")
           .addCommonProperty("druid.emitter.composing.emitters", "[\"latching\",\"kafka\"]")
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT1s")
           .addCommonProperty("druid.monitoring.monitors", "[\"org.apache.druid.java.util.metrics.JvmMonitor\"]")
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
  public void test_ingest50kRows_ofSelfClusterMetrics_andVerifyValues()
  {
    final int maxRowsPerSegment = 1000;
    final int expectedSegmentsHandedOff = 50;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(supervisorId, maxRowsPerSegment);

    final Map<String, String> startSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for segments to be handed off
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, List.of(dataSource)),
        agg -> agg.hasSum(expectedSegmentsHandedOff)
    );

    // Check number of segments and rows in the datasource
    final int numSegments = Integer.parseInt(
        runSql("SELECT COUNT(*) FROM sys.segments WHERE datasource = '%s'", dataSource)
    );
    Assertions.assertTrue(numSegments >= expectedSegmentsHandedOff);

    final int numRows = Integer.parseInt(
        runSql("SELECT COUNT(*) FROM %s", dataSource)
    );
    Assertions.assertTrue(numRows >= expectedSegmentsHandedOff * maxRowsPerSegment);

    verifyMetricReportedByServer("segment/assigned/count", coordinator);
    verifyMetricReportedByServer("jvm/mem/used", broker);
    verifyMetricReportedByServer("jvm/pool/committed", indexer);
    verifyMetricReportedByServer("jvm/mem/used", overlord);

    // Suspend the supervisor and verify the state
    getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
    Assertions.assertTrue(getSupervisorStatus(supervisorId).isSuspended());
  }

  /**
   * SELECTs the total count of the given metric in the {@link #dataSource} and
   * verifies it against the metrics actually emitted by the server.
   */
  private void verifyMetricReportedByServer(String metricName, EmbeddedDruidServer server)
  {
    // Get the value of the metric from the datasource
    final int expectedValueForSegmentsAssigned = (int) Double.parseDouble(
        runSql(
            "SELECT COUNT(*) FROM %s WHERE metric = '%s' AND host = '%s' AND service = '%s'",
            dataSource, metricName, server.selfNode().getHostAndPort(), server.selfNode().getServiceName()
        )
    );

    // Verify the number of metrics actually emitted from this server
    server.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName(metricName),
        agg -> agg.hasCount(expectedValueForSegmentsAssigned)
    );
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, int maxRowsPerSegment)
  {
    final Period taskDuration = Period.seconds(5);
    final Period completionTimeout = Period.minutes(2);
    final Period startDelay = Period.millis(10);
    final boolean useEarliestOffset = true;

    return new KafkaSupervisorSpec(
        supervisorId,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", "iso", null))
                  .withGranularity(new UniformGranularitySpec(Granularities.HOUR, null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        createTuningConfig(maxRowsPerSegment),
        new KafkaSupervisorIOConfig(
            TOPIC,
            null,
            new JsonInputFormat(null, null, null, null, null),
            null, null,
            taskDuration,
            kafkaServer.consumerProperties(),
            null, null, null,
            startDelay,
            null,
            useEarliestOffset,
            completionTimeout,
            null, null, null, null, null, null, null
        ),
        null, null, null, null, null, null, null, null, null, null, null
    );
  }

  private KafkaSupervisorTuningConfig createTuningConfig(int maxRowsPerSegment)
  {
    return new KafkaSupervisorTuningConfig(
        null,
        null, null, null,
        maxRowsPerSegment,
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );
  }
}
