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

package org.apache.druid.indexing;

import com.google.common.collect.ImmutableList;
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
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.simulate.EmbeddedBroker;
import org.apache.druid.testing.simulate.EmbeddedCoordinator;
import org.apache.druid.testing.simulate.EmbeddedDruidCluster;
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
public class KafkaIngestSelfClusterMetricsTest extends IndexingSimulationTestBase
{
  private static final String TOPIC = IndexingSimulationTestBase.createTestDataourceName();

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
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

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addExtension(KafkaEmitterModule.class)
           .addExtension(LatchableEmitterModule.class)
           .addCommonProperty("druid.emitter", "composing")
           .addCommonProperty("druid.emitter.composing.emitters", "[\"latching\",\"kafka\"]")
           .addCommonProperty("druid.monitoring.emissionPeriod", "PT1s")
           .addResource(kafkaServer)
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(broker)
           .addServer(new EmbeddedHistorical())
           .addServer(new EmbeddedRouter());

    return cluster;
  }

  @Test
  public void test_ingest40Segments_with100RowsEach_andVerifyMetricValues()
  {
    final int maxRowsPerSegment = 100;

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(supervisorId, maxRowsPerSegment);

    final Map<String, String> startSupervisorResult = getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for the broker to discover the realtime segments
    broker.emitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    SupervisorStatus supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals(dataSource, supervisorStatus.getDataSource());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());
    Assertions.assertEquals(TOPIC, supervisorStatus.getSource());

    // Wait for some segments to be handed off
    final int expectedSegmentsHandedOff = 40;
    final String handoffMetric = "ingest/handoff/count";
    indexer.emitter().waitForEventAggregate(
        event -> event.hasMetricName(handoffMetric)
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

    // Get the value of the handoff metric for this datasource from the datasource itself
    final double numSegmentsHandedOff = Double.parseDouble(
        runSql(
            "SELECT SUM(\"value\") FROM %s WHERE metric = '%s' AND dataSource = '%s'",
            dataSource, handoffMetric, dataSource
        )
    );

    // The latest emitted data might not be queryable yet, so expect a delta
    Assertions.assertTrue(expectedSegmentsHandedOff - numSegmentsHandedOff < 5);

    // Suspend the supervisor and verify the state
    getResult(
        cluster.leaderOverlord().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
    supervisorStatus = getSupervisorStatus(supervisorId);
    Assertions.assertTrue(supervisorStatus.isSuspended());
  }

  private SupervisorStatus getSupervisorStatus(String supervisorId)
  {
    final List<SupervisorStatus> supervisors = ImmutableList.copyOf(
        getResult(cluster.leaderOverlord().supervisorStatuses())
    );
    for (SupervisorStatus supervisor : supervisors) {
      if (supervisor.getId().equals(supervisorId)) {
        return supervisor;
      }
    }

    Assertions.fail("Could not find supervisor for id " + supervisorId);
    return null;
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, int maxRowsPerSegment)
  {
    final Period taskDuration = Period.minutes(1);
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
