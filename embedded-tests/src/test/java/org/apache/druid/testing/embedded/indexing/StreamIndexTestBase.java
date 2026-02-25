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

import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpecBuilder;
import org.apache.druid.indexing.kinesis.KinesisRegion;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorIOConfig;
import org.apache.druid.indexing.kinesis.supervisor.KinesisSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.kinesis.KinesisResource;
import org.apache.druid.testing.tools.EventSerializer;
import org.apache.druid.testing.tools.JsonEventSerializer;
import org.apache.druid.testing.tools.StreamGenerator;
import org.apache.druid.testing.tools.WikipediaStreamEventStreamGenerator;
import org.joda.time.Period;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;

public abstract class StreamIndexTestBase extends EmbeddedClusterTestBase
{
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();
  protected final EmbeddedIndexer indexer = new EmbeddedIndexer();
  protected final EmbeddedBroker broker = new EmbeddedBroker();
  protected final EmbeddedHistorical historical = new EmbeddedHistorical();
  protected final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();

  protected abstract StreamIngestResource<?> getStreamIngestResource();

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .useContainerFriendlyHostname()
        .useLatchableEmitter()
        .useDefaultTimeoutForLatchableEmitter(60)
        .addResource(getStreamIngestResource())
        .addServer(indexer)
        .addServer(coordinator)
        .addServer(overlord)
        .addServer(broker)
        .addServer(historical)
        .addServer(new EmbeddedRouter());
  }

  protected KafkaSupervisorSpecBuilder createKafkaSupervisor(KafkaResource kafkaServer)
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", "iso", null)))
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskCount(2)
        );
  }

  protected KinesisSupervisorSpec createKinesisSupervisor(KinesisResource kinesis, String dataSource, String topic)
  {
    return new KinesisSupervisorSpec(
        dataSource,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", null, null))
                  .withGranularity(new UniformGranularitySpec(Granularities.HOUR, null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        null,
        new KinesisSupervisorIOConfig(
            topic,
            new JsonInputFormat(null, null, null, null, null),
            kinesis.getEndpoint(),
            KinesisRegion.fromString(kinesis.getRegion()),
            1,
            1,
            Period.millis(500),
            Period.millis(10),
            Period.millis(10),
            true,
            Period.seconds(5),
            null, null, null, null, null, null, null, null,
            false
        ),
        Map.of(),
        false,
        null, null, null, null, null, null, null, null, null, null
    );
  }

  /**
   * Waits until number of processed events matches {@code expectedRowCount}.
   */
  protected void waitUntilPublishedRecordsAreIngested(int expectedRowCount)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expectedRowCount)
    );

    final int totalEventsProcessed = indexer
        .latchableEmitter()
        .getMetricValues("ingest/events/processed", Map.of(DruidMetrics.DATASOURCE, dataSource))
        .stream()
        .mapToInt(Number::intValue)
        .sum();
    Assertions.assertEquals(expectedRowCount, totalEventsProcessed);
  }

  protected void verifySupervisorIsRunningHealthy(String supervisorId)
  {
    final SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertTrue(status.isHealthy());
    Assertions.assertFalse(status.isSuspended());
    Assertions.assertEquals("RUNNING", status.getState());
  }

  /**
   * Verifies that the row count in {@link #dataSource} matches the {@code expectedRowCount}.
   */
  protected void verifyRowCount(int expectedRowCount)
  {
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM %s",
        dataSource,
        String.valueOf(expectedRowCount)
    );
  }

  protected int publish1kRecords(String topic, boolean useTransactions)
  {
    final EventSerializer serializer = new JsonEventSerializer(overlord.bindings().jsonMapper());
    final StreamGenerator streamGenerator = new WikipediaStreamEventStreamGenerator(serializer, 100, 100);
    List<byte[]> records = streamGenerator.generateEvents(10);

    if (useTransactions) {
      getStreamIngestResource().publishRecordsToTopic(topic, records);
    } else {
      getStreamIngestResource().publishRecordsToTopicWithoutTransaction(topic, records);
    }
    return records.size();
  }
}
