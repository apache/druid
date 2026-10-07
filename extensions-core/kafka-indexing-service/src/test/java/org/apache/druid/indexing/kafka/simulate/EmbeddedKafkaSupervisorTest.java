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

package org.apache.druid.indexing.kafka.simulate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.supervisor.KafkaHeaderBasedFilterConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpecBuilder;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.indexing.seekablestream.supervisor.IdleConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class EmbeddedKafkaSupervisorTest extends EmbeddedClusterTestBase
{
  private static final String COL_ITEM = "item";
  private static final String COL_TIMESTAMP = "timestamp";

  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private final EmbeddedCoordinator coordinator = new EmbeddedCoordinator();
  private KafkaResource kafkaServer;
  private KafkaSupervisorSpec supervisorSpec;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");
    indexer.addProperty("druid.monitoring.emissionPeriod", "PT0.1s");

    kafkaServer = new KafkaResource();

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addResource(kafkaServer)
           .useLatchableEmitter()
           .addServer(coordinator)
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);

    return cluster;
  }

  @AfterEach
  public void stopSupervisor()
  {
    if (supervisorSpec != null) {
      try {
        cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
        // Suspension stops new tasks, but existing tasks may still be publishing.
        for (final TaskStatusPlus task : getActiveTasks()) {
          cluster.callApi().onLeaderOverlord(o -> o.cancelTask(task.getId()));
        }
        cluster.callApi().waitForResult(this::getActiveTasks, List::isEmpty).go();
      }
      finally {
        supervisorSpec = null;
      }
    }
  }

  @Test
  public void testSupervisorIngestsOnlySelectedPartitions()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 4);
    final KafkaSupervisorSpec spec = newKafkaSupervisor()
        .withIoConfig(io -> io.withPartitionIds(Set.of(1)).withTaskCount(2).withStopTaskCount(1))
        .build(dataSource, topic);
    cluster.callApi().postSupervisor(spec);

    // One record per partition, but only partition 1 is selected.
    kafkaServer.produceRecordsToTopic(partitionRecords(topic, 4));
    waitForSelectedRows(dataSource, 1);

    Assertions.assertEquals("1", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertEquals("0", cluster.runSql("SELECT COUNT(*) FROM %s WHERE item != 'p1'", dataSource));

    // Replacing the supervisor with a wider selection picks up partition 3 but still skips partition 2.
    final KafkaSupervisorSpec expanded = newKafkaSupervisor()
        .withIoConfig(io -> io.withPartitionIds(Set.of(1, 3)).withTaskCount(2).withStopTaskCount(1))
        .build(dataSource, topic);
    cluster.callApi().postSupervisor(expanded);
    waitForSelectedRows(dataSource, 2);

    Assertions.assertEquals("2", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
    Assertions.assertEquals("0", cluster.runSql("SELECT COUNT(*) FROM %s WHERE item = 'p2'", dataSource));
    cluster.callApi().postSupervisor(expanded.createSuspendedSpec());
  }

  private static List<ProducerRecord<byte[], byte[]>> partitionRecords(String topic, int partitions)
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int partition = 0; partition < partitions; partition++) {
      records.add(new ProducerRecord<>(
          topic,
          partition,
          null,
          StringUtils.toUtf8("2025-06-01T00:00:00Z,p" + partition)
      ));
    }
    return records;
  }

  private void waitForSelectedRows(String source, int count)
  {
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed").hasDimension(DruidMetrics.DATASOURCE, source),
        agg -> agg.hasSumAtLeast(count)
    );
  }

  @Test
  public void test_runKafkaSupervisor()
  {
    final String topic = dataSource;
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int expectedSegments = 10;
    kafkaServer.produceRecordsToTopic(
        generateRecordsForTopic(topic, expectedSegments, DateTimes.of("2025-06-01"))
    );

    // Submit and start a supervisor
    final String supervisorId = dataSource + "_supe";
    final KafkaSupervisorSpec kafkaSupervisorSpec
        = newKafkaSupervisor().withId(supervisorId).build(dataSource, topic);

    Assertions.assertEquals(supervisorId, submitSupervisor(kafkaSupervisorSpec));

    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    SupervisorStatus supervisorStatus = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals(dataSource, supervisorStatus.getDataSource());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());
    Assertions.assertEquals(topic, supervisorStatus.getSource());

    // Get the task statuses
    final List<TaskStatusPlus> taskStatuses = getActiveTasks();
    // A publishing task and its replacement can both be active during rollover.
    Assertions.assertFalse(taskStatuses.isEmpty());
    for (final TaskStatusPlus task : taskStatuses) {
      Assertions.assertEquals(TaskState.RUNNING, task.getStatusCode());
    }

    // Wait until all produced records have been ingested before verifying the row count,
    // otherwise the query below can race ingestion and observe fewer than the expected rows
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expectedSegments)
    );

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals(
        String.valueOf(expectedSegments),
        cluster.runSql("SELECT COUNT(*) FROM %s", dataSource)
    );

    // Suspend the supervisor and verify the state
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
    supervisorStatus = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertTrue(supervisorStatus.isSuspended());
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expectedSegments)
    );
    overlord.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("task/action/run/time")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasDimension(DruidMetrics.TASK_ACTION_TYPE, "lockRelease"),
        agg -> agg.hasCountAtLeast(expectedSegments)
    );
    List<LockFilterPolicy> lockFilterPolicies = List.of(new LockFilterPolicy(dataSource, 0, null, null));
    Map<String, List<Interval>> lockedIntervals = cluster.callApi()
                                                         .onLeaderOverlord(client -> client.findLockedIntervals(lockFilterPolicies));
    Assertions.assertEquals(0, lockedIntervals.size());
  }

  @Test
  public void test_runKafkaSupervisorWithHeaderFiltering()
  {
    final String topic = dataSource;
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int totalRecords = 10;
    final int expectedRecords = 4; // Only production records (indices 0, 3, 6, 9)
    kafkaServer.produceRecordsToTopic(
        generateRecordsWithEnvironmentHeaders(topic, totalRecords, DateTimes.of("2025-06-01"))
    );

    // Submit supervisor with header filtering for production environment
    final String supervisorId = dataSource + "_filtered";
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisorWithHeaderFilter(supervisorId, topic);

    // Use submitSupervisor so the @AfterEach teardown suspends the supervisor and cancels any
    // remaining tasks, consistent with the other tests' stabilization pattern.
    Assertions.assertEquals(supervisorId, submitSupervisor(kafkaSupervisorSpec));

    // Wait for the broker to discover the realtime segments
    broker.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    SupervisorStatus supervisorStatus = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());

    // Suspend the supervisor and wait for segment handoff
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(expectedRecords)
    );

    // Verify only filtered records were ingested
    Assertions.assertEquals(String.valueOf(expectedRecords), cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));
  }

  @Test
  public void test_supervisorBecomesIdle_ifTopicHasNoData()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    final long idleAfterMillis = 100L;
    final KafkaSupervisorSpec supervisorSpec = newKafkaSupervisor()
        .withIoConfig(ioConfig -> ioConfig.withIdleConfig(new IdleConfig(true, idleAfterMillis)).withTaskCount(1))
        .build(dataSource, topic);
    submitSupervisor(supervisorSpec);

    // Wait for the first set of tasks to finish
    overlord.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("task/run/time")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

    // Verify that the supervisor is now idle
    final SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisorSpec.getId());
    Assertions.assertFalse(status.isSuspended());
    Assertions.assertTrue(status.isHealthy());
    Assertions.assertEquals("IDLE", status.getState());

    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    kafkaServer.deleteTopic(topic);
  }

  @Test
  public void test_runSupervisor_withEmptyDimension()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    final String emptyColumn = "unknownColumn";
    final KafkaSupervisorSpec supervisorSpec = newKafkaSupervisor()
        // Use the default row limit so this test does not create a segment for each row.
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(null))
        // Allow ingestion to finish before time-based rollover; suspension below triggers publishing.
        .withIoConfig(ioConfig -> ioConfig.withTaskDuration(Period.minutes(1)))
        .withDataSchema(
            s -> s.withDimensions(
                DimensionsSpec.getDefaultSchemas(List.of(emptyColumn, COL_ITEM))
            )
        )
        .build(dataSource, topic);
    submitSupervisor(supervisorSpec);

    final int numRows = 100;
    // One-second timestamp steps keep all 100 rows in one daily segment interval.
    kafkaServer.produceRecordsToTopic(
        generateRecordsForTopic(topic, numRows, DateTimes.of("2025-06-01"), Period.seconds(1))
    );

    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/events/processed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(numRows)
    );

    cluster.callApi().postSupervisor(supervisorSpec.createSuspendedSpec());
    // Processed-row metrics are emitted before publishing; wait for the segment handoff as well.
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(1)
    );
    cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator, broker);

    Assertions.assertEquals(
        "100",
        cluster.runSql("SELECT COUNT(*) FROM %s WHERE %s IS NULL", dataSource, emptyColumn)
    );
    Assertions.assertEquals(
        "0",
        cluster.runSql("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL", dataSource, emptyColumn)
    );
    Assertions.assertEquals(
        StringUtils.format("%s,YES,VARCHAR", emptyColumn),
        cluster.runSql(
            "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE"
            + " FROM INFORMATION_SCHEMA.COLUMNS"
            + " WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'",
            dataSource, emptyColumn
        )
    );
  }

  private KafkaSupervisorSpecBuilder newKafkaSupervisor()
  {
    return new KafkaSupervisorSpecBuilder()
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec(COL_TIMESTAMP, null, null))
                .withDimensions(DimensionsSpec.EMPTY)
        )
        .withTuningConfig(
            tuningConfig -> tuningConfig
                .withMaxRowsPerSegment(1)
                .withReleaseLocksOnHandoff(true)
        )
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(new CsvInputFormat(List.of(COL_TIMESTAMP, COL_ITEM), null, null, false, 0, false))
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withTaskDuration(Period.millis(500))
                .withStartDelay(Period.millis(10))
                .withSupervisorRunPeriod(Period.millis(500))
                .withCompletionTimeout(Period.seconds(5))
                .withUseEarliestSequenceNumber(true)
        );
  }

  private KafkaSupervisorSpec createKafkaSupervisorWithHeaderFilter(String supervisorId, String topic)
  {
    InDimFilter filter = new InDimFilter("environment", ImmutableSet.of("production"));
    KafkaHeaderBasedFilterConfig headerFilterConfig = new KafkaHeaderBasedFilterConfig(filter, "UTF-8", 1000);

    return new KafkaSupervisorSpecBuilder()
        .withDataSchema(
            schema -> schema
                .withTimestamp(new TimestampSpec("timestamp", null, null))
                .withDimensions(DimensionsSpec.EMPTY)
        )
        .withTuningConfig(
            tuningConfig -> tuningConfig
                .withMaxRowsPerSegment(1)
                .withReleaseLocksOnHandoff(true)
        )
        .withIoConfig(
            ioConfig -> ioConfig
                .withInputFormat(new CsvInputFormat(List.of("timestamp", "item"), null, null, false, 0, false))
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withUseEarliestSequenceNumber(true)
                .withHeaderBasedFilterConfig(headerFilterConfig)
        )
        .withId(supervisorId)
        .build(dataSource, topic);
  }

  private String submitSupervisor(final KafkaSupervisorSpec spec)
  {
    supervisorSpec = spec;
    return cluster.callApi().postSupervisor(spec);
  }

  private List<TaskStatusPlus> getActiveTasks()
  {
    return ImmutableList.copyOf(
        (CloseableIterator<TaskStatusPlus>)
            cluster.callApi().onLeaderOverlord(o -> o.taskStatuses(null, dataSource, 0))
    );
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecordsForTopic(
      String topic,
      int numRecords,
      DateTime startTime
  )
  {
    // Daily steps place each row in a separate segment interval for the handoff and lock-release assertions.
    return generateRecordsForTopic(topic, numRecords, startTime, Period.days(1));
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecordsForTopic(
      final String topic,
      final int numRecords,
      final DateTime startTime,
      final Period timestampStep
  )
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      String valueCsv = StringUtils.format(
          "%s,%s,%d",
          startTime.plus(timestampStep.multipliedBy(i)),
          IdUtils.getRandomId(),
          ThreadLocalRandom.current().nextInt(1000)
      );
      records.add(
          new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8(valueCsv))
      );
    }
    return records;
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecordsWithEnvironmentHeaders(String topic, int numRecords, DateTime startTime)
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    final String[] environments = {"production", "staging", "development"};

    for (int i = 0; i < numRecords; ++i) {
      String valueCsv = StringUtils.format("%s,%s", startTime.plusDays(i), IdUtils.getRandomId());
      ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8(valueCsv));
      record.headers().add("environment", environments[i % environments.length].getBytes(StandardCharsets.UTF_8));
      records.add(record);
    }
    return records;
  }
}
