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

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLMetadataStorageModule;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedCoordinator;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedHistorical;
import org.apache.druid.testing.embedded.EmbeddedIndexer;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.testing.embedded.msq.EmbeddedMSQApis;
import org.apache.druid.testing.embedded.psql.PostgreSQLMetadataResource;
import org.apache.druid.testing.embedded.server.EmbeddedEventCollector;
import org.apache.druid.timeline.DataSegment;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Contains a variety of basic ingestion tests.
 */
public class IngestionSmokeTest extends EmbeddedClusterTestBase
{
  protected final EmbeddedOverlord overlord = new EmbeddedOverlord();

  /**
   * Indexer with 2 slots and 200MB each as the minimum required memory for the
   * MSQ tasks in {@link #test_ingestWikipedia1DayWithMSQ_andQueryData()} is 133 MB.
   */
  protected EmbeddedIndexer indexer = new EmbeddedIndexer()
      .setServerMemory(300_000_000)
      .addProperty("druid.worker.capacity", "2")
      .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

  /**
   * Broker with a short metadata refresh period.
   */
  protected EmbeddedBroker broker = new EmbeddedBroker()
      .addProperty("druid.sql.planner.metadataRefreshPeriod", "PT1s");

  /**
   * Event collector used to wait for metric events to occur.
   */
  protected final EmbeddedEventCollector eventCollector = new EmbeddedEventCollector()
      .addProperty("druid.emitter", "latching");

  protected final KafkaResource kafkaServer = new KafkaResource();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return addServers(
        EmbeddedDruidCluster
            .withZookeeper()
            .addExtensions(
                KafkaIndexTaskModule.class,
                LatchableEmitterModule.class,
                PostgreSQLMetadataStorageModule.class
            )
            .addResource(new PostgreSQLMetadataResource())
            .addResource(new MinIOStorageResource())
            .addResource(kafkaServer)
            .addCommonProperty("druid.emitter", "http")
            .addCommonProperty("druid.emitter.http.recipientBaseUrl", eventCollector.getMetricsUrl())
            .addCommonProperty("druid.emitter.http.flushMillis", "500")
    );
  }

  /**
   * Adds servers to the given cluster.
   *
   * @return The updated cluster.
   */
  protected EmbeddedDruidCluster addServers(EmbeddedDruidCluster cluster)
  {
    return cluster
        .addServer(new EmbeddedCoordinator())
        .addServer(overlord)
        .addServer(indexer)
        .addServer(broker)
        .addServer(eventCollector)
        .addServer(new EmbeddedHistorical())
        .addServer(new EmbeddedRouter());
  }

  @AfterEach
  public void cleanUp()
  {
    markSegmentsAsUnused(dataSource);
  }

  protected int markSegmentsAsUnused(String dataSource)
  {
    return cluster.callApi()
                  .onLeaderOverlord(o -> o.markSegmentsAsUnused(dataSource))
                  .getNumChangedSegments();
  }

  @Test
  public void test_runIndexTask_andKillData()
  {
    final int numSegments = 10;

    // Run an 'index' task and verify the ingested data
    final String taskId = IdUtils.getRandomId();
    final IndexTask task = MoreResources.Task.BASIC_INDEX.get().dataSource(dataSource).withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, eventCollector.latchableEmitter());

    verifyUsedSegmentCount(numSegments);
    waitForSegmentsToBeQueryable(numSegments);

    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM sys.segments WHERE datasource='%s'", dataSource, "10");
    cluster.callApi().verifySqlQuery("SELECT * FROM %s", dataSource, Resources.InlineData.CSV_10_DAYS);

    // Mark all segments as unused and verify state
    Assertions.assertEquals(
        numSegments,
        markSegmentsAsUnused(dataSource)
    );
    verifyUsedSegmentCount(0);
    eventCollector.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/loadQueue/success")
                      .hasDimension(DruidMetrics.DESCRIPTION, "DROP")
                      .hasService("druid/coordinator"),
        agg -> agg.hasSumAtLeast(numSegments)
    );

    eventCollector.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/schemaCache/dataSource/removed")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasService("druid/broker")
    );

    cluster.callApi().verifySqlQuery("SELECT * FROM sys.segments WHERE datasource='%s'", dataSource, "");

    // Kill all unused segments
    final String killTaskId = cluster.callApi().onLeaderOverlord(
        o -> o.runKillTask(IdUtils.getRandomId(), dataSource, Intervals.ETERNITY, null, null, null)
    );
    cluster.callApi().waitForTaskToSucceed(killTaskId, eventCollector.latchableEmitter());

    eventCollector.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/nuked/bytes")
                      .hasService("druid/overlord"),
        agg -> agg.hasCountAtLeast(numSegments)
    );
  }

  @Test
  public void test_runIndexParallelTask_andCompactData()
  {
    final int numInitialSegments = 1;

    // Run an 'index_parallel' task and verify the ingested data
    final String taskId = IdUtils.getRandomId();
    final ParallelIndexSupervisorTask task = TaskBuilder
        .ofTypeIndexParallel()
        .timestampColumn("timestamp")
        .jsonInputFormat()
        .inputSource(Resources.HttpData.wikipedia1Day())
        .dimensions()
        .tuningConfig(t -> t.withMaxNumConcurrentSubTasks(1))
        .dataSource(dataSource)
        .withId(taskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    cluster.callApi().waitForTaskToSucceed(taskId, eventCollector.latchableEmitter());

    waitForSegmentsToBeQueryable(numInitialSegments);
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM %s", dataSource, "24433");
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM sys.segments WHERE datasource='%s'", dataSource, "1");

    final String[] segmentIntervalParts = cluster.runSql(
        "SELECT \"start\", \"end\" FROM sys.segments WHERE datasource='%s'",
        dataSource
    ).split(",");
    final Interval segmentInterval = Intervals.of("%s/%s", segmentIntervalParts[0], segmentIntervalParts[1]);

    // Run compaction for this interval
    final String compactTaskId = IdUtils.getRandomId();
    final CompactionTask compactionTask = TaskBuilder
        .ofTypeCompact()
        .dataSource(dataSource)
        .interval(segmentInterval)
        .dynamicPartitionWithMaxRows(5000)
        .withId(compactTaskId);
    cluster.callApi().onLeaderOverlord(o -> o.runTask(compactTaskId, compactionTask));
    cluster.callApi().waitForTaskToSucceed(taskId, eventCollector.latchableEmitter());

    // Verify the compacted data
    final int numCompactedSegments = 5;
    waitForSegmentsToBeQueryable(numInitialSegments + numCompactedSegments);
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM %s", dataSource, "24433");
    cluster.callApi().verifySqlQuery(
        "SELECT COUNT(*) FROM sys.segments WHERE datasource='%s' AND is_overshadowed=0",
        dataSource,
        "5"
    );
  }

  @Test
  public void test_ingestWikipedia1DayWithMSQ_andQueryData()
  {
    final String sql =
        "INSERT INTO %s"
        + " SELECT "
        + "  TIME_PARSE(\"timestamp\") AS __time, *"
        + " FROM TABLE("
        + "  EXTERN("
        + "    '{\"type\":\"http\",\"uris\":[\"https://druid.apache.org/data/wikipedia.json.gz\"]}',\n"
        + "    '{\"type\":\"json\"}',\n"
        + "    '[{\"name\":\"isRobot\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"string\"},{\"name\":\"flags\",\"type\":\"string\"},{\"name\":\"isUnpatrolled\",\"type\":\"string\"},{\"name\":\"page\",\"type\":\"string\"},{\"name\":\"diffUrl\",\"type\":\"string\"},{\"name\":\"added\",\"type\":\"long\"},{\"name\":\"comment\",\"type\":\"string\"},{\"name\":\"commentLength\",\"type\":\"long\"},{\"name\":\"isNew\",\"type\":\"string\"},{\"name\":\"isMinor\",\"type\":\"string\"},{\"name\":\"delta\",\"type\":\"long\"},{\"name\":\"isAnonymous\",\"type\":\"string\"},{\"name\":\"user\",\"type\":\"string\"},{\"name\":\"deltaBucket\",\"type\":\"long\"},{\"name\":\"deleted\",\"type\":\"long\"},{\"name\":\"namespace\",\"type\":\"string\"},{\"name\":\"cityName\",\"type\":\"string\"},{\"name\":\"countryName\",\"type\":\"string\"},{\"name\":\"regionIsoCode\",\"type\":\"string\"},{\"name\":\"metroCode\",\"type\":\"long\"},{\"name\":\"countryIsoCode\",\"type\":\"string\"},{\"name\":\"regionName\",\"type\":\"string\"}]'"
        + "  )"
        + ")"
        + " PARTITIONED BY DAY"
        + " CLUSTERED BY countryName";

    final EmbeddedMSQApis msqApis = new EmbeddedMSQApis(cluster, overlord);
    final SqlTaskStatus taskStatus = msqApis.submitTaskSql(sql, dataSource);
    cluster.callApi().waitForTaskToSucceed(taskStatus.getTaskId(), eventCollector.latchableEmitter());

    waitForSegmentsToBeQueryable(1);
    cluster.callApi().verifySqlQuery("SELECT COUNT(*) FROM %s", dataSource, "24433");
  }

  @Test
  public void test_runKafkaSupervisor()
  {
    final String topic = dataSource;
    kafkaServer.createTopicWithPartitions(topic, 2);

    kafkaServer.produceRecordsToTopic(
        generateRecordsForTopic(topic, 10, DateTimes.of("2025-06-01"))
    );

    // Submit and start a supervisor
    final String supervisorId = dataSource;
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(topic);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    waitForSegmentsToBeQueryable(1);

    SupervisorStatus supervisorStatus = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertFalse(supervisorStatus.isSuspended());
    Assertions.assertTrue(supervisorStatus.isHealthy());
    Assertions.assertEquals("RUNNING", supervisorStatus.getState());
    Assertions.assertEquals(topic, supervisorStatus.getSource());

    // Get the task statuses
    List<TaskStatusPlus> taskStatuses = ImmutableList.copyOf(
        (CloseableIterator<TaskStatusPlus>)
            cluster.callApi().onLeaderOverlord(o -> o.taskStatuses(null, dataSource, 1))
    );
    Assertions.assertFalse(taskStatuses.isEmpty());
    Assertions.assertEquals(TaskState.RUNNING, taskStatuses.get(0).getStatusCode());

    // Suspend the supervisor and verify the state
    cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
    supervisorStatus = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertTrue(supervisorStatus.isSuspended());
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String topic)
  {
    return MoreResources.Supervisor.KAFKA_JSON
        .get()
        .withDataSchema(schema -> schema.withTimestamp(new TimestampSpec("timestamp", null, null)))
        .withIoConfig(
            ioConfig -> ioConfig
                .withConsumerProperties(kafkaServer.consumerProperties())
                .withInputFormat(new CsvInputFormat(List.of("timestamp", "item"), null, null, false, 0, false))
        )
        .withTuningConfig(tuningConfig -> tuningConfig.withMaxRowsPerSegment(1))
        .build(dataSource, topic);
  }

  private List<ProducerRecord<byte[], byte[]>> generateRecordsForTopic(
      String topic,
      int numRecords,
      DateTime startTime
  )
  {
    final List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
    for (int i = 0; i < numRecords; ++i) {
      String valueCsv = StringUtils.format(
          "%s,%s,%d",
          startTime.plusDays(i),
          IdUtils.getRandomId(),
          ThreadLocalRandom.current().nextInt(1000)
      );
      records.add(
          new ProducerRecord<>(topic, 0, null, StringUtils.toUtf8(valueCsv))
      );
    }
    return records;
  }

  private void waitForSegmentsToBeQueryable(int numSegments)
  {
    eventCollector.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("segment/schemaCache/refresh/count")
                      .hasService("druid/broker")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource),
        agg -> agg.hasSumAtLeast(numSegments)
    );
  }

  /**
   * Verifies the total number of used segments in {@link #dataSource}.
   */
  private void verifyUsedSegmentCount(int expectedCount)
  {
    final Set<DataSegment> allUsedSegments = overlord
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.INCLUDING_OVERSHADOWED);
    Assertions.assertEquals(expectedCount, allUsedSegments.size());
  }
}
