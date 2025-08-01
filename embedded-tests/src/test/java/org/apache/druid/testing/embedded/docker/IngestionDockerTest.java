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

package org.apache.druid.testing.embedded.docker;

import com.google.common.collect.ImmutableList;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.CompactionTask;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.TaskBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.storage.postgresql.PostgreSQLMetadataStorageModule;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.http.SqlTaskStatus;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.DruidCommand;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.emitter.LatchableEmitterModule;
import org.apache.druid.testing.embedded.indexing.MoreResources;
import org.apache.druid.testing.embedded.indexing.Resources;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Runs some basic ingestion tests using {@code DruidContainers}.
 * <p>
 * This test verifies functionality of Druid Docker images using
 * {@link DruidContainerResource}. However, the underlying cluster also uses
 * embedded servers either to provide visibility into the cluster state or to
 * test backwards compatibility of a given feature.
 */
public class IngestionDockerTest extends DockerTestBase
{
  // Druid Docker containers
  protected final DruidContainerResource overlordLeader =
      new DruidContainerResource(DruidCommand.OVERLORD).usingTestImage();
  protected final DruidContainerResource coordinator =
      new DruidContainerResource(DruidCommand.COORDINATOR).usingTestImage();
  protected final DruidContainerResource historical =
      new DruidContainerResource(DruidCommand.HISTORICAL).usingTestImage();
  protected final DruidContainerResource broker =
      new DruidContainerResource(DruidCommand.BROKER)
          .addProperty("druid.sql.planner.metadataRefreshPeriod", "PT1s")
          .usingTestImage();
  protected final DruidContainerResource router =
      new DruidContainerResource(DruidCommand.ROUTER).usingTestImage();
  protected final DruidContainerResource middleManager =
      new DruidContainerResource(DruidCommand.MIDDLE_MANAGER)
          .addProperty("druid.segment.handoff.pollDuration", "PT0.1s")
          .usingTestImage();

  // Follower EmbeddedOverlord to help serialize Task and Supervisor payloads
  protected final EmbeddedOverlord overlordFollower =
      new EmbeddedOverlord().addProperty("druid.plaintextPort", "7090");

  // Event collector to watch for metric events
  private final EmbeddedEventCollector eventCollector = new EmbeddedEventCollector()
      .addProperty("druid.emitter", "latching")
      .addProperty("druid.server.http.numThreads", "20");

  private final KafkaResource kafkaServer = new KafkaResource();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    return EmbeddedDruidCluster
        .withZookeeper()
        .useDruidContainers()
        // Needed for overlordFollower to recognize the KafkaSupervisor type
        .addExtensions(KafkaIndexTaskModule.class, LatchableEmitterModule.class, PostgreSQLMetadataStorageModule.class)
        .addResource(new PostgreSQLMetadataResource())
        .addResource(new MinIOStorageResource())
        .addResource(kafkaServer)
        .addCommonProperty(
            "druid.extensions.loadList",
            "[\"druid-s3-extensions\", \"druid-kafka-indexing-service\","
            + "\"druid-multi-stage-query\", \"postgresql-metadata-storage\"]"
        )
        .addCommonProperty("druid.emitter", "http")
        .addCommonProperty("druid.emitter.http.recipientBaseUrl", eventCollector.getMetricsUrl())
        .addCommonProperty("druid.emitter.http.flushMillis", "500")
        .addResource(coordinator)
        .addResource(overlordLeader)
        .addResource(middleManager)
        .addResource(historical)
        .addResource(broker)
        .addResource(router)
        .addServer(overlordFollower)
        .addServer(eventCollector);
  }

  @BeforeEach
  public void verifyOverlordLeader()
  {
    Assertions.assertFalse(
        overlordFollower.bindings().overlordLeaderSelector().isLeader()
    );
  }

  @AfterEach
  public void cleanUp()
  {
    markSegmentsAsUnused(dataSource);
    verifyOverlordLeader();
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
  public void test_runMsqTask_andQueryData()
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

    final EmbeddedMSQApis msqApis = new EmbeddedMSQApis(cluster, overlordFollower);
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
    Assertions.assertEquals(1, taskStatuses.size());
    Assertions.assertEquals(TaskState.RUNNING, taskStatuses.get(0).getStatusCode());

    // Suspend the supervisor and verify the state
    cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec.createSuspendedSpec())
    );
    supervisorStatus = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertTrue(supervisorStatus.isSuspended());
  }

  protected int markSegmentsAsUnused(String dataSource)
  {
    return cluster.callApi()
                  .onLeaderOverlord(o -> o.markSegmentsAsUnused(dataSource))
                  .getNumChangedSegments();
  }

  private KafkaSupervisorSpec createKafkaSupervisor(String topic)
  {
    return new KafkaSupervisorSpec(
        null,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("timestamp", null, null))
                  .withDimensions(DimensionsSpec.EMPTY)
                  .build(),
        createTuningConfig(),
        new KafkaSupervisorIOConfig(
            topic,
            null,
            new CsvInputFormat(List.of("timestamp", "item"), null, null, false, 0, false),
            null, null,
            null,
            kafkaServer.consumerProperties(),
            null, null, null, null, null,
            true,
            null, null, null, null, null, null, null, null
        ),
        null, null, null, null, null, null, null, null, null, null, null
    );
  }

  private KafkaSupervisorTuningConfig createTuningConfig()
  {
    return new KafkaSupervisorTuningConfig(
        null,
        null, null, null,
        1,
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null
    );
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
    final Set<DataSegment> allUsedSegments = overlordFollower
        .bindings()
        .segmentsMetadataStorage()
        .retrieveAllUsedSegments(dataSource, Segments.INCLUDING_OVERSHADOWED);
    Assertions.assertEquals(expectedCount, allUsedSegments.size());
  }
}
