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
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorTuningConfig;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedBroker;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.EmbeddedOverlord;
import org.apache.druid.testing.embedded.EmbeddedRouter;
import org.apache.druid.testing.embedded.derby.EmbeddedDerbyMetadataResource;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.druid.testing.embedded.minio.MinIOStorageResource;
import org.apache.druid.timeline.DataSegment;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;
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
 * Runs some basic ingestion tests using {@link DruidContainers}.
 * <p>
 * This test verifies functionality of Druid Docker images using
 * {@link DruidContainerResource}. However, the underlying cluster also uses
 * embedded servers either to provide visibility into the cluster state or to
 * test backwards compatibility of a given feature.
 */
public class IngestionDockerTest extends EmbeddedClusterTestBase
{
  static {
    System.setProperty("druid.testing.docker.image", "apache/druid:35.0.0-SNAPSHOT");
  }

  // Druid Docker containers
  protected final DruidContainerResource overlordLeader = DruidContainers.newOverlord().usingTestImage();
  protected final DruidContainerResource coordinator = DruidContainers.newCoordinator().usingTestImage();
  protected final DruidContainerResource historical = DruidContainers.newHistorical().usingTestImage();
  protected final DruidContainerResource broker1 = DruidContainers.newBroker().usingTestImage();
  protected final DruidContainerResource middleManager = DruidContainers
      .newMiddleManager()
      .usingTestImage()
      .addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

  // Follower EmbeddedOverlord to watch segment publish events
  private final EmbeddedOverlord overlordFollower = new EmbeddedOverlord()
      .addProperty("druid.plaintextPort", "7090")
      .addProperty("druid.manager.segments.useIncrementalCache", "always")
      .addProperty("druid.manager.segments.pollDuration", "PT0.1s");

  // Additional EmbeddedBroker to wait for segments to become queryable
  private final EmbeddedBroker broker2 = new EmbeddedBroker()
      .addProperty("druid.plaintextPort", "7082");

  private final KafkaResource kafkaServer = new KafkaResource();

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withZookeeper();

    // Use DruidContainerResources in the cluster as they are the test target
    // Use EmbeddedDruidServers to provide visiblility into the cluster state
    return cluster
        .useDruidContainers()
        .useLatchableEmitter()
        // Needed for overlordFollower to recognize the KafkaSupervisor type
        .addExtension(KafkaIndexTaskModule.class)
        .addResource(new EmbeddedDerbyMetadataResource())
        .addResource(new MinIOStorageResource())
        .addResource(kafkaServer)
        .addCommonProperty(
            "druid.extensions.loadList",
            "[\"druid-s3-extensions\", \"druid-kafka-indexing-service\", \"druid-multi-stage-query\"]"
        )
        .addResource(coordinator)
        .addResource(overlordLeader)
        .addResource(middleManager)
        .addResource(historical)
        .addResource(broker1)
        .addServer(overlordFollower)
        .addServer(broker2)
        .addServer(new EmbeddedRouter());
  }

  @BeforeEach
  @AfterEach
  public void verifyOverlordLeader()
  {
    Assertions.assertFalse(
        overlordFollower.bindings().overlordLeaderSelector().isLeader()
    );
  }

  @Test
  public void test_runIndexTask_andKillData()
  {
    // To be populated once TaskBuilder is merged in #18207
  }

  @Test
  public void test_runIndexParallelTask_andCompactData()
  {
    // To be populated once TaskBuilder is merged in #18207
  }

  @Test
  public void test_runMsqTask_andQueryData()
  {
    // To be populated once TaskBuilder is merged in #18207
  }

  @Test
  public void test_runIndexTask()
  {
    final String taskId = IdUtils.getRandomId();
    final Object task = createIndexTaskForInlineData(
        taskId,
        StringUtils.replace(Resources.CSV_DATA_10_DAYS, "\n", "\\n")
    );

    cluster.callApi().onLeaderOverlord(o -> o.runTask(taskId, task));
    waitForCachedUsedSegmentCount(10);
    verifyUsedSegmentCount(10);
  }

  private Object createIndexTaskForInlineData(String taskId, String inlineDataCsv)
  {
    return EmbeddedClusterApis.createTaskFromPayload(
        taskId,
        StringUtils.format(Resources.INDEX_TASK_PAYLOAD_WITH_INLINE_DATA, inlineDataCsv, dataSource)
    );
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
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(supervisorId, topic);

    final Map<String, String> startSupervisorResult = cluster.callApi().onLeaderOverlord(
        o -> o.postSupervisor(kafkaSupervisorSpec)
    );
    Assertions.assertEquals(Map.of("id", supervisorId), startSupervisorResult);

    // Wait for the broker to discover the realtime segments
    broker2.latchableEmitter().waitForEvent(
        event -> event.hasDimension(DruidMetrics.DATASOURCE, dataSource)
    );

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

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, String topic)
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

  /**
   * Waits for the {@link #overlordFollower} to add the expected number of used
   * segments to its cache. This is a proxy for waiting for the task to finish.
   * Since the tasks are being managed by the {@link #overlordLeader} which is
   * running in a container, we cannot watch the metrics emitted by it.
   */
  private void waitForCachedUsedSegmentCount(int expectedCount)
  {
    overlordFollower.latchableEmitter().waitForEvent(
        event -> event.hasMetricName("segment/metadataCache/used/count")
                      .hasDimension(DruidMetrics.DATASOURCE, dataSource)
                      .hasValueAtLeast(expectedCount)
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
