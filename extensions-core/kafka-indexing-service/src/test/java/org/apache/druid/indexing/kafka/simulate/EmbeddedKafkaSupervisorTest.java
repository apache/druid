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
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpecBuilder;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.query.DruidMetrics;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class EmbeddedKafkaSupervisorTest extends EmbeddedClusterTestBase
{
  private final EmbeddedBroker broker = new EmbeddedBroker();
  private final EmbeddedIndexer indexer = new EmbeddedIndexer();
  private final EmbeddedOverlord overlord = new EmbeddedOverlord();
  private final EmbeddedHistorical historical = new EmbeddedHistorical();
  private KafkaResource kafkaServer;

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    final EmbeddedDruidCluster cluster = EmbeddedDruidCluster.withEmbeddedDerbyAndZookeeper();
    indexer.addProperty("druid.segment.handoff.pollDuration", "PT0.1s");

    kafkaServer = new KafkaResource();

    cluster.addExtension(KafkaIndexTaskModule.class)
           .addResource(kafkaServer)
           .useLatchableEmitter()
           .addServer(new EmbeddedCoordinator())
           .addServer(overlord)
           .addServer(indexer)
           .addServer(historical)
           .addServer(broker);

    return cluster;
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
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor(supervisorId, topic);

    Assertions.assertEquals(supervisorId, cluster.callApi().postSupervisor(kafkaSupervisorSpec));

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
    List<TaskStatusPlus> taskStatuses = ImmutableList.copyOf(
        (CloseableIterator<TaskStatusPlus>)
            cluster.callApi().onLeaderOverlord(o -> o.taskStatuses(null, dataSource, 1))
    );
    Assertions.assertEquals(1, taskStatuses.size());
    Assertions.assertEquals(TaskState.RUNNING, taskStatuses.get(0).getStatusCode());

    // Verify the count of rows ingested into the datasource so far
    Assertions.assertEquals("10", cluster.runSql("SELECT COUNT(*) FROM %s", dataSource));

    // Suspend the supervisor and verify the state
    cluster.callApi().postSupervisor(kafkaSupervisorSpec.createSuspendedSpec());
    supervisorStatus = cluster.callApi().getSupervisorStatus(supervisorId);
    Assertions.assertTrue(supervisorStatus.isSuspended());
    indexer.latchableEmitter().waitForEventAggregate(
        event -> event.hasMetricName("ingest/handoff/count")
                      .hasDimension(DruidMetrics.DATASOURCE, List.of(dataSource)),
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

  private KafkaSupervisorSpec createKafkaSupervisor(String supervisorId, String topic)
  {
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
        )
        .withId(supervisorId)
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
}
