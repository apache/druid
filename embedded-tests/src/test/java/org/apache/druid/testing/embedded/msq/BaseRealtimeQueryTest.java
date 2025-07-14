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

package org.apache.druid.testing.embedded.msq;

import com.fasterxml.jackson.core.JsonProcessingException;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskModule;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorIOConfig;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedClusterApis;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.junit5.EmbeddedClusterTestBase;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Base test for Kafka related embedded test.
 */
public class BaseRealtimeQueryTest extends EmbeddedClusterTestBase
{
  private static final Period TASK_DURATION = Period.hours(1);
  private static final int TASK_COUNT = 2;

  protected KafkaResource kafka;
  protected String topic;

  @Override
  protected EmbeddedDruidCluster createCluster()
  {
    kafka = new KafkaResource();
    return EmbeddedDruidCluster
        .withEmbeddedDerbyAndZookeeper()
        .addExtension(KafkaIndexTaskModule.class)
        .addResource(kafka);
  }

  @BeforeEach
  void setUp()
  {
    // Create Kafka topic.
    topic = EmbeddedClusterApis.createTestDatasourceName();
    kafka.createTopicWithPartitions(topic, 2);
  }

  /**
   * Submits a supervisor spec to the Overlord.
   */
  protected void submitSupervisor()
  {
    // Submit a supervisor.
    final KafkaSupervisorSpec kafkaSupervisorSpec = createKafkaSupervisor();
    final Map<String, String> startSupervisorResult =
        cluster.callApi().onLeaderOverlord(o -> o.postSupervisor(kafkaSupervisorSpec));
    Assertions.assertEquals(Map.of("id", dataSource), startSupervisorResult);
  }

  /**
   * Publishes data from a {@link QueryableIndex} to Kafka.
   */
  protected void publishToKafka(QueryableIndex index)
  {
    // Send data to Kafka.
    final QueryableIndexCursorFactory wikiCursorFactory =
        new QueryableIndexCursorFactory(index);
    final RowSignature wikiSignature = wikiCursorFactory.getRowSignature();
    kafka.produceRecordsToTopic(
        FrameTestUtil.readRowsFromCursorFactory(wikiCursorFactory)
                     .map(row -> {
                       final Map<String, Object> rowMap = new LinkedHashMap<>();
                       for (int i = 0; i < row.size(); i++) {
                         rowMap.put(wikiSignature.getColumnName(i), row.get(i));
                       }
                       try {
                         return new ProducerRecord<>(
                             topic,
                             ByteArrays.EMPTY_ARRAY,
                             TestHelper.JSON_MAPPER.writeValueAsBytes(rowMap)
                         );
                       }
                       catch (JsonProcessingException e) {
                         throw new RuntimeException(e);
                       }
                     })
                     .toList()
    );
  }

  @AfterEach
  void tearDownEach() throws ExecutionException, InterruptedException, IOException
  {
    final Map<String, String> terminateSupervisorResult =
        cluster.callApi().onLeaderOverlord(o -> o.terminateSupervisor(dataSource));
    Assertions.assertEquals(Map.of("id", dataSource), terminateSupervisorResult);

    // Cancel all running tasks, so we don't need to wait for them to hand off their segments.
    try (final CloseableIterator<TaskStatusPlus> it = cluster.leaderOverlord().taskStatuses(null, null, null).get()) {
      while (it.hasNext()) {
        cluster.leaderOverlord().cancelTask(it.next().getId());
      }
    }

    kafka.deleteTopic(topic);
  }

  private KafkaSupervisorSpec createKafkaSupervisor()
  {
    final Period startDelay = Period.millis(10);
    final Period supervisorRunPeriod = Period.millis(500);
    final boolean useEarliestOffset = true;

    return new KafkaSupervisorSpec(
        dataSource,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(new TimestampSpec("__time", "auto", null))
                  .withGranularity(new UniformGranularitySpec(Granularities.DAY, null, null))
                  .withDimensions(DimensionsSpec.builder().useSchemaDiscovery(true).build())
                  .build(),
        null,
        new KafkaSupervisorIOConfig(
            topic,
            null,
            new JsonInputFormat(null, null, null, null, null),
            null,
            TASK_COUNT,
            TASK_DURATION,
            kafka.consumerProperties(),
            null,
            null,
            null,
            startDelay,
            supervisorRunPeriod,
            useEarliestOffset,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        ),
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
