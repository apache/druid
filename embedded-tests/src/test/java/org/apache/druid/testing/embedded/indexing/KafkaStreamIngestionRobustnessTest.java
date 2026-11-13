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

import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.apache.druid.testing.embedded.tools.EventSerializer;
import org.apache.druid.testing.embedded.tools.FaultyStreamEventStreamGenerator;
import org.apache.druid.testing.embedded.tools.FaultyStreamEventStreamGenerator.DataVariant;
import org.apache.druid.testing.embedded.tools.JsonEventSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests that verify the robustness of streaming ingestion when the stream contains
 * faulty data such as invalid JSON, null/empty fields, multi-row data, and empty strings.
 *
 * <p>These tests ensure that the supervisor and indexing tasks remain healthy and continue
 * to ingest valid data even when some records are malformed.
 */
public class KafkaStreamIngestionRobustnessTest extends StreamIndexTestBase
{
  private final KafkaResource kafkaServer = new KafkaResource();

  private static final int EVENTS_PER_SECOND = 6;
  private static final long CYCLE_PADDING_MS = 100;
  private static final int TOTAL_SECONDS = 10;
  private static final double FAULTY_RATIO = 0.2; // 20% faulty records

  @Override
  protected StreamIngestResource<?> getStreamIngestResource()
  {
    return kafkaServer;
  }

  /**
   * Creates a standard Kafka supervisor for JSON input format.
   */
  private KafkaSupervisorSpec createJsonSupervisor(final String topic)
  {
    return createKafkaSupervisor(kafkaServer)
        .build(dataSource, topic);
  }

  /**
   * Publishes records to the topic using a generator that mixes valid and faulty data.
   *
   * @return the number of valid records published
   */
  private int publishMixedRecords(
      final String topic,
      final DataVariant variant,
      final int totalSeconds,
      final double faultyRatio
  )
  {
    final EventSerializer serializer = new JsonEventSerializer(overlord.bindings().jsonMapper());
    final FaultyStreamEventStreamGenerator generator = new FaultyStreamEventStreamGenerator(
        serializer,
        EVENTS_PER_SECOND,
        CYCLE_PADDING_MS,
        variant,
        faultyRatio
    );

    final List<byte[]> validEvents = generator.generateEvents(totalSeconds);
    final List<byte[]> allRecords = new ArrayList<>();
    int validCount = 0;

    for (int i = 0; i < validEvents.size(); i++) {
      if (generator.isFaultyEvent(i)) {
        allRecords.add(generator.generateFaultyBytes(i));
      } else {
        allRecords.add(validEvents.get(i));
        validCount++;
      }
    }

    kafkaServer.publishRecordsToTopic(topic, allRecords);
    return validCount;
  }

  @Test
  @Timeout(60)
  public void test_supervisorHandlesInvalidJsonGracefully()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    // Publish mixed records: 80% valid + 20% invalid JSON
    final int validCount = publishMixedRecords(topic, DataVariant.INVALID_JSON, TOTAL_SECONDS, FAULTY_RATIO);

    // Create and start the supervisor
    final KafkaSupervisorSpec supervisor = createJsonSupervisor(topic);
    cluster.callApi().postSupervisor(supervisor);

    // Verify supervisor is healthy
    verifySupervisorIsRunningHealthy(supervisor.getId());

    // Wait for valid records to be ingested (invalid ones should be rejected but not crash)
    waitUntilPublishedRecordsAreIngested(validCount);

    // Verify the row count matches only valid records
    verifyRowCount(validCount);
  }

  @Test
  @Timeout(60)
  public void test_supervisorHandlesNullFieldsGracefully()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int validCount = publishMixedRecords(topic, DataVariant.NULL_FIELDS, TOTAL_SECONDS, FAULTY_RATIO);

    final KafkaSupervisorSpec supervisor = createJsonSupervisor(topic);
    cluster.callApi().postSupervisor(supervisor);

    verifySupervisorIsRunningHealthy(supervisor.getId());
    waitUntilPublishedRecordsAreIngested(validCount);
    verifyRowCount(validCount);
  }

  @Test
  @Timeout(60)
  public void test_supervisorHandlesEmptyJsonGracefully()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int validCount = publishMixedRecords(topic, DataVariant.EMPTY_JSON, TOTAL_SECONDS, FAULTY_RATIO);

    final KafkaSupervisorSpec supervisor = createJsonSupervisor(topic);
    cluster.callApi().postSupervisor(supervisor);

    verifySupervisorIsRunningHealthy(supervisor.getId());
    waitUntilPublishedRecordsAreIngested(validCount);
    verifyRowCount(validCount);
  }

  @Test
  @Timeout(60)
  public void test_supervisorHandlesMultiRowDataGracefully()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    // Multi-row data: each faulty record is a JSON array with 2 objects, so the total
    // ingested rows may exceed validCount. We just verify the supervisor stays healthy
    // and some data gets ingested.
    final int validCount = publishMixedRecords(topic, DataVariant.MULTI_ROW, TOTAL_SECONDS, FAULTY_RATIO);

    final KafkaSupervisorSpec supervisor = createJsonSupervisor(topic);
    cluster.callApi().postSupervisor(supervisor);

    verifySupervisorIsRunningHealthy(supervisor.getId());

    // Multi-row records will be split into individual rows by the JSON parser,
    // so the total ingested count may be higher than validCount. We just verify
    // that at least the valid records are ingested.
    waitUntilPublishedRecordsAreIngested(validCount);
  }

  @Test
  @Timeout(60)
  public void test_supervisorHandlesEmptyStringGracefully()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int validCount = publishMixedRecords(topic, DataVariant.EMPTY_STRING, TOTAL_SECONDS, FAULTY_RATIO);

    final KafkaSupervisorSpec supervisor = createJsonSupervisor(topic);
    cluster.callApi().postSupervisor(supervisor);

    verifySupervisorIsRunningHealthy(supervisor.getId());
    waitUntilPublishedRecordsAreIngested(validCount);
    verifyRowCount(validCount);
  }

  @Test
  @Timeout(60)
  public void test_supervisorHandlesAllValidDataCorrectly()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    final int validCount = publishMixedRecords(topic, DataVariant.ALL_VALID, TOTAL_SECONDS, 0.0);

    final KafkaSupervisorSpec supervisor = createJsonSupervisor(topic);
    cluster.callApi().postSupervisor(supervisor);

    verifySupervisorIsRunningHealthy(supervisor.getId());
    waitUntilPublishedRecordsAreIngested(validCount);
    verifyRowCount(validCount);
  }
}