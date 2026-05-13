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
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.indexing.kafka.simulate.KafkaResource;
import org.apache.druid.indexing.kafka.supervisor.KafkaSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.indexing.seekablestream.supervisor.BoundedStreamConfig;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for bounded Kafka supervisors (one-time ingestion with explicit start/end offsets).
 */
public class KafkaBoundedSupervisorTest extends StreamIndexTestBase
{
  private static final EmittingLogger log = new EmittingLogger(KafkaBoundedSupervisorTest.class);
  private final KafkaResource kafkaServer = new KafkaResource();

  @Override
  protected StreamIngestResource<?> getStreamIngestResource()
  {
    return kafkaServer;
  }

  @Test
  public void test_boundedSupervisor_ingestsDataAndCompletes()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);

    // Publish records before creating supervisor
    final int totalRecords = publish1kRecords(topic, false);

    // Get the current end offsets for all partitions
    Map<String, Long> endOffsets = kafkaServer.getPartitionOffsets(topic);
    Assertions.assertEquals(2, endOffsets.size(), "Should have 2 partitions");

    // Create bounded config with start offset 0 and current end offsets
    Map<String, Long> startOffsets = new HashMap<>();
    startOffsets.put("0", 0L);
    startOffsets.put("1", 0L);

    BoundedStreamConfig boundedConfig = new BoundedStreamConfig(startOffsets, endOffsets);

    // Create bounded supervisor
    final KafkaSupervisorSpec supervisor = createBoundedKafkaSupervisor(
        kafkaServer,
        topic,
        boundedConfig
    );

    cluster.callApi().postSupervisor(supervisor);

    // Wait for records to be ingested
    waitUntilPublishedRecordsAreIngested(totalRecords);

    // Wait for supervisor to transition to COMPLETED state
    waitForSupervisorToComplete(supervisor.getId());

    // Verify row count
    verifyRowCount(totalRecords);

    // Verify supervisor is in COMPLETED state
    final SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisor.getId());
    Assertions.assertEquals("COMPLETED", status.getState());
    Assertions.assertTrue(status.isHealthy());
  }

  @Test
  public void test_boundedSupervisor_withEmptyRange_completesImmediately()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 1);

    // Publish some records
    publish1kRecords(topic, false);

    // Get current offset
    Map<String, Long> currentOffsets = kafkaServer.getPartitionOffsets(topic);
    Long currentOffset = currentOffsets.get("0");

    // Create bounded config with start == end (empty range)
    Map<String, Long> offsets = new HashMap<>();
    offsets.put("0", currentOffset);

    BoundedStreamConfig boundedConfig = new BoundedStreamConfig(offsets, offsets);

    // Create bounded supervisor
    final KafkaSupervisorSpec supervisor = createBoundedKafkaSupervisor(
        kafkaServer,
        topic,
        boundedConfig
    );

    cluster.callApi().postSupervisor(supervisor);

    // Wait for supervisor to transition to COMPLETED state
    waitForSupervisorToComplete(supervisor.getId());

    // Verify supervisor is in COMPLETED state
    final SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisor.getId());
    Assertions.assertEquals("COMPLETED", status.getState());
  }

  private KafkaSupervisorSpec createBoundedKafkaSupervisor(
      KafkaResource kafkaServer,
      String topic,
      BoundedStreamConfig boundedConfig
  )
  {
    return createKafkaSupervisor(kafkaServer)
        .withIoConfig(io -> io
            .withKafkaInputFormat(new JsonInputFormat(null, null, null, null, null))
            .withBoundedStreamConfig(boundedConfig)
        )
        .build(dataSource, topic);
  }

  @Test
  public void test_boundedSupervisor_withMismatchedMetadata_is_unhealthy()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);
    publish1kRecords(topic, false);

    // Get the current end offsets for all partitions
    Map<String, Long> currentOffsets = kafkaServer.getPartitionOffsets(topic);
    Assertions.assertEquals(2, currentOffsets.size(), "Should have 2 partitions");

    // Create first bounded config - ingest only the first 100 records from each partition
    Map<String, Long> startOffsets1 = new HashMap<>();
    startOffsets1.put("0", 0L);
    startOffsets1.put("1", 0L);

    Map<String, Long> endOffsets1 = new HashMap<>();
    endOffsets1.put("0", 100L);
    endOffsets1.put("1", 100L);

    BoundedStreamConfig boundedConfig1 = new BoundedStreamConfig(startOffsets1, endOffsets1);

    // Create first bounded supervisor and run it to completion
    final KafkaSupervisorSpec supervisor1 = createBoundedKafkaSupervisor(
        kafkaServer,
        topic,
        boundedConfig1
    );

    cluster.callApi().postSupervisor(supervisor1);

    // Wait for records to be ingested (approximately 200 records total from both partitions)
    waitUntilPublishedRecordsAreIngested(200);

    // Wait for supervisor to transition to COMPLETED state
    waitForSupervisorToComplete(supervisor1.getId());

    // Verify supervisor is in COMPLETED state
    final SupervisorStatus status1 = cluster.callApi().getSupervisorStatus(supervisor1.getId());
    Assertions.assertEquals("COMPLETED", status1.getState());

    // Now try to create a second bounded supervisor with different bounded config on the same datasource
    Map<String, Long> startOffsets2 = new HashMap<>();
    startOffsets2.put("0", 50L);  // Different start offset
    startOffsets2.put("1", 50L);

    Map<String, Long> endOffsets2 = new HashMap<>();
    endOffsets2.put("0", 200L);  // Different end offset
    endOffsets2.put("1", 200L);

    BoundedStreamConfig boundedConfig2 = new BoundedStreamConfig(startOffsets2, endOffsets2);

    final KafkaSupervisorSpec supervisor2 = createBoundedKafkaSupervisor(
        kafkaServer,
        topic,
        boundedConfig2
    );

    // Post the second supervisor (it should use the same supervisor ID/datasource)
    cluster.callApi().postSupervisor(supervisor2);

    // Wait for the supervisor to process and detect the metadata mismatch
    // The exception we're testing for is thrown and logged, and causes the supervisor to become unhealthy
    waitForSupervisorToBeUnhealthy(supervisor2.getId());

    // Verify the supervisor is unhealthy
    final SupervisorStatus status2 = cluster.callApi().getSupervisorStatus(supervisor2.getId());
    Assertions.assertFalse(status2.isHealthy(), "Supervisor should be unhealthy after detecting metadata mismatch");
    Assertions.assertEquals("UNHEALTHY_SUPERVISOR", status2.getState(), "Supervisor state should be UNHEALTHY_SUPERVISOR");
  }

  /**
   * A new bounded run whose endOffset is less than the offset committed by a prior
   * run must not silently reach COMPLETED.
   */
  @Test
  public void test_boundedSupervisor_doesNotSilentlyCompleteWhenStaleOffsetExceedsNewEnd()
  {
    final String topic = IdUtils.getRandomId();
    kafkaServer.createTopicWithPartitions(topic, 2);
    publish1kRecords(topic, false);

    // Run 1: ingest up to offset 100 on each partition and complete.
    Map<String, Long> startOffsets1 = new HashMap<>();
    startOffsets1.put("0", 0L);
    startOffsets1.put("1", 0L);

    Map<String, Long> endOffsets1 = new HashMap<>();
    endOffsets1.put("0", 100L);
    endOffsets1.put("1", 150L);

    BoundedStreamConfig boundedConfig1 = new BoundedStreamConfig(startOffsets1, endOffsets1);
    final KafkaSupervisorSpec supervisor1 = createBoundedKafkaSupervisor(kafkaServer, topic, boundedConfig1);

    cluster.callApi().postSupervisor(supervisor1);
    waitUntilPublishedRecordsAreIngested(250);
    waitForSupervisorToComplete(supervisor1.getId());

    final SupervisorStatus status1 = cluster.callApi().getSupervisorStatus(supervisor1.getId());
    Assertions.assertEquals("COMPLETED", status1.getState());

    // Run 2: same datasource, endOffset (50) < stale committed offset (100).
    // Without the fix the supervisor reaches COMPLETED immediately without running tasks.
    // With the fix it detects the config mismatch and becomes UNHEALTHY_SUPERVISOR.
    Map<String, Long> startOffsets2 = new HashMap<>();
    startOffsets2.put("0", 0L);
    startOffsets2.put("1", 0L);

    Map<String, Long> endOffsets2 = new HashMap<>();
    endOffsets2.put("0", 50L);
    endOffsets2.put("1", 50L);

    BoundedStreamConfig boundedConfig2 = new BoundedStreamConfig(startOffsets2, endOffsets2);
    final KafkaSupervisorSpec supervisor2 = createBoundedKafkaSupervisor(kafkaServer, topic, boundedConfig2);

    cluster.callApi().postSupervisor(supervisor2);
    waitForSupervisorToBeUnhealthy(supervisor2.getId());

    final SupervisorStatus status2 = cluster.callApi().getSupervisorStatus(supervisor2.getId());
    Assertions.assertFalse(status2.isHealthy(), "Supervisor should be unhealthy after detecting metadata mismatch");
    Assertions.assertEquals("UNHEALTHY_SUPERVISOR", status2.getState(), "Supervisor state should be UNHEALTHY_SUPERVISOR");
  }

  private void waitForSupervisorToComplete(String supervisorId)
  {
    // Wait for supervisor to reach COMPLETED state
    int maxAttempts = 60; // 60 seconds timeout
    int attempt = 0;

    while (attempt < maxAttempts) {
      try {
        SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisorId);
        if ("COMPLETED".equals(status.getState())) {
          return;
        }
        Thread.sleep(1000);
        attempt++;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for supervisor to complete", e);
      }
      catch (Exception e) {
        // Supervisor might not be found immediately, retry
        attempt++;
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting", ie);
        }
      }
    }

    Assertions.fail("Supervisor did not complete within timeout");
  }

  private void waitForSupervisorToBeUnhealthy(String supervisorId)
  {
    // Wait for supervisor to become unhealthy after detecting the metadata mismatch
    int maxAttempts = 30; // 30 seconds timeout
    int attempt = 0;

    while (attempt < maxAttempts) {
      try {
        SupervisorStatus status = cluster.callApi().getSupervisorStatus(supervisorId);

        // The supervisor should become unhealthy when the exception is thrown
        if (!status.isHealthy()) {
          log.info("Supervisor became unhealthy with state: %s", status.getDetailedState());
          return;
        }

        Thread.sleep(1000);
        attempt++;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for supervisor to become unhealthy", e);
      }
      catch (Exception e) {
        // Supervisor might not be found immediately, retry
        attempt++;
        try {
          Thread.sleep(1000);
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting", ie);
        }
      }
    }

    Assertions.fail("Supervisor did not become unhealthy due to metadata mismatch within timeout");
  }
}
