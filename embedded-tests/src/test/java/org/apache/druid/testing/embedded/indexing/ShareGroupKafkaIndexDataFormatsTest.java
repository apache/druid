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

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.kafka.ShareGroupIndexTask;
import org.apache.druid.indexing.kafka.ShareGroupIndexTaskIOConfig;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.embedded.EmbeddedDruidCluster;
import org.apache.druid.testing.embedded.StreamIngestResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;

@Tag("kafka-share-group")
public class ShareGroupKafkaIndexDataFormatsTest extends StreamIndexDataFormatsTestBase
{
  private final ShareGroupKafkaResource kafka = new ShareGroupKafkaResource();

  @Override
  protected StreamIngestResource<?> getStreamResource()
  {
    return kafka;
  }

  @Override
  public EmbeddedDruidCluster createCluster()
  {
    indexer().addProperty("druid.indexer.task.gracefulShutdownTimeout", "PT5S");
    return super.createCluster().useDefaultTimeoutForLatchableEmitter(30);
  }

  @Override
  protected void runIngestionAndVerify(
      String dataSource,
      String topic,
      InputFormat inputFormat,
      int expectedCount
  )
  {
    final String groupId = dataSource + "-format-test";
    kafka.setShareGroupAutoOffsetReset(groupId, "earliest");

    final ShareGroupIndexTask task = new ShareGroupIndexTask(
        null,
        null,
        DataSchema.builder()
                  .withDataSource(dataSource)
                  .withTimestamp(TimestampSpec.DEFAULT)
                  .withDimensions(DimensionsSpec.EMPTY)
                  .withGranularity(
                      new UniformGranularitySpec(
                          Granularities.HOUR,
                          Granularities.NONE,
                          false,
                          null
                      )
                  )
                  .build(),
        defaultTuningConfig(),
        new ShareGroupIndexTaskIOConfig(
            topic,
            groupId,
            kafka.consumerProperties(),
            inputFormat,
            null
        ),
        null,
        overlord().bindings().jsonMapper()
    );

    cluster.callApi().submitTask(task);
    try {
      indexer().latchableEmitter().waitForEventAggregate(
          event -> event.hasMetricName("ingest/events/processed")
                        .hasDimension(DruidMetrics.DATASOURCE, dataSource),
          aggregate -> aggregate.hasSumAtLeast(expectedCount)
      );

      assertExactRowCountEventually(dataSource, expectedCount);
      cluster.callApi().waitForAllSegmentsToBeAvailable(dataSource, coordinator(), broker());
    }
    finally {
      cluster.callApi().onLeaderOverlord(overlord -> overlord.cancelTask(task.getId()));
      cluster.callApi().waitForTaskToFinish(task.getId(), overlord().latchableEmitter());
      waitForTaskRunnerRemoval(task.getId());
    }
  }

  private void assertExactRowCountEventually(String dataSource, int expectedCount)
  {
    final long deadlineMillis = System.currentTimeMillis() + 20_000L;
    String lastResult = null;
    Exception lastException = null;
    while (System.currentTimeMillis() < deadlineMillis) {
      try {
        lastResult = cluster.runSql("SELECT COUNT(*) FROM %s", dataSource);
        final long actualCount = Long.parseLong(lastResult);
        if (actualCount == expectedCount) {
          return;
        }
        Assertions.assertTrue(
            actualCount < expectedCount,
            "Expected exactly [" + expectedCount + "] rows but found [" + actualCount + "]"
        );
        lastException = null;
      }
      catch (AssertionError e) {
        throw e;
      }
      catch (Exception e) {
        lastException = e;
      }

      try {
        Thread.sleep(250L);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        Assertions.fail("Interrupted while waiting for share-group rows", e);
      }
    }

    Assertions.fail(
        "Expected exactly [" + expectedCount + "] rows but last result was [" + lastResult
        + "], lastException=" + lastException
    );
  }

  private void waitForTaskRunnerRemoval(String taskId)
  {
    final TaskRunner taskRunner = indexer().bindings().getInstance(TaskRunner.class);
    final long deadlineMillis = System.currentTimeMillis() + 10_000L;
    while (System.currentTimeMillis() < deadlineMillis) {
      final boolean taskPresent = taskRunner.getKnownTasks()
                                            .stream()
                                            .anyMatch(task -> taskId.equals(task.getTaskId()));
      if (!taskPresent) {
        return;
      }

      try {
        Thread.sleep(100L);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        Assertions.fail("Interrupted while waiting for task runner cleanup", e);
      }
    }

    Assertions.fail("Task [" + taskId + "] remained in the indexer task runner after cancellation");
  }

  private static KafkaIndexTaskTuningConfig defaultTuningConfig()
  {
    return new KafkaIndexTaskTuningConfig(
        null,
        null,
        null,
        null,
        1,
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
