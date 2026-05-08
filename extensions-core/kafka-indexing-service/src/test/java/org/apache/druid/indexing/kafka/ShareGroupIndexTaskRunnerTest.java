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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.indexing.DataSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

/**
 * Lightweight unit tests for {@link ShareGroupIndexTaskRunner}; the run loop
 * itself is covered end-to-end by {@code EmbeddedShareGroupIngestionTest}.
 */
public class ShareGroupIndexTaskRunnerTest
{
  private ObjectMapper mapper;
  private ShareGroupIndexTask task;
  private TaskToolbox toolbox;

  @Before
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    toolbox = Mockito.mock(TaskToolbox.class);

    final DataSchema dataSchema = DataSchema.builder()
        .withDataSource("test_datasource")
        .withTimestamp(new TimestampSpec("__time", null, null))
        .withDimensions(DimensionsSpec.EMPTY)
        .build();

    final Map<String, Object> consumerProps = ImmutableMap.of("bootstrap.servers", "localhost:9092");
    final ShareGroupIndexTaskIOConfig ioConfig = new ShareGroupIndexTaskIOConfig(
        "test-topic",
        "test-share-group",
        consumerProps,
        null,
        null
    );
    final KafkaIndexTaskTuningConfig tuningConfig = new KafkaIndexTaskTuningConfig(
        null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null,
        null, null
    );
    task = new ShareGroupIndexTask(
        "task_runner_test",
        null,
        dataSchema,
        tuningConfig,
        ioConfig,
        null,
        mapper
    );
  }

  @Test
  public void testRequestWakeupIsNullSafeWhenNoActiveSupplier()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    runner.requestWakeup();
  }

  @Test
  public void testStopGracefullyBeforeRunTaskIsSafe()
  {
    Assert.assertFalse(task.isStopRequested());
    task.stopGracefully(null);
    Assert.assertTrue(task.isStopRequested());
  }

  @Test
  public void testRunnerAcceptsCustomSupplierFactory()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(
        task,
        toolbox,
        mapper,
        ioConfig -> Mockito.mock(
            org.apache.druid.indexing.seekablestream.common.AcknowledgingRecordSupplier.class
        )
    );
    Assert.assertNotNull(runner);
    runner.requestWakeup();
  }

  @Test
  public void testCommitFailureMetricName()
  {
    Assert.assertEquals(
        "ingest/shareGroup/commitFailures",
        ShareGroupIndexTaskRunner.METRIC_COMMIT_FAILURES
    );
  }
}
