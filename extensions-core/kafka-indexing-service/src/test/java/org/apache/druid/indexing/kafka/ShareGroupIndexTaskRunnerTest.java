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
 * Targeted unit tests for {@link ShareGroupIndexTaskRunner} that do not
 * require a full Druid toolbox. These cover behaviors that are isolatable
 * outside of {@link ShareGroupIndexTaskRunner#run()} (which itself is
 * exercised end-to-end by {@code EmbeddedShareGroupIngestionTest}).
 *
 * <p>Specifically validates:</p>
 * <ul>
 *   <li>{@link ShareGroupIndexTaskRunner#requestWakeup()} is null-safe when the
 *       run loop is not active (called by {@code stopGracefully} before/after
 *       {@code runTask}).</li>
 *   <li>The DIP-friendly constructor correctly accepts a custom
 *       {@code AcknowledgingRecordSupplier} factory; the factory is the only
 *       seam needed to write higher-fidelity runner tests in the future
 *       (Phase 2 supervisor work).</li>
 *   <li>The commit-failure metric name is the agreed contract
 *       ({@code ingest/shareGroup/commitFailures}).</li>
 * </ul>
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

  /**
   * {@code stopGracefully} may be called before {@code runTask} or after it
   * exits. Both windows leave {@code activeSupplier} == null, so
   * {@code requestWakeup} must be a safe no-op.
   */
  @Test
  public void testRequestWakeupIsNullSafeWhenNoActiveSupplier()
  {
    final ShareGroupIndexTaskRunner runner = new ShareGroupIndexTaskRunner(task, toolbox, mapper);
    runner.requestWakeup();
  }

  /**
   * A task lifecycle assertion: calling {@code stopGracefully} when no
   * runner is active (i.e. before {@code runTask} ran or after it exited)
   * must not throw and must set {@code stopRequested}.
   */
  @Test
  public void testStopGracefullyBeforeRunTaskIsSafe()
  {
    Assert.assertFalse(task.isStopRequested());
    task.stopGracefully(null);
    Assert.assertTrue(task.isStopRequested());
  }

  /**
   * Sanity check that the DIP factory constructor is wired and does not crash
   * when only constructing the runner (run() is exercised by IT).
   */
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

  /**
   * Locks the commit-failure metric name as a public contract: it is the
   * single signal operators use to alert on broker commit issues.
   */
  @Test
  public void testCommitFailureMetricName()
  {
    Assert.assertEquals(
        "ingest/shareGroup/commitFailures",
        ShareGroupIndexTaskRunner.METRIC_COMMIT_FAILURES
    );
  }
}
