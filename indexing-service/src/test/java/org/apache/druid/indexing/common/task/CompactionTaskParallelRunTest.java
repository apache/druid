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

package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.stats.RowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.batch.parallel.AbstractParallelIndexSupervisorTaskTest;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseParallelIndexingTest.TestSupervisorTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class CompactionTaskParallelRunTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  private static final String DATA_SOURCE = "test";
  private static final RetryPolicyFactory RETRY_POLICY_FACTORY = new RetryPolicyFactory(new RetryPolicyConfig());

  private final AppenderatorsManager appenderatorsManager = new TestAppenderatorsManager();
  private final LockGranularity lockGranularity;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final CoordinatorClient coordinatorClient;

  public CompactionTaskParallelRunTest(LockGranularity lockGranularity)
  {
    this.lockGranularity = lockGranularity;
    this.rowIngestionMetersFactory = new TestUtils().getRowIngestionMetersFactory();
    coordinatorClient = new CoordinatorClient(null, null)
    {
      @Override
      public List<DataSegment> getDatabaseSegmentDataSourceSegments(String dataSource, List<Interval> intervals)
      {
        return getStorageCoordinator().getUsedSegmentsForIntervals(dataSource, intervals);
      }

      @Override
      public DataSegment getDatabaseSegmentDataSourceSegment(String dataSource, String segmentId)
      {
        ImmutableDruidDataSource druidDataSource = getMetadataSegmentManager().getImmutableDataSourceWithUsedSegments(
            dataSource
        );
        if (druidDataSource == null) {
          throw new ISE("Unknown datasource[%s]", dataSource);
        }

        for (SegmentId possibleSegmentId : SegmentId.iteratePossibleParsingsWithDataSource(dataSource, segmentId)) {
          DataSegment segment = druidDataSource.getSegment(possibleSegmentId);
          if (segment != null) {
            return segment;
          }
        }
        throw new ISE("Can't find segment for id[%s]", segmentId);
      }
    };
  }

  @Before
  public void setup() throws IOException
  {
    indexingServiceClient = new LocalIndexingServiceClient();
    localDeepStorage = temporaryFolder.newFolder();
  }

  @After
  public void teardown()
  {
    indexingServiceClient.shutdown();
    temporaryFolder.delete();
  }

  @Test
  public void testRunParallel() throws Exception
  {
    runIndexTask();

    final CompactionTask compactionTask = new TestCompactionTask(
        null,
        null,
        DATA_SOURCE,
        new CompactionIOConfig(new CompactionIntervalSpec(Intervals.of("2014-01-01/2014-01-02"), null)),
        null,
        null,
        null,
        null,
        newTuningConfig(),
        null,
        getObjectMapper(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        rowIngestionMetersFactory,
        coordinatorClient,
        indexingServiceClient,
        getSegmentLoaderFactory(),
        RETRY_POLICY_FACTORY,
        appenderatorsManager
    );

    runTask(compactionTask);
  }

  private void runIndexTask() throws Exception
  {
    File tmpDir = temporaryFolder.newFolder();
    File tmpFile = File.createTempFile("druid", "index", tmpDir);

    try (BufferedWriter writer = Files.newWriter(tmpFile, StandardCharsets.UTF_8)) {
      writer.write("2014-01-01T00:00:10Z,a,1\n");
      writer.write("2014-01-01T00:00:10Z,b,2\n");
      writer.write("2014-01-01T00:00:10Z,c,3\n");
      writer.write("2014-01-01T01:00:20Z,a,1\n");
      writer.write("2014-01-01T01:00:20Z,b,2\n");
      writer.write("2014-01-01T01:00:20Z,c,3\n");
      writer.write("2014-01-01T02:00:30Z,a,1\n");
      writer.write("2014-01-01T02:00:30Z,b,2\n");
      writer.write("2014-01-01T02:00:30Z,c,3\n");
    }

    IndexTask indexTask = new IndexTask(
        null,
        null,
        IndexTaskTest.createIngestionSpec(
            getObjectMapper(),
            tmpDir,
            CompactionTaskRunTest.DEFAULT_PARSE_SPEC,
            new UniformGranularitySpec(
                Granularities.HOUR,
                Granularities.MINUTE,
                null
            ),
            IndexTaskTest.createTuningConfig(2, 2, null, 2L, null, null, false, true),
            false
        ),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        rowIngestionMetersFactory,
        appenderatorsManager
    );

    runTask(indexTask);
  }

  private void runTask(Task task) throws Exception
  {
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);
    prepareTaskForLocking(task);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
    shutdownTask(task);
  }

  private static ParallelIndexTuningConfig newTuningConfig()
  {
    return new ParallelIndexTuningConfig(
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
        2,
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

  private static class TestCompactionTask extends CompactionTask
  {
    private final IndexingServiceClient indexingServiceClient;

    TestCompactionTask(
        String id,
        TaskResource taskResource,
        String dataSource,
        @Nullable CompactionIOConfig ioConfig,
        @Nullable DimensionsSpec dimensions,
        @Nullable DimensionsSpec dimensionsSpec,
        @Nullable AggregatorFactory[] metricsSpec,
        @Nullable Granularity segmentGranularity,
        @Nullable ParallelIndexTuningConfig tuningConfig,
        @Nullable Map<String, Object> context,
        ObjectMapper jsonMapper,
        AuthorizerMapper authorizerMapper,
        ChatHandlerProvider chatHandlerProvider,
        RowIngestionMetersFactory rowIngestionMetersFactory,
        CoordinatorClient coordinatorClient,
        @Nullable IndexingServiceClient indexingServiceClient,
        SegmentLoaderFactory segmentLoaderFactory,
        RetryPolicyFactory retryPolicyFactory,
        AppenderatorsManager appenderatorsManager
    )
    {
      super(
          id,
          taskResource,
          dataSource,
          null,
          null,
          ioConfig,
          dimensions,
          dimensionsSpec,
          metricsSpec,
          segmentGranularity,
          tuningConfig,
          context,
          jsonMapper,
          authorizerMapper,
          chatHandlerProvider,
          rowIngestionMetersFactory,
          coordinatorClient,
          indexingServiceClient,
          segmentLoaderFactory,
          retryPolicyFactory,
          appenderatorsManager
      );
      this.indexingServiceClient = indexingServiceClient;
    }

    @Override
    ParallelIndexSupervisorTask newTask(String taskId, ParallelIndexIngestionSpec ingestionSpec)
    {
      return new TestSupervisorTask(
          taskId,
          null,
          ingestionSpec,
          createContextForSubtask(),
          indexingServiceClient
      );
    }
  }
}
