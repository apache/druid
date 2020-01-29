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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.ImmutableList;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(Parameterized.class)
public class HashPartitionMultiPhaseParallelIndexingTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final ParseSpec PARSE_SPEC = new CSVParseSpec(
      new TimestampSpec(
          "ts",
          "auto",
          null
      ),
      new DimensionsSpec(
          DimensionsSpec.getDefaultSchemas(Arrays.asList("ts", "dim1", "dim2")),
          new ArrayList<>(),
          new ArrayList<>()
      ),
      null,
      Arrays.asList("ts", "dim1", "dim2", "val"),
      false,
      0
  );
  private static final int MAX_NUM_CONCURRENT_SUB_TASKS = 2;

  @Parameterized.Parameters(name = "{0}, useInputFormatApi={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, false},
        new Object[]{LockGranularity.TIME_CHUNK, true},
        new Object[]{LockGranularity.SEGMENT, true}
    );
  }

  private File inputDir;

  public HashPartitionMultiPhaseParallelIndexingTest(LockGranularity lockGranularity, boolean useInputFormatApi)
  {
    super(lockGranularity, useInputFormatApi);
  }

  @Override
  @Before
  public void setup() throws IOException
  {
    super.setup();

    inputDir = temporaryFolder.newFolder("data");
    // set up data
    for (int i = 0; i < 10; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "test_" + i).toPath(), StandardCharsets.UTF_8)) {
        for (int j = 0; j < 10; j++) {
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", j + 1, i + 10, i));
          writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", j + 2, i + 11, i));
        }
      }
    }

    for (int i = 0; i < 5; i++) {
      try (final Writer writer =
               Files.newBufferedWriter(new File(inputDir, "filtered_" + i).toPath(), StandardCharsets.UTF_8)) {
        writer.write(StringUtils.format("2017-12-%d,%d,%d th test file\n", i + 1, i + 10, i));
      }
    }
  }

  @Test
  public void testRun() throws Exception
  {
    final Set<DataSegment> publishedSegments = runTestTask(
        PARSE_SPEC,
        Intervals.of("2017/2018"),
        inputDir,
        "test_*",
        new HashedPartitionsSpec(null, 2, ImmutableList.of("dim1", "dim2")),
        MAX_NUM_CONCURRENT_SUB_TASKS
    );
    assertHashedPartition(publishedSegments);
  }

  private void assertHashedPartition(Set<DataSegment> publishedSegments) throws IOException
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    publishedSegments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );
    final File tempSegmentDir = temporaryFolder.newFolder();
    for (List<DataSegment> segmentsInInterval : intervalToSegments.values()) {
      Assert.assertEquals(2, segmentsInInterval.size());
      for (DataSegment segment : segmentsInInterval) {
        List<ScanResultValue> results = querySegment(segment, ImmutableList.of("dim1", "dim2"), tempSegmentDir);
        final int hash = HashBasedNumberedShardSpec.hash(getObjectMapper(), (List<Object>) results.get(0).getEvents());
        for (ScanResultValue value : results) {
          Assert.assertEquals(
              hash,
              HashBasedNumberedShardSpec.hash(getObjectMapper(), (List<Object>) value.getEvents())
          );
        }
      }
    }
  }

  @Override
  ParallelIndexSupervisorTask createParallelIndexSupervisorTask(
      String id,
      TaskResource taskResource,
      ParallelIndexIngestionSpec ingestionSchema,
      Map<String, Object> context,
      IndexingServiceClient indexingServiceClient
  )
  {
    return new TestSupervisorTask(id, taskResource, ingestionSchema, context, indexingServiceClient);
  }

  private static class TestSupervisorTask extends TestParallelIndexSupervisorTask
  {
    TestSupervisorTask(
        String id,
        TaskResource taskResource,
        ParallelIndexIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(id, taskResource, ingestionSchema, context, indexingServiceClient);
    }

    @Override
    public PartialHashSegmentGenerateParallelIndexTaskRunner createPartialHashSegmentGenerateRunner(TaskToolbox toolbox)
    {
      return new TestPartialHashSegmentGenerateRunner(toolbox, this, getIndexingServiceClient());
    }

    @Override
    public PartialHashSegmentMergeParallelIndexTaskRunner createPartialHashSegmentMergeRunner(
        TaskToolbox toolbox,
        List<PartialHashSegmentMergeIOConfig> ioConfigs
    )
    {
      return new TestPartialHashSegmentMergeParallelIndexTaskRunner(
          toolbox,
          this,
          ioConfigs,
          getIndexingServiceClient()
      );
    }
  }

  private static class TestPartialHashSegmentGenerateRunner extends PartialHashSegmentGenerateParallelIndexTaskRunner
  {
    private TestPartialHashSegmentGenerateRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema(),
          supervisorTask.getContext(),
          indexingServiceClient,
          new LocalParallelIndexTaskClientFactory(supervisorTask),
          new TestAppenderatorsManager()
      );
    }
  }

  private static class TestPartialHashSegmentMergeParallelIndexTaskRunner
      extends PartialHashSegmentMergeParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    private TestPartialHashSegmentMergeParallelIndexTaskRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        List<PartialHashSegmentMergeIOConfig> mergeIOConfigs,
        IndexingServiceClient indexingServiceClient
    )
    {
      super(
          toolbox,
          supervisorTask.getId(),
          supervisorTask.getGroupId(),
          supervisorTask.getIngestionSchema().getDataSchema(),
          mergeIOConfigs,
          supervisorTask.getIngestionSchema().getTuningConfig(),
          supervisorTask.getContext(),
          indexingServiceClient
      );
      this.supervisorTask = supervisorTask;
    }

    @Override
    SubTaskSpec<PartialHashSegmentMergeTask> newTaskSpec(PartialHashSegmentMergeIOConfig ioConfig)
    {
      final PartialHashSegmentMergeIngestionSpec ingestionSpec =
          new PartialHashSegmentMergeIngestionSpec(
              supervisorTask.getIngestionSchema().getDataSchema(),
              ioConfig,
              getTuningConfig()
          );
      return new SubTaskSpec<PartialHashSegmentMergeTask>(
          getTaskId() + "_" + getAndIncrementNextSpecId(),
          getGroupId(),
          getTaskId(),
          getContext(),
          new InputSplit<>(ioConfig.getPartitionLocations())
      )
      {
        @Override
        public PartialHashSegmentMergeTask newSubTask(int numAttempts)
        {
          return new TestPartialHashSegmentMergeTask(
              null,
              getGroupId(),
              null,
              getSupervisorTaskId(),
              numAttempts,
              ingestionSpec,
              getContext(),
              getIndexingServiceClient(),
              new LocalParallelIndexTaskClientFactory(supervisorTask),
              getToolbox()
          );
        }
      };
    }
  }

  private static class TestPartialHashSegmentMergeTask extends PartialHashSegmentMergeTask
  {
    private final TaskToolbox toolbox;

    private TestPartialHashSegmentMergeTask(
        @Nullable String id,
        String groupId,
        TaskResource taskResource,
        String supervisorTaskId,
        int numAttempts,
        PartialHashSegmentMergeIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient,
        IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
        TaskToolbox toolbox
    )
    {
      super(
          id,
          groupId,
          taskResource,
          supervisorTaskId,
          numAttempts,
          ingestionSchema,
          context,
          indexingServiceClient,
          taskClientFactory,
          null
      );
      this.toolbox = toolbox;
    }

    @Override
    File fetchSegmentFile(File partitionDir, HashPartitionLocation location)
    {
      final File zippedFile = toolbox.getIntermediaryDataManager().findPartitionFile(
          getSupervisorTaskId(),
          location.getSubTaskId(),
          location.getInterval(),
          location.getPartitionId()
      );
      if (zippedFile == null) {
        throw new ISE("Can't find segment file for location[%s] at path[%s]", location);
      }
      return zippedFile;
    }
  }
}
