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
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(Parameterized.class)
public class MultiPhaseParallelIndexingTest extends AbstractParallelIndexSupervisorTaskTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK},
        new Object[]{LockGranularity.SEGMENT}
    );
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final LockGranularity lockGranularity;
  private File inputDir;

  public MultiPhaseParallelIndexingTest(LockGranularity lockGranularity)
  {
    this.lockGranularity = lockGranularity;
  }

  @Before
  public void setup() throws IOException
  {
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

    indexingServiceClient = new LocalIndexingServiceClient();
    localDeepStorage = temporaryFolder.newFolder("localStorage");
    initializeIntermeidaryDataManager();
  }

  @After
  public void teardown()
  {
    indexingServiceClient.shutdown();
    temporaryFolder.delete();
  }

  @Test
  public void testRun() throws Exception
  {
    final Set<DataSegment> publishedSegments = runTestTask(
        Intervals.of("2017/2018"),
        new HashedPartitionsSpec(null, 2, ImmutableList.of("dim1", "dim2"))
    );
    assertHashedPartition(publishedSegments);
  }

  private Set<DataSegment> runTestTask(Interval interval, HashedPartitionsSpec partitionsSpec) throws Exception
  {
    final ParallelIndexSupervisorTask task = newTask(
        interval,
        new ParallelIndexIOConfig(
            new LocalFirehoseFactory(inputDir, "test_*", null),
            false
        ),
        partitionsSpec
    );
    actionClient = createActionClient(task);
    toolbox = createTaskToolbox(task);

    prepareTaskForLocking(task);
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    Assert.assertTrue(task.isReady(actionClient));
    Assert.assertEquals(TaskState.SUCCESS, task.run(toolbox).getStatusCode());
    shutdownTask(task);
    return actionClient.getPublishedSegments();
  }

  private ParallelIndexSupervisorTask newTask(
      Interval interval,
      ParallelIndexIOConfig ioConfig,
      HashedPartitionsSpec partitionsSpec
  )
  {
    return newTask(
        interval,
        Granularities.DAY,
        ioConfig,
        new ParallelIndexTuningConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            partitionsSpec,
            null,
            null,
            null,
            true,
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
        )
    );
  }

  private ParallelIndexSupervisorTask newTask(
      Interval interval,
      Granularity segmentGranularity,
      ParallelIndexIOConfig ioConfig,
      ParallelIndexTuningConfig tuningConfig
  )
  {
    // set up ingestion spec
    final ParseSpec parseSpec = new CSVParseSpec(
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

    //noinspection unchecked
    final ParallelIndexIngestionSpec ingestionSpec = new ParallelIndexIngestionSpec(
        new DataSchema(
            "dataSource",
            getObjectMapper().convertValue(
                new StringInputRowParser(
                    parseSpec,
                    null
                ),
                Map.class
            ),
            new AggregatorFactory[]{
                new LongSumAggregatorFactory("val", "val")
            },
            new UniformGranularitySpec(
                segmentGranularity,
                Granularities.MINUTE,
                interval == null ? null : Collections.singletonList(interval)
            ),
            null,
            getObjectMapper()
        ),
        ioConfig,
        tuningConfig
    );

    // set up test tools
    return new TestSupervisorTask(
        null,
        null,
        ingestionSpec,
        new HashMap<>(),
        indexingServiceClient
    );
  }

  private void assertHashedPartition(Set<DataSegment> publishedSegments) throws IOException, SegmentLoadingException
  {
    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    publishedSegments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );
    final File tempSegmentDir = temporaryFolder.newFolder();
    for (List<DataSegment> segmentsInInterval : intervalToSegments.values()) {
      Assert.assertEquals(2, segmentsInInterval.size());
      for (DataSegment segment : segmentsInInterval) {
        final SegmentLoader loader = new SegmentLoaderFactory(getIndexIO(), getObjectMapper())
            .manufacturate(tempSegmentDir);
        ScanQueryRunnerFactory factory = new ScanQueryRunnerFactory(
            new ScanQueryQueryToolChest(
                new ScanQueryConfig().setLegacy(false),
                DefaultGenericQueryMetricsFactory.instance()
            ),
            new ScanQueryEngine(),
            new ScanQueryConfig()
        );
        final QueryRunner<ScanResultValue> runner = factory.createRunner(loader.getSegment(segment));
        final List<ScanResultValue> results = runner.run(
            QueryPlus.wrap(
                new ScanQuery(
                    new TableDataSource("dataSource"),
                    new SpecificSegmentSpec(
                        new SegmentDescriptor(
                            segment.getInterval(),
                            segment.getVersion(),
                            segment.getShardSpec().getPartitionNum()
                        )
                    ),
                    null,
                    null,
                    0,
                    0,
                    null,
                    null,
                    ImmutableList.of("dim1", "dim2"),
                    false,
                    null
                )
            )
        ).toList();
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
    public PartialSegmentGenerateParallelIndexTaskRunner createPartialSegmentGenerateRunner(TaskToolbox toolbox)
    {
      return new TestPartialSegmentGenerateRunner(toolbox, this, getIndexingServiceClient());
    }

    @Override
    public PartialSegmentMergeParallelIndexTaskRunner createPartialSegmentMergeRunner(
        TaskToolbox toolbox,
        List<PartialSegmentMergeIOConfig> ioConfigs
    )
    {
      return new TestPartialSegmentMergeParallelIndexTaskRunner(toolbox, this, ioConfigs, getIndexingServiceClient());
    }
  }

  private static class TestPartialSegmentGenerateRunner extends PartialSegmentGenerateParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    private TestPartialSegmentGenerateRunner(
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
          indexingServiceClient
      );
      this.supervisorTask = supervisorTask;
    }

    @Override
    Iterator<SubTaskSpec<PartialSegmentGenerateTask>> subTaskSpecIterator() throws IOException
    {
      final Iterator<SubTaskSpec<PartialSegmentGenerateTask>> iterator = super.subTaskSpecIterator();
      return new Iterator<SubTaskSpec<PartialSegmentGenerateTask>>()
      {
        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public SubTaskSpec<PartialSegmentGenerateTask> next()
        {
          try {
            Thread.sleep(10);
            return iterator.next();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    @Override
    SubTaskSpec<PartialSegmentGenerateTask> newTaskSpec(InputSplit split)
    {
      final ParallelIndexIngestionSpec subTaskIngestionSpec = new ParallelIndexIngestionSpec(
          getIngestionSchema().getDataSchema(),
          new ParallelIndexIOConfig(
              getBaseFirehoseFactory().withSplit(split),
              getIngestionSchema().getIOConfig().isAppendToExisting()
          ),
          getIngestionSchema().getTuningConfig()
      );
      return new SubTaskSpec<PartialSegmentGenerateTask>(
          getTaskId() + "_" + getAndIncrementNextSpecId(),
          getGroupId(),
          getTaskId(),
          getContext(),
          split
      )
      {
        @Override
        public PartialSegmentGenerateTask newSubTask(int numAttempts)
        {
          return new PartialSegmentGenerateTask(
              null,
              getGroupId(),
              null,
              getSupervisorTaskId(),
              numAttempts,
              subTaskIngestionSpec,
              getContext(),
              getIndexingServiceClient(),
              new LocalParallelIndexTaskClientFactory(supervisorTask),
              new TestAppenderatorsManager()
          );
        }
      };
    }
  }

  private static class TestPartialSegmentMergeParallelIndexTaskRunner extends PartialSegmentMergeParallelIndexTaskRunner
  {
    private final ParallelIndexSupervisorTask supervisorTask;

    private TestPartialSegmentMergeParallelIndexTaskRunner(
        TaskToolbox toolbox,
        ParallelIndexSupervisorTask supervisorTask,
        List<PartialSegmentMergeIOConfig> mergeIOConfigs,
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
    Iterator<SubTaskSpec<PartialSegmentMergeTask>> subTaskSpecIterator()
    {
      final Iterator<SubTaskSpec<PartialSegmentMergeTask>> iterator = super.subTaskSpecIterator();
      return new Iterator<SubTaskSpec<PartialSegmentMergeTask>>()
      {
        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public SubTaskSpec<PartialSegmentMergeTask> next()
        {
          try {
            Thread.sleep(10);
            return iterator.next();
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    @Override
    SubTaskSpec<PartialSegmentMergeTask> newTaskSpec(PartialSegmentMergeIOConfig ioConfig)
    {
      final PartialSegmentMergeIngestionSpec ingestionSpec = new PartialSegmentMergeIngestionSpec(
          supervisorTask.getIngestionSchema().getDataSchema(),
          ioConfig,
          getTuningConfig()
      );
      return new SubTaskSpec<PartialSegmentMergeTask>(
          getTaskId() + "_" + getAndIncrementNextSpecId(),
          getGroupId(),
          getTaskId(),
          getContext(),
          new InputSplit<>(ioConfig.getPartitionLocations())
      )
      {
        @Override
        public PartialSegmentMergeTask newSubTask(int numAttempts)
        {
          return new TestPartialSegmentMergeTask(
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

  private static class TestPartialSegmentMergeTask extends PartialSegmentMergeTask
  {
    private final TaskToolbox toolbox;

    private TestPartialSegmentMergeTask(
        @Nullable String id,
        String groupId,
        TaskResource taskResource,
        String supervisorTaskId,
        int numAttempts,
        PartialSegmentMergeIngestionSpec ingestionSchema,
        Map<String, Object> context,
        IndexingServiceClient indexingServiceClient,
        IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory,
        TaskToolbox toolboxo
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
      this.toolbox = toolboxo;
    }

    @Override
    File fetchSegmentFile(File partitionDir, PartitionLocation location)
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
