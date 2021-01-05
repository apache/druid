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

import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.input.DruidInputSource;
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
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.firehose.LocalFirehoseFactory;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("SameParameterValue")
abstract class AbstractMultiPhaseParallelIndexingTest extends AbstractParallelIndexSupervisorTaskTest
{
  protected static final String DATASOURCE = "dataSource";
  protected static final Granularity SEGMENT_GRANULARITY = Granularities.DAY;

  private static final ScanQueryRunnerFactory SCAN_QUERY_RUNNER_FACTORY = new ScanQueryRunnerFactory(
      new ScanQueryQueryToolChest(
          new ScanQueryConfig().setLegacy(false),
          DefaultGenericQueryMetricsFactory.instance()
      ),
      new ScanQueryEngine(),
      new ScanQueryConfig()
  );

  private final LockGranularity lockGranularity;
  private final boolean useInputFormatApi;

  AbstractMultiPhaseParallelIndexingTest(LockGranularity lockGranularity, boolean useInputFormatApi)
  {
    this.lockGranularity = lockGranularity;
    this.useInputFormatApi = useInputFormatApi;
    getObjectMapper().registerSubtypes(ParallelIndexTuningConfig.class, DruidInputSource.class);
  }

  boolean isUseInputFormatApi()
  {
    return useInputFormatApi;
  }

  Set<DataSegment> runTestTask(
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable InputFormat inputFormat,
      @Nullable ParseSpec parseSpec,
      Interval interval,
      File inputDir,
      String filter,
      PartitionsSpec partitionsSpec,
      int maxNumConcurrentSubTasks,
      TaskState expectedTaskStatus
  )
  {
    return runTestTask(
        timestampSpec,
        dimensionsSpec,
        inputFormat,
        parseSpec,
        interval,
        inputDir,
        filter,
        partitionsSpec,
        maxNumConcurrentSubTasks,
        expectedTaskStatus,
        false
    );
  }

  Set<DataSegment> runTestTask(
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable InputFormat inputFormat,
      @Nullable ParseSpec parseSpec,
      Interval interval,
      File inputDir,
      String filter,
      PartitionsSpec partitionsSpec,
      int maxNumConcurrentSubTasks,
      TaskState expectedTaskStatus,
      boolean appendToExisting
  )
  {
    final ParallelIndexSupervisorTask task = newTask(
        timestampSpec,
        dimensionsSpec,
        inputFormat,
        parseSpec,
        interval,
        inputDir,
        filter,
        partitionsSpec,
        maxNumConcurrentSubTasks,
        appendToExisting
    );

    return runTask(task, expectedTaskStatus);
  }

  Set<DataSegment> runTask(Task task, TaskState expectedTaskStatus)
  {
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    TaskStatus taskStatus = getIndexingServiceClient().runAndWait(task);
    Assert.assertEquals(expectedTaskStatus, taskStatus.getStatusCode());
    return getIndexingServiceClient().getPublishedSegments(task);
  }

  private ParallelIndexSupervisorTask newTask(
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable InputFormat inputFormat,
      @Nullable ParseSpec parseSpec,
      Interval interval,
      File inputDir,
      String filter,
      PartitionsSpec partitionsSpec,
      int maxNumConcurrentSubTasks,
      boolean appendToExisting
  )
  {
    GranularitySpec granularitySpec = new UniformGranularitySpec(
        SEGMENT_GRANULARITY,
        Granularities.MINUTE,
        interval == null ? null : Collections.singletonList(interval)
    );

    ParallelIndexTuningConfig tuningConfig = newTuningConfig(
        partitionsSpec,
        maxNumConcurrentSubTasks,
        !appendToExisting
    );

    final ParallelIndexIngestionSpec ingestionSpec;

    if (useInputFormatApi) {
      Preconditions.checkArgument(parseSpec == null);
      ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          null,
          new LocalInputSource(inputDir, filter),
          inputFormat,
          appendToExisting
      );
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              DATASOURCE,
              timestampSpec,
              dimensionsSpec,
              new AggregatorFactory[]{new LongSumAggregatorFactory("val", "val")},
              granularitySpec,
              null
          ),
          ioConfig,
          tuningConfig
      );
    } else {
      Preconditions.checkArgument(inputFormat == null);
      ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          new LocalFirehoseFactory(inputDir, filter, null),
          appendToExisting
      );
      //noinspection unchecked
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              "dataSource",
              getObjectMapper().convertValue(
                  new StringInputRowParser(parseSpec, null),
                  Map.class
              ),
              new AggregatorFactory[]{
                  new LongSumAggregatorFactory("val", "val")
              },
              granularitySpec,
              null,
              getObjectMapper()
          ),
          ioConfig,
          tuningConfig
      );
    }

    // set up test tools
    return new ParallelIndexSupervisorTask(
        null,
        null,
        null,
        ingestionSpec,
        Collections.emptyMap()
    );
  }

  List<ScanResultValue> querySegment(DataSegment dataSegment, List<String> columns, File tempSegmentDir)
  {
    Segment segment = loadSegment(dataSegment, tempSegmentDir);
    final QueryRunner<ScanResultValue> runner = SCAN_QUERY_RUNNER_FACTORY.createRunner(segment);
    return runner.run(
        QueryPlus.wrap(
            new ScanQuery(
                new TableDataSource("dataSource"),
                new SpecificSegmentSpec(
                    new SegmentDescriptor(
                        dataSegment.getInterval(),
                        dataSegment.getVersion(),
                        dataSegment.getShardSpec().getPartitionNum()
                    )
                ),
                null,
                null,
                0,
                0,
                0,
                null,
                null,
                columns,
                false,
                null
            )
        )
    ).toList();
  }

  private Segment loadSegment(DataSegment dataSegment, File tempSegmentDir)
  {
    final SegmentLoader loader = new SegmentLoaderFactory(getIndexIO(), getObjectMapper())
        .manufacturate(tempSegmentDir);
    try {
      return loader.getSegment(dataSegment, false);
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }
}
