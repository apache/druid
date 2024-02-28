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
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
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
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheLoader;
import org.apache.druid.segment.loading.TombstoneLoadSpec;
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

  AbstractMultiPhaseParallelIndexingTest(
      LockGranularity lockGranularity,
      boolean useInputFormatApi,
      double transientTaskFailureRate,
      double transientApiCallFailureRate
  )
  {
    super(transientTaskFailureRate, transientApiCallFailureRate);
    this.lockGranularity = lockGranularity;
    this.useInputFormatApi = useInputFormatApi;
    getObjectMapper().registerSubtypes(
        ParallelIndexTuningConfig.class,
        DruidInputSource.class,
        TombstoneLoadSpec.class
    );
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
        false,
        false
    );
  }

  Set<DataSegment> runTestTask(
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable InputFormat inputFormat,
      @Nullable ParseSpec parseSpec,
      Interval interval,
      File inputDirectory,
      String filter,
      PartitionsSpec partitionsSpec,
      int maxNumConcurrentSubTasks,
      TaskState expectedTaskStatus,
      boolean appendToExisting,
      boolean dropExisting
  )
  {
    final ParallelIndexSupervisorTask task = createTask(
        timestampSpec,
        dimensionsSpec,
        inputFormat,
        parseSpec,
        interval,
        inputDirectory,
        filter,
        partitionsSpec,
        maxNumConcurrentSubTasks,
        appendToExisting,
        dropExisting
    );

    return runTask(task, expectedTaskStatus);
  }

  void runTaskAndVerifyStatus(Task task, TaskState expectedTaskStatus)
  {
    task.addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockGranularity == LockGranularity.TIME_CHUNK);
    TaskStatus taskStatus = getIndexingServiceClient().runAndWait(task);
    Assert.assertEquals("Actual task status: " + taskStatus, expectedTaskStatus, taskStatus.getStatusCode());
  }

  Set<DataSegment> runTask(Task task, TaskState expectedTaskStatus)
  {
    runTaskAndVerifyStatus(task, expectedTaskStatus);
    return getIndexingServiceClient().getPublishedSegments(task);
  }

  Map<String, Object> runTaskAndGetReports(Task task, TaskState expectedTaskStatus)
  {
    runTaskAndVerifyStatus(task, expectedTaskStatus);
    return FutureUtils.getUnchecked(getIndexingServiceClient().taskReportAsMap(task.getId()), true);
  }

  protected ParallelIndexSupervisorTask createTask(
      @Nullable TimestampSpec timestampSpec,
      @Nullable DimensionsSpec dimensionsSpec,
      @Nullable InputFormat inputFormat,
      @Nullable ParseSpec parseSpec,
      Interval interval,
      File inputDirectory,
      String filter,
      PartitionsSpec partitionsSpec,
      int maxNumConcurrentSubTasks,
      boolean appendToExisting,
      boolean dropExisting
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
          new LocalInputSource(inputDirectory, filter),
          inputFormat,
          appendToExisting,
          dropExisting
      );
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              DATASOURCE,
              timestampSpec,
              dimensionsSpec,
              DEFAULT_METRICS_SPEC,
              granularitySpec,
              null
          ),
          ioConfig,
          tuningConfig
      );
    } else {
      Preconditions.checkArgument(inputFormat == null && parseSpec != null);
      ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
          null,
          new LocalInputSource(inputDirectory, filter),
          createInputFormatFromParseSpec(parseSpec),
          appendToExisting,
          dropExisting
      );
      ingestionSpec = new ParallelIndexIngestionSpec(
          new DataSchema(
              DATASOURCE,
              parseSpec.getTimestampSpec(),
              parseSpec.getDimensionsSpec(),
              DEFAULT_METRICS_SPEC,
              granularitySpec,
              null
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
                null,
                columns,
                false,
                null,
                null
            )
        )
    ).toList();
  }

  private Segment loadSegment(DataSegment dataSegment, File tempSegmentDir)
  {
    final SegmentCacheManager cacheManager = new SegmentCacheManagerFactory(getObjectMapper())
        .manufacturate(tempSegmentDir);
    final SegmentLoader loader = new SegmentLocalCacheLoader(cacheManager, getIndexIO(), getObjectMapper());
    try {
      return loader.getSegment(dataSegment, false, SegmentLazyLoadFailCallback.NOOP);
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
  }
}
