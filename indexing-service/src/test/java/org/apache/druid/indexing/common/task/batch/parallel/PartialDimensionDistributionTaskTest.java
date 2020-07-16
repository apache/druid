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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.apache.logging.log4j.core.LogEvent;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@RunWith(Enclosed.class)
public class PartialDimensionDistributionTaskTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();
  private static final SingleDimensionPartitionsSpec SINGLE_DIM_PARTITIONS_SPEC =
      new ParallelIndexTestingFactory.SingleDimensionPartitionsSpecBuilder().build();

  public static class ConstructorTest
  {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void requiresForceGuaranteedRollup()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("forceGuaranteedRollup must be set");

      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .forceGuaranteedRollup(false)
          .partitionsSpec(new DynamicPartitionsSpec(null, null))
          .build();

      new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();
    }

    @Test
    public void requiresSingleDimensionPartitions()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("single_dim partitionsSpec required");

      PartitionsSpec partitionsSpec = new HashedPartitionsSpec(null, 1, null);
      ParallelIndexTuningConfig tuningConfig =
          new ParallelIndexTestingFactory.TuningConfigBuilder().partitionsSpec(partitionsSpec).build();

      new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();
    }

    @Test
    public void requiresGranularitySpecInputIntervals()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("Missing intervals in granularitySpec");

      DataSchema dataSchema = ParallelIndexTestingFactory.createDataSchema(Collections.emptyList());

      new PartialDimensionDistributionTaskBuilder()
          .dataSchema(dataSchema)
          .build();
    }

    @Test
    public void serializesDeserializes()
    {
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .build();
      TestHelper.testSerializesDeserializes(OBJECT_MAPPER, task);
    }

    @Test
    public void hasCorrectPrefixForAutomaticId()
    {
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .id(ParallelIndexTestingFactory.AUTOMATIC_ID)
          .build();
      Assert.assertThat(task.getId(), Matchers.startsWith(PartialDimensionDistributionTask.TYPE));
    }
  }

  public static class RunTaskTest
  {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public LoggerCaptureRule logger = new LoggerCaptureRule(PartialDimensionDistributionTask.class);

    private TaskToolbox taskToolbox;

    @Before
    public void setup()
    {
      taskToolbox = EasyMock.mock(TaskToolbox.class);
      EasyMock.expect(taskToolbox.getIndexingTmpDir()).andStubReturn(temporaryFolder.getRoot());
      EasyMock.replay(taskToolbox);
    }

    @Test
    public void requiresPartitionDimension() throws Exception
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("partitionDimension must be specified");

      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(
              new ParallelIndexTestingFactory.SingleDimensionPartitionsSpecBuilder().partitionDimension(null).build()
          )
          .build();
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();

      task.runTask(taskToolbox);
    }

    @Test
    public void logsParseExceptionsIfEnabled() throws Exception
    {
      long invalidTimestamp = Long.MAX_VALUE;
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(invalidTimestamp, "a")
      );
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(SINGLE_DIM_PARTITIONS_SPEC)
          .logParseExceptions(true)
          .build();
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .inputSource(inlineInputSource)
          .tuningConfig(tuningConfig)
          .taskClientFactory(ParallelIndexTestingFactory.createTaskClientFactory())
          .build();

      task.runTask(taskToolbox);

      List<LogEvent> logEvents = logger.getLogEvents();
      Assert.assertEquals(1, logEvents.size());
      String logMessage = logEvents.get(0).getMessage().getFormattedMessage();
      Assert.assertThat(logMessage, Matchers.containsString("Encountered parse exception"));
    }

    @Test
    public void doesNotLogParseExceptionsIfDisabled() throws Exception
    {
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(SINGLE_DIM_PARTITIONS_SPEC)
          .logParseExceptions(false)
          .build();
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .taskClientFactory(ParallelIndexTestingFactory.createTaskClientFactory())
          .build();

      task.runTask(taskToolbox);

      Assert.assertEquals(Collections.emptyList(), logger.getLogEvents());
    }

    @Test
    public void failsWhenTooManyParseExceptions() throws Exception
    {
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(SINGLE_DIM_PARTITIONS_SPEC)
          .maxParseExceptions(0)
          .build();
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .taskClientFactory(ParallelIndexTestingFactory.createTaskClientFactory())
          .build();

      exception.expect(RuntimeException.class);
      exception.expectMessage("Max parse exceptions exceeded");

      task.runTask(taskToolbox);
    }

    @Test
    public void failsIfRowHasMultipleDimensionValues()
    {
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(0, Arrays.asList("a", "b"))
      );
      PartialDimensionDistributionTaskBuilder taskBuilder = new PartialDimensionDistributionTaskBuilder()
          .inputSource(inlineInputSource);

      exception.expect(RuntimeException.class);
      exception.expectMessage("Cannot partition on multi-value dimension [dim]");

      runTask(taskBuilder);
    }

    @Test
    public void sendsCorrectReportWhenAssumeGroupedTrue()
    {
      long timestamp = 0;
      String dimensionValue = "a";
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(timestamp, dimensionValue)
          + "\n" + ParallelIndexTestingFactory.createRow(timestamp + 1, dimensionValue)
      );
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(
              new ParallelIndexTestingFactory.SingleDimensionPartitionsSpecBuilder().assumeGrouped(true).build()
          )
          .build();
      PartialDimensionDistributionTaskBuilder taskBuilder = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .inputSource(inlineInputSource);

      DimensionDistributionReport report = runTask(taskBuilder);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, StringDistribution> intervalToDistribution = report.getIntervalToDistribution();
      StringDistribution distribution = Iterables.getOnlyElement(intervalToDistribution.values());
      Assert.assertNotNull(distribution);
      PartitionBoundaries partitions = distribution.getEvenPartitionsByMaxSize(1);
      Assert.assertEquals(2, partitions.size());
      Assert.assertNull(partitions.get(0));
      Assert.assertNull(partitions.get(1));
    }

    @Test
    public void groupsRowsWhenAssumeGroupedFalse()
    {
      long timestamp = 0;
      String dimensionValue = "a";
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(timestamp, dimensionValue)
          + "\n" + ParallelIndexTestingFactory.createRow(timestamp + 1, dimensionValue)
      );
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(
              new ParallelIndexTestingFactory.SingleDimensionPartitionsSpecBuilder().assumeGrouped(false).build()
          )
          .build();
      PartialDimensionDistributionTaskBuilder taskBuilder = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .inputSource(inlineInputSource);

      DimensionDistributionReport report = runTask(taskBuilder);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, StringDistribution> intervalToDistribution = report.getIntervalToDistribution();
      StringDistribution distribution = Iterables.getOnlyElement(intervalToDistribution.values());
      Assert.assertNotNull(distribution);
      PartitionBoundaries partitions = distribution.getEvenPartitionsByMaxSize(1);
      Assert.assertEquals(2, partitions.size());
      Assert.assertNull(partitions.get(0));
      Assert.assertNull(partitions.get(1));
    }

    @Test
    public void preservesMinAndMaxWhenAssumeGroupedFalse()
    {
      // Create a small bloom filter so that it saturates quickly
      int smallBloomFilter = 1;
      double manyFalsePositiveBloomFilter = 0.5;
      int minBloomFilterBits = Long.SIZE;

      long timestamp = 0;
      List<String> dimensionValues = IntStream.range(0, minBloomFilterBits * 10)
                                              .mapToObj(i -> StringUtils.format("%010d", i))
                                              .collect(Collectors.toCollection(ArrayList::new));
      List<String> rows = dimensionValues.stream()
                                         .map(d -> ParallelIndexTestingFactory.createRow(timestamp, d))
                                         .collect(Collectors.toList());
      Joiner joiner = Joiner.on("\n");
      InputSource inlineInputSource = new InlineInputSource(
          joiner.join(
              joiner.join(rows.subList(1, rows.size())),  // saturate bloom filter first
              rows.get(0),
              rows.get(rows.size() - 1)
          )
      );
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(
              new ParallelIndexTestingFactory.SingleDimensionPartitionsSpecBuilder().assumeGrouped(false).build()
          )
          .build();
      DataSchema dataSchema = ParallelIndexTestingFactory.createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS);
      PartialDimensionDistributionTaskBuilder taskBuilder = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .dataSchema(dataSchema)
          .inputSource(inlineInputSource)
          .dedupRowDimValueFilterSupplier(
              () -> new PartialDimensionDistributionTask.DedupInputRowFilter(
                  dataSchema.getGranularitySpec().getQueryGranularity(),
                  smallBloomFilter,
                  manyFalsePositiveBloomFilter
              )
          );

      DimensionDistributionReport report = runTask(taskBuilder);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, StringDistribution> intervalToDistribution = report.getIntervalToDistribution();
      StringDistribution distribution = Iterables.getOnlyElement(intervalToDistribution.values());
      Assert.assertNotNull(distribution);
      PartitionBoundaries partitions = distribution.getEvenPartitionsByMaxSize(1);
      Assert.assertEquals(minBloomFilterBits + 2, partitions.size()); // 2 = min + max

      String minDimensionValue = dimensionValues.get(0);
      Assert.assertEquals(minDimensionValue, ((StringSketch) distribution).getMin());

      String maxDimensionValue = dimensionValues.get(dimensionValues.size() - 1);
      Assert.assertEquals(maxDimensionValue, ((StringSketch) distribution).getMax());
    }

    @Test
    public void returnsSuccessIfNoExceptions() throws Exception
    {
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .taskClientFactory(ParallelIndexTestingFactory.createTaskClientFactory())
          .build();

      TaskStatus taskStatus = task.runTask(taskToolbox);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, taskStatus.getId());
      Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
    }

    private DimensionDistributionReport runTask(PartialDimensionDistributionTaskBuilder taskBuilder)
    {
      Capture<SubTaskReport> reportCapture = Capture.newInstance();
      ParallelIndexSupervisorTaskClient taskClient = EasyMock.mock(ParallelIndexSupervisorTaskClient.class);
      taskClient.report(EasyMock.eq(ParallelIndexTestingFactory.SUPERVISOR_TASK_ID), EasyMock.capture(reportCapture));
      EasyMock.replay(taskClient);

      try {
        taskBuilder.taskClientFactory((taskInfoProvider, callerId, numThreads, httpTimeout, numRetries) -> taskClient)
                   .build()
                   .runTask(taskToolbox);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return (DimensionDistributionReport) reportCapture.getValue();
    }
  }

  private static class PartialDimensionDistributionTaskBuilder
  {
    private static final InputFormat INPUT_FORMAT = ParallelIndexTestingFactory.getInputFormat();

    private String id = ParallelIndexTestingFactory.ID;
    private InputSource inputSource = new InlineInputSource("row-with-invalid-timestamp");
    private ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
        .partitionsSpec(new ParallelIndexTestingFactory.SingleDimensionPartitionsSpecBuilder().build())
        .build();
    private DataSchema dataSchema =
        ParallelIndexTestingFactory.createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS);
    private IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory =
        ParallelIndexTestingFactory.TASK_CLIENT_FACTORY;
    private Supplier<PartialDimensionDistributionTask.DedupInputRowFilter> dedupRowDimValueFilterSupplier = null;

    @SuppressWarnings("SameParameterValue")
    PartialDimensionDistributionTaskBuilder id(String id)
    {
      this.id = id;
      return this;
    }

    PartialDimensionDistributionTaskBuilder inputSource(InputSource inputSource)
    {
      this.inputSource = inputSource;
      return this;
    }

    PartialDimensionDistributionTaskBuilder tuningConfig(ParallelIndexTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    PartialDimensionDistributionTaskBuilder dataSchema(DataSchema dataSchema)
    {
      this.dataSchema = dataSchema;
      return this;
    }

    PartialDimensionDistributionTaskBuilder taskClientFactory(
        IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> taskClientFactory
    )
    {
      this.taskClientFactory = taskClientFactory;
      return this;
    }

    PartialDimensionDistributionTaskBuilder dedupRowDimValueFilterSupplier(
        Supplier<PartialDimensionDistributionTask.DedupInputRowFilter> dedupRowDimValueFilterSupplier
    )
    {
      this.dedupRowDimValueFilterSupplier = dedupRowDimValueFilterSupplier;
      return this;
    }

    PartialDimensionDistributionTask build()
    {
      ParallelIndexIngestionSpec ingestionSpec =
          ParallelIndexTestingFactory.createIngestionSpec(inputSource, INPUT_FORMAT, tuningConfig, dataSchema);

      Supplier<PartialDimensionDistributionTask.DedupInputRowFilter> supplier =
          dedupRowDimValueFilterSupplier == null
          ? () -> new PartialDimensionDistributionTask.DedupInputRowFilter(
              dataSchema.getGranularitySpec().getQueryGranularity()
          )
          : dedupRowDimValueFilterSupplier;

      return new PartialDimensionDistributionTask(
          id,
          ParallelIndexTestingFactory.GROUP_ID,
          ParallelIndexTestingFactory.TASK_RESOURCE,
          ParallelIndexTestingFactory.SUPERVISOR_TASK_ID,
          ParallelIndexTestingFactory.NUM_ATTEMPTS,
          ingestionSpec,
          ParallelIndexTestingFactory.CONTEXT,
          ParallelIndexTestingFactory.INDEXING_SERVICE_CLIENT,
          taskClientFactory,
          supplier
      );
    }
  }
}
