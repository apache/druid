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

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.TuningConfigBuilder;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringSketch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.testing.junit.LoggerCaptureExtension;
import org.apache.druid.timeline.partition.PartitionBoundaries;
import org.apache.logging.log4j.core.LogEvent;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartialDimensionDistributionTaskTest
{
  private static final SingleDimensionPartitionsSpec SINGLE_DIM_PARTITIONS_SPEC =
      new SingleDimensionPartitionsSpec(null, 1000, "dim", false);

  public static class ConstructorTest
  {
    @Test
    public void requiresForceGuaranteedRollup()
    {
      ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
          .forParallelIndexTask()
          .withForceGuaranteedRollup(false)
          .build();

      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> new PartialDimensionDistributionTaskBuilder()
              .tuningConfig(tuningConfig)
              .build()
      );
      Assertions.assertTrue(exception.getMessage().contains("forceGuaranteedRollup must be set"));
    }

    @Test
    public void requiresMultiDimensionPartitions()
    {
      ParallelIndexTuningConfig tuningConfig =
          TuningConfigBuilder.forParallelIndexTask()
                             .withForceGuaranteedRollup(true)
                             .withPartitionsSpec(new HashedPartitionsSpec(null, 1, null))
                             .build();

      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> new PartialDimensionDistributionTaskBuilder()
              .tuningConfig(tuningConfig)
              .build()
      );
      Assertions.assertTrue(exception.getMessage().contains("range partitionsSpec required"));
    }

    @Test
    public void hasCorrectPrefixForAutomaticId()
    {
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .id(ParallelIndexTestingFactory.AUTOMATIC_ID)
          .build();
      Assertions.assertTrue(task.getId().startsWith(PartialDimensionDistributionTask.TYPE));
    }
  }

  public static class RunTaskTest
  {
    @RegisterExtension
    private final TemporaryFolderExtension temporaryFolderExtension = new TemporaryFolderExtension();
    @RegisterExtension
    private final LoggerCaptureExtension logger = new LoggerCaptureExtension(ParseExceptionHandler.class);

    private Capture<SubTaskReport> reportCapture;
    private TaskToolbox taskToolbox;

    @BeforeEach
    public void setup()
    {
      reportCapture = Capture.newInstance();
      ParallelIndexSupervisorTaskClient taskClient = EasyMock.mock(ParallelIndexSupervisorTaskClient.class);
      taskClient.report(EasyMock.capture(reportCapture));
      EasyMock.replay(taskClient);
      taskToolbox = EasyMock.mock(TaskToolbox.class);
      EasyMock.expect(taskToolbox.getIndexingTmpDir()).andStubReturn(temporaryFolderExtension.getRoot());
      EasyMock.expect(taskToolbox.getSupervisorTaskClientProvider())
              .andReturn((supervisorTaskId, httpTimeout, numRetries) -> taskClient);
      EasyMock.expect(taskToolbox.getOverlordClient()).andReturn(null);
      EasyMock.expect(taskToolbox.getRowIngestionMetersFactory()).andReturn(new DropwizardRowIngestionMetersFactory());
      EasyMock.replay(taskToolbox);
    }

    @Test
    public void requiresPartitionDimensions() throws Exception
    {
      final IllegalArgumentException exception = Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> {
            ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
                .forParallelIndexTask()
                .withForceGuaranteedRollup(true)
                .withPartitionsSpec(new DimensionRangePartitionsSpec(null, null, null, false))
                .build();
            PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
                .tuningConfig(tuningConfig)
                .build();
            task.runTask(taskToolbox);
          }
      );
      Assertions.assertTrue(exception.getMessage().contains("partitionDimensions must be specified"));
    }

    @Test
    public void logsParseExceptionsIfEnabled() throws Exception
    {
      long invalidTimestamp = Long.MAX_VALUE;
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(invalidTimestamp, "a")
      );
      ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
          .forParallelIndexTask()
          .withForceGuaranteedRollup(true)
          .withPartitionsSpec(SINGLE_DIM_PARTITIONS_SPEC)
          .withLogParseExceptions(true)
          .build();
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .inputSource(inlineInputSource)
          .tuningConfig(tuningConfig)
          .build();

      task.runTask(taskToolbox);

      List<LogEvent> logEvents = logger.getLogEvents();
      Assertions.assertEquals(1, logEvents.size());
      String logMessage = logEvents.get(0).getMessage().getFormattedMessage();
      Assertions.assertTrue(logMessage.contains("Encountered parse exception"));
    }

    @Test
    public void doesNotLogParseExceptionsIfDisabled() throws Exception
    {
      ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
          .forParallelIndexTask()
          .withForceGuaranteedRollup(true)
          .withPartitionsSpec(SINGLE_DIM_PARTITIONS_SPEC)
          .withLogParseExceptions(false)
          .build();
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();

      task.runTask(taskToolbox);

      Assertions.assertEquals(Collections.emptyList(), logger.getLogEvents());
    }

    @Test
    public void failsWhenTooManyParseExceptions() throws Exception
    {
      ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
          .forParallelIndexTask()
          .withForceGuaranteedRollup(true)
          .withPartitionsSpec(SINGLE_DIM_PARTITIONS_SPEC)
          .withMaxParseExceptions(0)
          .build();
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();

      final RuntimeException exception = Assertions.assertThrows(
          RuntimeException.class,
          () -> task.runTask(taskToolbox)
      );
      Assertions.assertTrue(exception.getMessage().contains("Max parse exceptions[0] exceeded"));
    }

    @Test
    public void failsIfRowHasMultipleDimensionValues()
    {
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(0, Arrays.asList("a", "b"))
      );
      PartialDimensionDistributionTaskBuilder taskBuilder = new PartialDimensionDistributionTaskBuilder()
          .inputSource(inlineInputSource);

      final RuntimeException exception = Assertions.assertThrows(
          RuntimeException.class,
          () -> runTask(taskBuilder)
      );
      Assertions.assertTrue(exception.getMessage().contains("Cannot partition on multi-value dimension [dim]"));
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
      ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
          .forParallelIndexTask()
          .withForceGuaranteedRollup(true)
          .withPartitionsSpec(
              new DimensionRangePartitionsSpec(null, 1000, Collections.singletonList("dim1"), true)
          )
          .build();
      PartialDimensionDistributionTaskBuilder taskBuilder = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .inputSource(inlineInputSource);

      DimensionDistributionReport report = runTask(taskBuilder);

      Assertions.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, StringDistribution> intervalToDistribution = report.getIntervalToDistribution();
      StringDistribution distribution = Iterables.getOnlyElement(intervalToDistribution.values());
      Assertions.assertNotNull(distribution);
      PartitionBoundaries partitions = distribution.getEvenPartitionsByMaxSize(1);
      Assertions.assertEquals(2, partitions.size());
      Assertions.assertNull(partitions.get(0));
      Assertions.assertNull(partitions.get(1));
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
      ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
          .forParallelIndexTask()
          .withForceGuaranteedRollup(true)
          .withPartitionsSpec(
              new DimensionRangePartitionsSpec(null, 1000, Collections.singletonList("dim1"), false)
          )
          .build();
      PartialDimensionDistributionTaskBuilder taskBuilder = new PartialDimensionDistributionTaskBuilder()
          .tuningConfig(tuningConfig)
          .inputSource(inlineInputSource);

      DimensionDistributionReport report = runTask(taskBuilder);

      Assertions.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, StringDistribution> intervalToDistribution = report.getIntervalToDistribution();
      StringDistribution distribution = Iterables.getOnlyElement(intervalToDistribution.values());
      Assertions.assertNotNull(distribution);
      PartitionBoundaries partitions = distribution.getEvenPartitionsByMaxSize(1);
      Assertions.assertEquals(2, partitions.size());
      Assertions.assertNull(partitions.get(0));
      Assertions.assertNull(partitions.get(1));
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
      ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
          .forParallelIndexTask()
          .withForceGuaranteedRollup(true)
          .withPartitionsSpec(
              new DimensionRangePartitionsSpec(null, 1000, Collections.singletonList("dim"), false)
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

      Assertions.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, StringDistribution> intervalToDistribution = report.getIntervalToDistribution();
      StringDistribution distribution = Iterables.getOnlyElement(intervalToDistribution.values());
      Assertions.assertNotNull(distribution);
      PartitionBoundaries partitions = distribution.getEvenPartitionsByMaxSize(1);
      Assertions.assertEquals(minBloomFilterBits + 2, partitions.size()); // 2 = min + max

      StringTuple minDimensionValue = StringTuple.create(dimensionValues.get(0));
      Assertions.assertEquals(minDimensionValue, ((StringSketch) distribution).getMin());

      StringTuple maxDimensionValue = StringTuple.create(dimensionValues.get(dimensionValues.size() - 1));
      Assertions.assertEquals(maxDimensionValue, ((StringSketch) distribution).getMax());
    }

    @Test
    public void returnsSuccessIfNoExceptions() throws Exception
    {
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .build();

      TaskStatus taskStatus = task.runTask(taskToolbox);

      Assertions.assertEquals(ParallelIndexTestingFactory.ID, taskStatus.getId());
      Assertions.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
    }

    @Test
    public void testInputSourceResources()
    {
      PartialDimensionDistributionTask task = new PartialDimensionDistributionTaskBuilder()
          .build();

      Assertions.assertEquals(
          Collections.singleton(
              new ResourceAction(
                  new Resource(InlineInputSource.TYPE_KEY, ResourceType.EXTERNAL),
                  Action.READ
              )),
          task.getInputSourceResources()
      );
    }

    private DimensionDistributionReport runTask(PartialDimensionDistributionTaskBuilder taskBuilder)
    {


      try {
        taskBuilder.build()
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
    private static final InputFormat INPUT_FORMAT = new JsonInputFormat(null, null, null, null, null);

    private String id = ParallelIndexTestingFactory.ID;
    private InputSource inputSource = new InlineInputSource("row-with-invalid-timestamp");
    private ParallelIndexTuningConfig tuningConfig = TuningConfigBuilder
        .forParallelIndexTask()
        .withForceGuaranteedRollup(true)
        .withPartitionsSpec(
            new DimensionRangePartitionsSpec(null, 1000, Collections.singletonList("dim"), false)
        )
        .build();
    private DataSchema dataSchema =
        ParallelIndexTestingFactory.createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS);
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
              dataSchema.getGranularitySpec().getQueryGranularity(),
              null,
              null
          )
          : dedupRowDimValueFilterSupplier;

      return new PartialDimensionDistributionTask(
          id,
          ParallelIndexTestingFactory.GROUP_ID,
          ParallelIndexTestingFactory.TASK_RESOURCE,
          ParallelIndexTestingFactory.SUPERVISOR_TASK_ID,
          ParallelIndexTestingFactory.SUBTASK_SPEC_ID,
          ParallelIndexTestingFactory.NUM_ATTEMPTS,
          ingestionSpec,
          ParallelIndexTestingFactory.CONTEXT,
          supplier
      );
    }
  }
}
