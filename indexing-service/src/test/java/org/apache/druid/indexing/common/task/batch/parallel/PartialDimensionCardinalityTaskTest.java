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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.client.indexing.NoopIndexingServiceClient;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.core.LogEvent;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Enclosed.class)
public class PartialDimensionCardinalityTaskTest
{
  private static final ObjectMapper OBJECT_MAPPER = ParallelIndexTestingFactory.createObjectMapper();
  private static final HashedPartitionsSpec HASHED_PARTITIONS_SPEC = HashedPartitionsSpec.defaultSpec();

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

      new PartialDimensionCardinalityTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();
    }

    @Test
    public void requiresHashedPartitions()
    {
      exception.expect(IllegalArgumentException.class);
      exception.expectMessage("hashed partitionsSpec required");

      PartitionsSpec partitionsSpec = new SingleDimensionPartitionsSpec(null, 1, "a", false);
      ParallelIndexTuningConfig tuningConfig =
          new ParallelIndexTestingFactory.TuningConfigBuilder().partitionsSpec(partitionsSpec).build();

      new PartialDimensionCardinalityTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();
    }

    @Test
    public void serializesDeserializes()
    {
      PartialDimensionCardinalityTask task = new PartialDimensionCardinalityTaskBuilder()
          .build();
      TestHelper.testSerializesDeserializes(OBJECT_MAPPER, task);
    }

    @Test
    public void hasCorrectPrefixForAutomaticId()
    {
      PartialDimensionCardinalityTask task = new PartialDimensionCardinalityTaskBuilder()
          .id(ParallelIndexTestingFactory.AUTOMATIC_ID)
          .build();
      Assert.assertThat(task.getId(), Matchers.startsWith(PartialDimensionCardinalityTask.TYPE));
    }
  }

  public static class RunTaskTest
  {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public LoggerCaptureRule logger = new LoggerCaptureRule(ParseExceptionHandler.class);

    private Capture<SubTaskReport> reportCapture;
    private TaskToolbox taskToolbox;

    @Before
    public void setup()
    {
      reportCapture = Capture.newInstance();
      ParallelIndexSupervisorTaskClient taskClient = EasyMock.mock(ParallelIndexSupervisorTaskClient.class);
      taskClient.report(EasyMock.eq(ParallelIndexTestingFactory.SUPERVISOR_TASK_ID), EasyMock.capture(reportCapture));
      EasyMock.replay(taskClient);
      taskToolbox = EasyMock.mock(TaskToolbox.class);
      EasyMock.expect(taskToolbox.getIndexingTmpDir()).andStubReturn(temporaryFolder.getRoot());
      EasyMock.expect(taskToolbox.getSupervisorTaskClientFactory()).andReturn(
          new IndexTaskClientFactory<ParallelIndexSupervisorTaskClient>()
          {
            @Override
            public ParallelIndexSupervisorTaskClient build(
                TaskInfoProvider taskInfoProvider,
                String callerId,
                int numThreads,
                Duration httpTimeout,
                long numRetries
            )
            {
              return taskClient;
            }
          }
      );
      EasyMock.expect(taskToolbox.getIndexingServiceClient()).andReturn(new NoopIndexingServiceClient());
      EasyMock.expect(taskToolbox.getRowIngestionMetersFactory()).andReturn(new DropwizardRowIngestionMetersFactory());
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
      PartialDimensionCardinalityTask task = new PartialDimensionCardinalityTaskBuilder()
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
          .partitionsSpec(HASHED_PARTITIONS_SPEC)
          .logParseExceptions(true)
          .build();
      PartialDimensionCardinalityTask task = new PartialDimensionCardinalityTaskBuilder()
          .inputSource(inlineInputSource)
          .tuningConfig(tuningConfig)
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
          .partitionsSpec(HASHED_PARTITIONS_SPEC)
          .logParseExceptions(false)
          .build();
      PartialDimensionCardinalityTask task = new PartialDimensionCardinalityTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();

      task.runTask(taskToolbox);

      Assert.assertEquals(Collections.emptyList(), logger.getLogEvents());
    }

    @Test
    public void failsWhenTooManyParseExceptions() throws Exception
    {
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(HASHED_PARTITIONS_SPEC)
          .maxParseExceptions(0)
          .build();
      PartialDimensionCardinalityTask task = new PartialDimensionCardinalityTaskBuilder()
          .tuningConfig(tuningConfig)
          .build();

      exception.expect(RuntimeException.class);
      exception.expectMessage("Max parse exceptions[0] exceeded");

      task.runTask(taskToolbox);
    }

    @Test
    public void sendsCorrectReportWhenRowHasMultipleDimensionValues()
    {
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(0, Arrays.asList("a", "b"))
      );
      PartialDimensionCardinalityTaskBuilder taskBuilder = new PartialDimensionCardinalityTaskBuilder()
          .inputSource(inlineInputSource);

      DimensionCardinalityReport report = runTask(taskBuilder);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, byte[]> intervalToCardinalities = report.getIntervalToCardinalities();
      byte[] hllSketchBytes = Iterables.getOnlyElement(intervalToCardinalities.values());
      HllSketch hllSketch = HllSketch.wrap(Memory.wrap(hllSketchBytes));
      Assert.assertNotNull(hllSketch);
      Assert.assertEquals(1L, (long) hllSketch.getEstimate());
    }

    @Test
    public void sendsCorrectReportWhenNonEmptyPartitionDimension()
    {
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRowFromMap(0, ImmutableMap.of("dim1", "a", "dim2", "1")) + "\n" +
          ParallelIndexTestingFactory.createRowFromMap(0, ImmutableMap.of("dim1", "a", "dim2", "2")) + "\n" +
          ParallelIndexTestingFactory.createRowFromMap(0, ImmutableMap.of("dim1", "b", "dim2", "3")) + "\n" +
          ParallelIndexTestingFactory.createRowFromMap(0, ImmutableMap.of("dim1", "b", "dim2", "4"))
      );
      HashedPartitionsSpec partitionsSpec = new HashedPartitionsSpec(null, null,
                                                                     Collections.singletonList("dim1")
      );
      ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
          .partitionsSpec(partitionsSpec)
          .build();

      PartialDimensionCardinalityTaskBuilder taskBuilder = new PartialDimensionCardinalityTaskBuilder()
          .inputSource(inlineInputSource)
          .tuningConfig(tuningConfig)
          .withDimensions(Arrays.asList("dim1", "dim2"));

      DimensionCardinalityReport report = runTask(taskBuilder);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, byte[]> intervalToCardinalities = report.getIntervalToCardinalities();
      byte[] hllSketchBytes = Iterables.getOnlyElement(intervalToCardinalities.values());
      HllSketch hllSketch = HllSketch.wrap(Memory.wrap(hllSketchBytes));
      Assert.assertNotNull(hllSketch);
      Assert.assertEquals(4L, (long) hllSketch.getEstimate());

    }

    @Test
    public void sendsCorrectReportWithMultipleIntervalsInData()
    {
      // Segment granularity is DAY, query granularity is HOUR
      InputSource inlineInputSource = new InlineInputSource(
          ParallelIndexTestingFactory.createRow(DateTimes.of("1970-01-01T00:00:00.001Z").getMillis(), "a") + "\n" +
          ParallelIndexTestingFactory.createRow(DateTimes.of("1970-01-02T03:46:40.000Z").getMillis(), "b") + "\n" +
          ParallelIndexTestingFactory.createRow(DateTimes.of("1970-01-02T03:46:40.000Z").getMillis(), "c") + "\n" +
          ParallelIndexTestingFactory.createRow(DateTimes.of("1970-01-02T04:02:40.000Z").getMillis(), "b") + "\n" +
          ParallelIndexTestingFactory.createRow(DateTimes.of("1970-01-02T05:19:10.000Z").getMillis(), "b")
      );
      PartialDimensionCardinalityTaskBuilder taskBuilder = new PartialDimensionCardinalityTaskBuilder()
          .inputSource(inlineInputSource);

      DimensionCardinalityReport report = runTask(taskBuilder);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, report.getTaskId());
      Map<Interval, byte[]> intervalToCardinalities = report.getIntervalToCardinalities();
      Assert.assertEquals(2, intervalToCardinalities.size());

      byte[] hllSketchBytes;
      HllSketch hllSketch;
      hllSketchBytes = intervalToCardinalities.get(Intervals.of("1970-01-01T00:00:00.000Z/1970-01-02T00:00:00.000Z"));
      hllSketch = HllSketch.wrap(Memory.wrap(hllSketchBytes));
      Assert.assertNotNull(hllSketch);
      Assert.assertEquals(1L, (long) hllSketch.getEstimate());

      hllSketchBytes = intervalToCardinalities.get(Intervals.of("1970-01-02T00:00:00.000Z/1970-01-03T00:00:00.000Z"));
      hllSketch = HllSketch.wrap(Memory.wrap(hllSketchBytes));
      Assert.assertNotNull(hllSketch);
      Assert.assertEquals(4L, (long) hllSketch.getEstimate());
    }

    @Test
    public void returnsSuccessIfNoExceptions() throws Exception
    {
      PartialDimensionCardinalityTask task = new PartialDimensionCardinalityTaskBuilder()
          .build();

      TaskStatus taskStatus = task.runTask(taskToolbox);

      Assert.assertEquals(ParallelIndexTestingFactory.ID, taskStatus.getId());
      Assert.assertEquals(TaskState.SUCCESS, taskStatus.getStatusCode());
    }

    private DimensionCardinalityReport runTask(PartialDimensionCardinalityTaskBuilder taskBuilder)
    {
      try {
        taskBuilder.build()
                   .runTask(taskToolbox);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }

      return (DimensionCardinalityReport) reportCapture.getValue();
    }
  }

  private static class PartialDimensionCardinalityTaskBuilder
  {
    private static final InputFormat INPUT_FORMAT = ParallelIndexTestingFactory.getInputFormat();

    private String id = ParallelIndexTestingFactory.ID;
    private InputSource inputSource = new InlineInputSource("row-with-invalid-timestamp");
    private ParallelIndexTuningConfig tuningConfig = new ParallelIndexTestingFactory.TuningConfigBuilder()
        .partitionsSpec(HASHED_PARTITIONS_SPEC)
        .build();
    private DataSchema dataSchema =
        ParallelIndexTestingFactory
            .createDataSchema(ParallelIndexTestingFactory.INPUT_INTERVALS)
            .withGranularitySpec(
                new UniformGranularitySpec(
                    Granularities.DAY,
                    Granularities.HOUR,
                    ImmutableList.of(Intervals.of("1970-01-01T00:00:00Z/P10D"))
                )
            );

    @SuppressWarnings("SameParameterValue")
    PartialDimensionCardinalityTaskBuilder id(String id)
    {
      this.id = id;
      return this;
    }

    PartialDimensionCardinalityTaskBuilder inputSource(InputSource inputSource)
    {
      this.inputSource = inputSource;
      return this;
    }

    PartialDimensionCardinalityTaskBuilder tuningConfig(ParallelIndexTuningConfig tuningConfig)
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    PartialDimensionCardinalityTaskBuilder dataSchema(DataSchema dataSchema)
    {
      this.dataSchema = dataSchema;
      return this;
    }

    PartialDimensionCardinalityTaskBuilder withDimensions(List<String> dims)
    {
      this.dataSchema = dataSchema.withDimensionsSpec(new DimensionsSpec(DimensionsSpec.getDefaultSchemas(dims)));
      return this;
    }


    PartialDimensionCardinalityTask build()
    {
      ParallelIndexIngestionSpec ingestionSpec =
          ParallelIndexTestingFactory.createIngestionSpec(inputSource, INPUT_FORMAT, tuningConfig, dataSchema);

      return new PartialDimensionCardinalityTask(
          id,
          ParallelIndexTestingFactory.GROUP_ID,
          ParallelIndexTestingFactory.TASK_RESOURCE,
          ParallelIndexTestingFactory.SUPERVISOR_TASK_ID,
          ParallelIndexTestingFactory.NUM_ATTEMPTS,
          ingestionSpec,
          ParallelIndexTestingFactory.CONTEXT,
          OBJECT_MAPPER
      );
    }
  }
}
