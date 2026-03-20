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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.DruidServer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.NoopChatHandlerProvider;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderator;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class SeekableStreamIndexTaskRunnerTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock
  private InputRow row;

  @Mock
  private SeekableStreamIndexTask task;

  private StubServiceEmitter emitter;

  @Before
  public void setup()
  {
    emitter = new StubServiceEmitter();
  }

  @Test
  public void testWithinMinMaxTime()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    DateTime now = DateTimes.nowUtc();

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(120L);
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(DateTimes.nowUtc().plusHours(2));
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(DateTimes.nowUtc().minusHours(2));
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);

    TestSeekableStreamIndexTaskRunner runner = new TestSeekableStreamIndexTaskRunner(
        task,
        LockGranularity.TIME_CHUNK
    );

    Mockito.when(row.getTimestamp()).thenReturn(now);
    Assert.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assert.assertEquals(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assert.assertEquals(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));
  }

  @Test
  public void testWithinMinMaxTimeNotPopulated()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    DateTime now = DateTimes.nowUtc();

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(null);
    // min max time not populated.
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(null);
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(null);
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);
    TestSeekableStreamIndexTaskRunner runner = new TestSeekableStreamIndexTaskRunner(
        task,
        LockGranularity.TIME_CHUNK
    );

    Mockito.when(row.getTimestamp()).thenReturn(now);
    Assert.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assert.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assert.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));
  }

  @Test
  public void testEnsureRowRejectionReasonForNullRow()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(null);
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(null);
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(null);
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);

    TestSeekableStreamIndexTaskRunner runner = new TestSeekableStreamIndexTaskRunner(
        task,
        LockGranularity.TIME_CHUNK
    );

    Assert.assertEquals(InputRowFilterResult.NULL_OR_EMPTY_RECORD, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(null));
  }

  @Test
  public void test_run_emitsRowCountAndSegmentCount_onSuccessfulPublish()
  {
    DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    DataSchema schema =
        DataSchema.builder()
                  .withDataSource("datasource")
                  .withTimestamp(new TimestampSpec(null, null, null))
                  .withDimensions(dimensionsSpec)
                  .withGranularity(
                      new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                  )
                  .build();

    SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = Mockito.mock(SeekableStreamStartSequenceNumbers.class);
    SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = Mockito.mock(SeekableStreamEndSequenceNumbers.class);

    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(null);
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);

    Mockito.when(endSequenceNumbers.getPartitionSequenceNumberMap()).thenReturn(ImmutableMap.of());
    Mockito.when(sequenceNumbers.getStream()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);
    Mockito.when(task.getId()).thenReturn("task1");
    Mockito.when(task.getSupervisorId()).thenReturn("supervisorId");
    TestSeekableStreamIndexTaskRunner runner = new TestSeekableStreamIndexTaskRunner(
        task,
        LockGranularity.TIME_CHUNK
    );
    Assert.assertEquals("supervisorId", runner.getSupervisorId());

    // Setup the task to return a RecordSupplier, StreamAppenderatorDriver, Appenderator
    final RecordSupplier<?, ?, ?> recordSupplier = Mockito.mock(RecordSupplier.class);
    Mockito.when(task.newTaskRecordSupplier(any()))
           .thenReturn(recordSupplier);

    final StreamAppenderator appenderator = Mockito.mock(StreamAppenderator.class);
    Mockito.when(task.newAppenderator(any(), any(), any(), any()))
           .thenReturn(appenderator);

    final List<DataSegment> segment = CreateDataSegments
        .ofDatasource(schema.getDataSource())
        .withNumPartitions(10)
        .withNumRows(1_000)
        .eachOfSizeInMb(500);
    final SegmentsAndCommitMetadata commitMetadata = new SegmentsAndCommitMetadata(segment, "offset-100");

    final StreamAppenderatorDriver driver = Mockito.mock(StreamAppenderatorDriver.class);
    Mockito.when(task.newDriver(any(), any(), any()))
           .thenReturn(driver);
    Mockito.when(driver.publish(any(), any(), any()))
           .thenReturn(Futures.immediateFuture(commitMetadata));
    Mockito.when(driver.registerHandoff(any()))
           .thenReturn(Futures.immediateFuture(commitMetadata));

    Mockito.doAnswer(invocation -> {
      final String metricName = invocation.getArgument(1);
      final Number value = invocation.getArgument(2);
      emitter.emit(ServiceMetricEvent.builder().setMetric(metricName, value).build("test", "localhost"));
      return null;
    }).when(task).emitMetric(any(), any(), any());

    runner.run(createTaskToolbox());
    emitter.verifyValue("ingest/segments/count", 10);
    emitter.verifyValue("ingest/rows/published", 10_000L);
  }

  private TaskToolbox createTaskToolbox()
  {
    final TestUtils testUtils = new TestUtils();
    final File taskWorkDir = createTaskWorkDirectory();
    return new TaskToolbox
        .Builder()
        .indexIO(TestHelper.getTestIndexIO())
        .taskWorkDir(taskWorkDir)
        .taskReportFileWriter(new NoopTestTaskReportFileWriter())
        .authorizerMapper(AuthTestUtils.TEST_AUTHORIZER_MAPPER)
        .rowIngestionMetersFactory(NoopRowIngestionMeters::new)
        .indexMerger(testUtils.getIndexMergerV9Factory().create(true))
        .chatHandlerProvider(new NoopChatHandlerProvider())
        .dataNodeService(new DataNodeService(DruidServer.DEFAULT_TIER, 100L, null, ServerType.HISTORICAL, 1))
        .lookupNodeService(new LookupNodeService(DruidServer.DEFAULT_TIER))
        .appenderatorsManager(new TestAppenderatorsManager())
        .serverAnnouncer(new DataSegmentServerAnnouncer.Noop())
        .druidNodeAnnouncer(new NoopDruidNodeAnnouncer())
        .jsonMapper(TestHelper.JSON_MAPPER)
        .emitter(emitter)
        .build();
  }

  private File createTaskWorkDirectory()
  {
    try {
      final File taskWorkDir = temporaryFolder.newFolder();
      FileUtils.mkdirp(taskWorkDir);
      FileUtils.mkdirp(new File(taskWorkDir, "persist"));
      return taskWorkDir;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class NoopDruidNodeAnnouncer implements DruidNodeAnnouncer
  {

    @Override
    public void announce(DiscoveryDruidNode discoveryDruidNode)
    {

    }

    @Override
    public void unannounce(DiscoveryDruidNode discoveryDruidNode)
    {

    }
  }

  static class TestSeekableStreamIndexTaskRunner extends SeekableStreamIndexTaskRunner
  {
    public TestSeekableStreamIndexTaskRunner(
        SeekableStreamIndexTask task,
        LockGranularity lockGranularityToUse
    )
    {
      super(task, lockGranularityToUse);
    }

    @Override
    protected boolean isEndOfShard(Object seqNum)
    {
      return false;
    }

    @Nullable
    @Override
    protected TreeMap<Integer, Map> getCheckPointsFromContext(TaskToolbox toolbox, String checkpointsString)
    {
      return null;
    }

    @Override
    protected Object getNextStartOffset(Object sequenceNumber)
    {
      return null;
    }

    @Override
    protected SeekableStreamEndSequenceNumbers deserializePartitionsFromMetadata(ObjectMapper mapper, Object object)
    {
      return null;
    }

    @Override
    protected List<OrderedPartitionableRecord> getRecords(RecordSupplier recordSupplier, TaskToolbox toolbox)
    {
      return null;
    }

    @Override
    protected SeekableStreamDataSourceMetadata createDataSourceMetadata(SeekableStreamSequenceNumbers partitions)
    {
      return null;
    }

    @Override
    protected OrderedSequenceNumber createSequenceNumber(Object sequenceNumber)
    {
      return null;
    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
      return false;
    }

    @Override
    protected TypeReference<List<SequenceMetadata>> getSequenceMetadataTypeReference()
    {
      return null;
    }

    @Override
    protected void possiblyResetDataSourceMetadata(TaskToolbox toolbox, RecordSupplier recordSupplier, Set assignment)
    {

    }
  }
}
