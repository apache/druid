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
import com.google.common.collect.ImmutableSet;
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
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.segment.realtime.appenderator.SegmentsAndCommitMetadata;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderator;
import org.apache.druid.segment.realtime.appenderator.StreamAppenderatorDriver;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.StreamRangeShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Period;
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
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;

@RunWith(MockitoJUnitRunner.class)
public class SeekableStreamIndexTaskRunnerTest
{
  private static final String DATA_SOURCE = "datasource";

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
  public void testGetLastSequenceMetadataUsesStableSnapshotIfSequenceCompletesConcurrently() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner();
    final SequenceMetadata<String, String> firstSequence = new SequenceMetadata<>(
        0,
        "test_0",
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "5"),
        true,
        ImmutableSet.of(),
        null
    );
    final SequenceMetadata<String, String> secondSequence = new SequenceMetadata<>(
        1,
        "test_1",
        ImmutableMap.of("partition", "5"),
        ImmutableMap.of("partition", "10"),
        false,
        ImmutableSet.of(),
        null
    );
    final ShrinkingCopyOnWriteArrayList<SequenceMetadata<String, String>> sequences =
        new ShrinkingCopyOnWriteArrayList<>();
    sequences.add(firstSequence);
    sequences.add(secondSequence);
    setSequences(runner, sequences);

    sequences.removeFirstElementDuringNextSnapshotOrSize();

    Assert.assertSame(secondSequence, runner.getLastSequenceMetadata());
    Assert.assertEquals(1, sequences.size());
    Assert.assertSame(secondSequence, sequences.get(0));
  }

  @Test
  public void testSetEndOffsetsReturnsBadRequestWhenTaskIsNotPaused() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createInitializedRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "10")
    );
    setStatus(runner, SeekableStreamIndexTaskRunner.Status.READING);

    final Response response = runner.setEndOffsets(ImmutableMap.of("partition", "5"), false);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertEquals("Task must be paused before changing the end offsets", response.getEntity());
  }

  @Test
  public void testSetEndOffsetsReturnsBadRequestWhenLatestSequenceIsCheckpointed() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createInitializedRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "10")
    );
    setSequences(
        runner,
        Arrays.asList(
            new SequenceMetadata<>(
                0,
                "test_0",
                ImmutableMap.of("partition", "0"),
                ImmutableMap.of("partition", "5"),
                true,
                ImmutableSet.of(),
                null
            )
        )
    );

    final Response response = runner.setEndOffsets(ImmutableMap.of("partition", "6"), false);

    Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assert.assertTrue(response.getEntity().toString().contains("has already endOffsets set"));
  }

  @Test
  public void testSetEndOffsetsReturnsBadRequestWhenEndOffsetPrecedesCurrentOffset() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createInitializedRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "10")
    );
    setCurrentOffsets(runner, ImmutableMap.of("partition", "5"));

    try (final PausedRunner ignored = pauseRunner(runner)) {
      final Response response = runner.setEndOffsets(ImmutableMap.of("partition", "4"), false);

      Assert.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      Assert.assertEquals(
          "End sequence must be >= current sequence for partition [partition] (current: 5)",
          response.getEntity()
      );
      Assert.assertFalse(runner.getLastSequenceMetadata().isCheckpointed());
      Assert.assertEquals(1, runner.getSequences().size());
    }
  }

  @Test
  public void testSetEndOffsetsCreatesNewSequenceWhenPaused() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createInitializedRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "10")
    );
    setCurrentOffsets(runner, ImmutableMap.of("partition", "4"));

    try (final PausedRunner pausedRunner = pauseRunner(runner)) {
      final Response response = runner.setEndOffsets(ImmutableMap.of("partition", "5"), false);

      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      pausedRunner.awaitResumed();
    }

    final List<SequenceMetadata<String, String>> sequences = runner.getSequences();
    Assert.assertEquals(2, sequences.size());
    Assert.assertTrue(sequences.get(0).isCheckpointed());
    Assert.assertEquals(ImmutableMap.of("partition", "5"), sequences.get(0).getEndOffsets());
    Assert.assertEquals("test_1", sequences.get(1).getSequenceName());
    Assert.assertEquals(ImmutableMap.of("partition", "5"), sequences.get(1).getStartOffsets());
    Assert.assertEquals(ImmutableMap.of("partition", "10"), sequences.get(1).getEndOffsets());
    Assert.assertEquals(ImmutableSet.of("partition"), sequences.get(1).getExclusiveStartPartitions());
  }

  @Test
  public void testSetEndOffsetsFinishUpdatesLatestSequenceWhenPaused() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createInitializedRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "10")
    );
    setCurrentOffsets(runner, ImmutableMap.of("partition", "4"));

    try (final PausedRunner pausedRunner = pauseRunner(runner)) {
      final Response response = runner.setEndOffsets(ImmutableMap.of("partition", "6"), true);

      Assert.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      pausedRunner.awaitResumed();
    }

    final List<SequenceMetadata<String, String>> sequences = runner.getSequences();
    Assert.assertEquals(1, sequences.size());
    Assert.assertTrue(sequences.get(0).isCheckpointed());
    Assert.assertEquals(ImmutableMap.of("partition", "6"), sequences.get(0).getEndOffsets());
  }

  @Test
  public void testWithinMinMaxTime()
  {
    final DateTime now = DateTimes.nowUtc();
    final TestSeekableStreamIndexTaskRunner runner = createRunnerWithMessageTimeBounds(
        120L,
        now.minusHours(2),
        now.plusHours(2)
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
    final DateTime now = DateTimes.nowUtc();
    final TestSeekableStreamIndexTaskRunner runner = createRunner();

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
    final TestSeekableStreamIndexTaskRunner runner = createRunner();

    Assert.assertEquals(InputRowFilterResult.NULL_OR_EMPTY_RECORD, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(null));
  }

  @Test
  public void test_run_emitsRowCountAndSegmentCount_onSuccessfulPublish()
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner();
    Mockito.when(task.getId()).thenReturn("task1");
    Mockito.when(task.getSupervisorId()).thenReturn("supervisorId");
    Assert.assertEquals("supervisorId", runner.getSupervisorId());

    // Setup the task to return a RecordSupplier, StreamAppenderatorDriver, Appenderator
    final RecordSupplier<?, ?, ?> recordSupplier = Mockito.mock(RecordSupplier.class);
    Mockito.when(task.newTaskRecordSupplier(any()))
           .thenReturn(recordSupplier);

    final StreamAppenderator appenderator = Mockito.mock(StreamAppenderator.class);
    Mockito.when(task.newAppenderator(any(), any(), any(), any()))
           .thenReturn(appenderator);

    final List<DataSegment> segment = CreateDataSegments
        .ofDatasource(DATA_SOURCE)
        .withNumPartitions(10)
        .withNumRows(1_000)
        .eachOfSizeInMb(500);
    final SegmentsAndCommitMetadata commitMetadata =
        new SegmentsAndCommitMetadata(segment, "offset-100").withWasPublished(true);

    final StreamAppenderatorDriver driver = Mockito.mock(StreamAppenderatorDriver.class);
    Mockito.when(task.newDriver(any(), any(), any()))
           .thenReturn(driver);
    // publishAndRegisterHandoff calls the 4-arg publish overload (with the shard-spec annotator function).
    Mockito.when(driver.publish(any(), any(), any(), any()))
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

  @Test
  public void testAnnotateSegmentStampsStreamRangeShardSpecForObservedValues() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        Map.of("partition", "0"),
        Map.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();
    // Observe out of order; the published values must come back sorted.
    observe(runner, lookupKey, "tenant", "tenant_c", "tenant_a", "tenant_b");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(
        "A segment created during the current run with observed values should get a StreamRangeShardSpec",
        annotated.getShardSpec() instanceof StreamRangeShardSpec
    );
    final StreamRangeShardSpec shardSpec = (StreamRangeShardSpec) annotated.getShardSpec();
    Assert.assertEquals(
        Arrays.asList("tenant_a", "tenant_b", "tenant_c"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * A segment that spans a task restart has incomplete observed values, so it must NOT declare any partition filters
   * (no pruning), to avoid wrongly pruning pre-restart rows. It is still stamped with an empty-filter
   * {@link StreamRangeShardSpec} (not a bare {@link NumberedShardSpec}) so that all segments in an interval keep a
   * uniform shard-spec class for {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper}, which
   * rejects a publish batch mixing shard-spec classes within an interval.
   */
  @Test
  public void testRestartSpannedSegmentGetsEmptyFilterStreamRangeShardSpec() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    // Post-restart, only tenant_c is observed; tenant_a/tenant_b live only in pre-restart hydrants.
    observe(runner, lookupKey, "tenant", "tenant_c");
    // The runner marks this segment as restored-from-disk (spans a restart).
    markRestartSpanned(runner, lookupKey);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(
        "A restart-spanned segment must be stamped with a StreamRangeShardSpec (class-uniform with freshly-stamped "
        + "segments in the same interval) so SegmentPublisherHelper does not reject the publish",
        annotated.getShardSpec() instanceof StreamRangeShardSpec
    );
    Assert.assertTrue(
        "Its filters must be empty (no pruning) so incompletely-observed pre-restart rows are never pruned away",
        ((StreamRangeShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
  }

  /**
   * A restart batch mixes a restart-spanned partition (empty-filter fallback) with a freshly-observed one in the same
   * interval. Both must keep a uniform shard-spec class so the publish isn't rejected.
   */
  @Test
  public void testRestartBatchMixingFallbackAndObservedSegmentsPublishesWithStreamRangeShardSpec()
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    // Two partitions in one interval: partition 0 was restored from disk across a restart, partition 1 created after.
    final List<DataSegment> sameIntervalPartitions = CreateDataSegments
        .ofDatasource(DATA_SOURCE)
        .startingAt("2025-01-01")
        .forIntervals(1, Granularities.DAY)
        .withNumPartitions(2)
        .eachOfSizeInMb(500);
    final DataSegment restartSpanned = sameIntervalPartitions.get(0);
    final DataSegment freshlyObserved = sameIntervalPartitions.get(1);

    markRestartSpanned(runner, restartSpanned.getId());
    observe(runner, restartSpanned.getId(), "tenant", "tenant_c");
    observe(runner, freshlyObserved.getId(), "tenant", "tenant_a");

    final DataSegment annotatedRestartSpanned = runner.annotateSegmentWithPartitionDimensionValues(restartSpanned);
    final DataSegment annotatedFreshlyObserved = runner.annotateSegmentWithPartitionDimensionValues(freshlyObserved);

    Assert.assertEquals(
        annotatedRestartSpanned.getShardSpec().getClass(),
        annotatedFreshlyObserved.getShardSpec().getClass()
    );
    Assert.assertTrue(annotatedRestartSpanned.getShardSpec() instanceof StreamRangeShardSpec);
    Assert.assertTrue(
        ((StreamRangeShardSpec) annotatedRestartSpanned.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
    Assert.assertEquals(
        List.of("tenant_a"),
        ((StreamRangeShardSpec) annotatedFreshlyObserved.getShardSpec()).getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * A dimension that ingested a null/missing value declares null (as a null list element) alongside its non-null
   * values, so {@code IS NULL} queries are not pruned. Here tenant saw tenant_a and a null; region saw only us-west.
   */
  @Test
  public void testNullValuedDimensionDeclaresNullInPartitionDimensionValues() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant", "region")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    // tenant saw a non-null value and (in another row) a null/missing value; region only saw non-null values.
    observe(runner, lookupKey, "tenant", "tenant_a", null);
    observe(runner, lookupKey, "region", "us-west");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(
        annotated.getShardSpec() instanceof StreamRangeShardSpec
    );
    final StreamRangeShardSpec shardSpec = (StreamRangeShardSpec) annotated.getShardSpec();
    // tenant declares both its non-null value AND null, so IS NULL queries are not pruned.
    Assert.assertEquals(
        Arrays.asList(null, "tenant_a"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
    Assert.assertEquals(
        ImmutableSet.of("us-west"),
        ImmutableSet.copyOf(shardSpec.getPartitionDimensionValues().get("region"))
    );
  }

  /**
   * A dimension that ingested only a null value declares {@code [null]} — pruned for concrete-value queries but never
   * for {@code IS NULL}.
   */
  @Test
  public void testOnlyNullValuedDimensionDeclaresNull() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    observe(runner, lookupKey, "tenant", (String) null);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertTrue(annotated.getShardSpec() instanceof StreamRangeShardSpec);
    final StreamRangeShardSpec shardSpec = (StreamRangeShardSpec) annotated.getShardSpec();
    Assert.assertEquals(
        Collections.singletonList(null),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * Feature on, but a segment ingested no values for any tracked dimension (nothing recorded under its key). It still
   * gets an empty-filter {@link StreamRangeShardSpec} rather than being returned as a bare {@link NumberedShardSpec},
   * so it stays class-uniform with its interval siblings for
   * {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper}.
   */
  @Test
  public void testSegmentWithNoObservedValuesGetsEmptyFilterStreamRangeShardSpec() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec())
           .thenReturn(new StreamingPartitionsSpec(List.of("tenant")));

    // No observe(...) call: nothing was recorded for this segment.
    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(createSingleSegment());

    Assert.assertTrue(annotated.getShardSpec() instanceof StreamRangeShardSpec);
    Assert.assertTrue(
        "A segment with no observed values declares no filters (no pruning) but stays a StreamRangeShardSpec",
        ((StreamRangeShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
  }

  /**
   * Feature off (no streamingPartitionsSpec): the segment is returned completely unchanged, retaining its original
   * shard spec.
   */
  @Test
  public void testFeatureOffReturnsSegmentUnchanged() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );
    Mockito.when(task.getTuningConfig().getStreamingPartitionsSpec()).thenReturn(null);

    final DataSegment segment = createSingleSegment();
    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assert.assertSame("With the feature off the segment must be returned unchanged", segment, annotated);
  }

  private static DataSegment createSingleSegment()
  {
    return CreateDataSegments
        .ofDatasource(DATA_SOURCE)
        .startingAt("2025-01-01")
        .forIntervals(1, Granularities.DAY)
        .withNumPartitions(1)
        .eachOfSizeInMb(500)
        .get(0);
  }

  private static void observe(
      SeekableStreamIndexTaskRunner runner,
      SegmentId segmentId,
      String dimension,
      String... values
  )
  {
    for (String value : values) {
      runner.recordObservedDimensionValueForTest(segmentId, dimension, value);
    }
  }

  private static void markRestartSpanned(SeekableStreamIndexTaskRunner runner, SegmentId segmentId)
  {
    runner.markSegmentRestartSpannedForTest(segmentId);
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
        .chatHandlerProvider(new ChatHandlerProvider())
        .dataNodeService(new DataNodeService(DruidServer.DEFAULT_TIER, 100L, null, ServerType.HISTORICAL, 1))
        .lookupNodeService(new LookupNodeService(DruidServer.DEFAULT_TIER))
        .appenderatorsManager(new TestAppenderatorsManager())
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

  private TestSeekableStreamIndexTaskRunner createRunner()
  {
    return createRunner(ImmutableMap.of(), ImmutableMap.of());
  }

  private TestSeekableStreamIndexTaskRunner createRunner(
      Map<String, String> startOffsets,
      Map<String, String> endOffsets
  )
  {
    return createRunner(createDataSchema(), null, null, null, startOffsets, endOffsets);
  }

  private TestSeekableStreamIndexTaskRunner createRunnerWithMessageTimeBounds(
      Long refreshRejectionPeriodsInMinutes,
      DateTime minMessageTime,
      DateTime maxMessageTime
  )
  {
    return createRunner(
        createDataSchema(),
        refreshRejectionPeriodsInMinutes,
        minMessageTime,
        maxMessageTime,
        ImmutableMap.of(),
        ImmutableMap.of()
    );
  }

  private TestSeekableStreamIndexTaskRunner createRunner(
      DataSchema schema,
      @Nullable Long refreshRejectionPeriodsInMinutes,
      @Nullable DateTime minMessageTime,
      @Nullable DateTime maxMessageTime,
      Map<String, String> startOffsets,
      Map<String, String> endOffsets
  )
  {
    final SeekableStreamIndexTaskTuningConfig tuningConfig = Mockito.mock(SeekableStreamIndexTaskTuningConfig.class);
    final SeekableStreamIndexTaskIOConfig<String, String> ioConfig = Mockito.mock(SeekableStreamIndexTaskIOConfig.class);
    final SeekableStreamStartSequenceNumbers<String, String> sequenceNumbers = new SeekableStreamStartSequenceNumbers<>(
        "test",
        startOffsets,
        ImmutableSet.of()
    );
    final SeekableStreamEndSequenceNumbers<String, String> endSequenceNumbers = new SeekableStreamEndSequenceNumbers<>(
        "test",
        endOffsets
    );

    Mockito.when(tuningConfig.getIntermediateHandoffPeriod()).thenReturn(Period.minutes(1));
    Mockito.when(ioConfig.getRefreshRejectionPeriodsInMinutes()).thenReturn(refreshRejectionPeriodsInMinutes);
    Mockito.when(ioConfig.getMaximumMessageTime()).thenReturn(maxMessageTime);
    Mockito.when(ioConfig.getMinimumMessageTime()).thenReturn(minMessageTime);
    Mockito.when(ioConfig.getInputFormat()).thenReturn(new JsonInputFormat(null, null, null, null, null));
    Mockito.when(ioConfig.getStartSequenceNumbers()).thenReturn(sequenceNumbers);
    Mockito.when(ioConfig.getEndSequenceNumbers()).thenReturn(endSequenceNumbers);
    Mockito.when(ioConfig.getBaseSequenceName()).thenReturn("test");

    Mockito.when(task.getDataSchema()).thenReturn(schema);
    Mockito.when(task.getIOConfig()).thenReturn(ioConfig);
    Mockito.when(task.getTuningConfig()).thenReturn(tuningConfig);
    Mockito.when(task.getContext()).thenReturn(ImmutableMap.of());

    return new TestSeekableStreamIndexTaskRunner(
        task,
        LockGranularity.TIME_CHUNK
    );
  }

  private static DataSchema createDataSchema()
  {
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new StringDimensionSchema("d1"),
            new StringDimensionSchema("d2")
        )
    );
    return DataSchema.builder()
                     .withDataSource(DATA_SOURCE)
                     .withTimestamp(TimestampSpec.DEFAULT)
                     .withDimensions(dimensionsSpec)
                     .withGranularity(
                         new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null)
                     )
                     .build();
  }

  private TestSeekableStreamIndexTaskRunner createInitializedRunner(
      Map<String, String> startOffsets,
      Map<String, String> endOffsets
  ) throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(startOffsets, endOffsets);
    runner.setToolbox(createTaskToolbox());
    runner.initializeSequences();
    setCurrentOffsets(runner, startOffsets);
    return runner;
  }

  private static void setSequences(
      SeekableStreamIndexTaskRunner runner,
      List<? extends SequenceMetadata> sequences
  ) throws NoSuchFieldException, IllegalAccessException
  {
    final Field sequencesField = SeekableStreamIndexTaskRunner.class.getDeclaredField("sequences");
    sequencesField.setAccessible(true);
    sequencesField.set(runner, sequences);
  }

  private static void setCurrentOffsets(
      SeekableStreamIndexTaskRunner runner,
      Map<?, ?> currentOffsets
  ) throws NoSuchFieldException, IllegalAccessException
  {
    final Field currOffsetsField = SeekableStreamIndexTaskRunner.class.getDeclaredField("currOffsets");
    currOffsetsField.setAccessible(true);
    final Map currOffsets = (Map) currOffsetsField.get(runner);
    currOffsets.clear();
    currOffsets.putAll(currentOffsets);
  }

  private static void setStatus(
      SeekableStreamIndexTaskRunner runner,
      SeekableStreamIndexTaskRunner.Status status
  ) throws NoSuchFieldException, IllegalAccessException
  {
    final Field statusField = SeekableStreamIndexTaskRunner.class.getDeclaredField("status");
    statusField.setAccessible(true);
    statusField.set(runner, status);
  }

  private static void setPauseRequested(
      SeekableStreamIndexTaskRunner runner,
      boolean pauseRequested
  ) throws NoSuchFieldException, IllegalAccessException
  {
    final Field pauseRequestedField = SeekableStreamIndexTaskRunner.class.getDeclaredField("pauseRequested");
    pauseRequestedField.setAccessible(true);
    pauseRequestedField.set(runner, pauseRequested);
  }

  private static SeekableStreamIndexTaskRunner.Status getStatus(
      SeekableStreamIndexTaskRunner runner
  ) throws NoSuchFieldException, IllegalAccessException
  {
    final Field statusField = SeekableStreamIndexTaskRunner.class.getDeclaredField("status");
    statusField.setAccessible(true);
    return (SeekableStreamIndexTaskRunner.Status) statusField.get(runner);
  }

  private static PausedRunner pauseRunner(TestSeekableStreamIndexTaskRunner runner) throws Exception
  {
    setStatus(runner, SeekableStreamIndexTaskRunner.Status.READING);
    setPauseRequested(runner, true);

    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<Boolean> possiblyPauseFuture = executor.submit(() -> invokePossiblyPause(runner));
    waitForStatus(runner, SeekableStreamIndexTaskRunner.Status.PAUSED);
    return new PausedRunner(runner, executor, possiblyPauseFuture);
  }

  private static void waitForStatus(
      SeekableStreamIndexTaskRunner runner,
      SeekableStreamIndexTaskRunner.Status status
  ) throws Exception
  {
    final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(2);
    while (System.currentTimeMillis() < deadline) {
      if (getStatus(runner) == status) {
        return;
      }
      Thread.sleep(10);
    }
    Assert.fail("Timed out waiting for status [" + status + "]");
  }

  private static boolean invokePossiblyPause(SeekableStreamIndexTaskRunner runner) throws Exception
  {
    final Method possiblyPauseMethod = SeekableStreamIndexTaskRunner.class.getDeclaredMethod("possiblyPause");
    possiblyPauseMethod.setAccessible(true);
    try {
      return (boolean) possiblyPauseMethod.invoke(runner);
    }
    catch (InvocationTargetException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      } else if (cause instanceof Error) {
        throw (Error) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  private static class PausedRunner implements AutoCloseable
  {
    private final TestSeekableStreamIndexTaskRunner runner;
    private final ExecutorService executor;
    private final Future<Boolean> possiblyPauseFuture;

    private PausedRunner(
        TestSeekableStreamIndexTaskRunner runner,
        ExecutorService executor,
        Future<Boolean> possiblyPauseFuture
    )
    {
      this.runner = runner;
      this.executor = executor;
      this.possiblyPauseFuture = possiblyPauseFuture;
    }

    void awaitResumed() throws Exception
    {
      Assert.assertTrue(possiblyPauseFuture.get(2, TimeUnit.SECONDS));
    }

    @Override
    public void close() throws Exception
    {
      try {
        if (!possiblyPauseFuture.isDone()) {
          runner.resume();
          awaitResumed();
        }
      }
      finally {
        executor.shutdownNow();
      }
    }
  }

  private static class ShrinkingCopyOnWriteArrayList<E> extends CopyOnWriteArrayList<E>
  {
    private final AtomicBoolean removeFirstElement = new AtomicBoolean(false);

    void removeFirstElementDuringNextSnapshotOrSize()
    {
      removeFirstElement.set(true);
    }

    @Override
    public boolean isEmpty()
    {
      return super.size() == 0;
    }

    @Override
    public int size()
    {
      final int size = super.size();
      if (removeFirstElement.compareAndSet(true, false)) {
        remove(0);
      }
      return size;
    }

    @Override
    public Object[] toArray()
    {
      final Object[] snapshot = super.toArray();
      if (removeFirstElement.compareAndSet(true, false)) {
        remove(0);
      }
      return snapshot;
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
      if (sequenceNumber == null) {
        return null;
      }
      return String.valueOf(Long.parseLong(sequenceNumber.toString()) + 1);
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
      if (sequenceNumber == null) {
        return null;
      }
      return new OrderedSequenceNumber<>(sequenceNumber.toString(), false)
      {
        @Override
        public int compareTo(OrderedSequenceNumber<String> other)
        {
          return Long.compare(Long.parseLong(get()), Long.parseLong(other.get()));
        }
      };
    }

    @Override
    protected boolean isEndOffsetExclusive()
    {
      return false;
    }

    @Override
    protected TypeReference<List<SequenceMetadata>> getSequenceMetadataTypeReference()
    {
      return new TypeReference<>()
      {
      };
    }

    @Override
    protected void possiblyResetDataSourceMetadata(TaskToolbox toolbox, RecordSupplier recordSupplier, Set assignment)
    {

    }
  }
}
