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
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LongDimensionSchema;
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
import org.apache.druid.segment.column.ColumnType;
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
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionValueSetShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;

public class SeekableStreamIndexTaskRunnerTest
{
  private static final String DATA_SOURCE = "datasource";

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  @Mock
  private InputRow row;

  @Mock
  private SeekableStreamIndexTask task;
  private AutoCloseable mocks;

  private StubServiceEmitter emitter;

  @BeforeEach
  public void setup()
  {
    mocks = MockitoAnnotations.openMocks(this);
    emitter = new StubServiceEmitter();
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    if (mocks != null) {
      mocks.close();
    }
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

    Assertions.assertSame(secondSequence, runner.getLastSequenceMetadata());
    Assertions.assertEquals(1, sequences.size());
    Assertions.assertSame(secondSequence, sequences.get(0));
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

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertEquals("Task must be paused before changing the end offsets", response.getEntity());
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

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
    Assertions.assertTrue(response.getEntity().toString().contains("has already endOffsets set"));
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

      Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), response.getStatus());
      Assertions.assertEquals(
          "End sequence must be >= current sequence for partition [partition] (current: 5)",
          response.getEntity()
      );
      Assertions.assertFalse(runner.getLastSequenceMetadata().isCheckpointed());
      Assertions.assertEquals(1, runner.getSequences().size());
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

      Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      pausedRunner.awaitResumed();
    }

    final List<SequenceMetadata<String, String>> sequences = runner.getSequences();
    Assertions.assertEquals(2, sequences.size());
    Assertions.assertTrue(sequences.get(0).isCheckpointed());
    Assertions.assertEquals(ImmutableMap.of("partition", "5"), sequences.get(0).getEndOffsets());
    Assertions.assertEquals("test_1", sequences.get(1).getSequenceName());
    Assertions.assertEquals(ImmutableMap.of("partition", "5"), sequences.get(1).getStartOffsets());
    Assertions.assertEquals(ImmutableMap.of("partition", "10"), sequences.get(1).getEndOffsets());
    Assertions.assertEquals(ImmutableSet.of("partition"), sequences.get(1).getExclusiveStartPartitions());
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

      Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
      pausedRunner.awaitResumed();
    }

    final List<SequenceMetadata<String, String>> sequences = runner.getSequences();
    Assertions.assertEquals(1, sequences.size());
    Assertions.assertTrue(sequences.get(0).isCheckpointed());
    Assertions.assertEquals(ImmutableMap.of("partition", "6"), sequences.get(0).getEndOffsets());
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
    Assertions.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assertions.assertEquals(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assertions.assertEquals(InputRowFilterResult.AFTER_MAX_MESSAGE_TIME, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));
  }

  @Test
  public void testWithinMinMaxTimeNotPopulated()
  {
    final DateTime now = DateTimes.nowUtc();
    final TestSeekableStreamIndexTaskRunner runner = createRunner();

    Mockito.when(row.getTimestamp()).thenReturn(now);
    Assertions.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.minusHours(2).minusMinutes(1));
    Assertions.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));

    Mockito.when(row.getTimestamp()).thenReturn(now.plusHours(2).plusMinutes(1));
    Assertions.assertEquals(InputRowFilterResult.ACCEPTED, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(row));
  }

  @Test
  public void testEnsureRowRejectionReasonForNullRow()
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner();

    Assertions.assertEquals(InputRowFilterResult.NULL_OR_EMPTY_RECORD, runner.ensureRowIsNonNullAndWithinMessageTimeBounds(null));
  }

  @Test
  public void test_run_emitsRowCountAndSegmentCount_onSuccessfulPublish()
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner();
    Mockito.when(task.getId()).thenReturn("task1");
    Mockito.when(task.getSupervisorId()).thenReturn("supervisorId");
    Assertions.assertEquals("supervisorId", runner.getSupervisorId());

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
  public void testAnnotateSegmentStampsDimensionValueSetShardSpecForObservedValues() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        Map.of("partition", "0"),
        Map.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"))
    );

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();
    // Observe out of order; the published values must come back sorted.
    observe(runner, lookupKey, "tenant", "tenant_c", "tenant_a", "tenant_b");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(
        annotated.getShardSpec() instanceof DimensionValueSetShardSpec,
        "A segment created during the current run with observed values should get a DimensionValueSetShardSpec"
    );
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    Assertions.assertEquals(
        Arrays.asList("tenant_a", "tenant_b", "tenant_c"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * A partition dimension explicitly declared as a {@link LongDimensionSchema} gets a {@link ColumnType#LONG} stamp in
   * {@link DimensionValueSetShardSpec#getDimensionColumnTypes()}, gating the broker's typed numeric-value pruning.
   */
  @Test
  public void testAnnotateSegmentStampsLongColumnTypeForLongDimensionSchema() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        createDataSchemaWithPartitionDimensionSchemas(),
        null,
        null,
        null,
        Map.of("partition", "0"),
        Map.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"))
    );

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();
    observe(runner, lookupKey, "tenant", "100", "200");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    Assertions.assertEquals(ColumnType.LONG, shardSpec.getDimensionColumnTypes().get("tenant"));
    // The new type stamp does not disturb value-stamping.
    Assertions.assertEquals(
        Arrays.asList("100", "200"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * A LONG partition dimension's raw value canonicalizes to the same string the indexer stores and the broker queries
   * with, so a non-canonical token (leading zeros, leading '+', trailing ".0", a boxed number) is not wrongly pruned.
   * Unparseable, multi-value, and null/missing values coerce to null so {@code IS NULL} is not pruned.
   */
  @Test
  public void testCanonicalLongValueMatchesIndexerCoercion()
  {
    // Non-canonical tokens collapse to the canonical Long string.
    Assertions.assertEquals("1", DimensionValueSetCollector.canonicalLongValue("00001", "tenant"));
    Assertions.assertEquals("5", DimensionValueSetCollector.canonicalLongValue("+5", "tenant"));
    Assertions.assertEquals("1", DimensionValueSetCollector.canonicalLongValue("1.0", "tenant"));
    Assertions.assertEquals("1", DimensionValueSetCollector.canonicalLongValue(1L, "tenant"));
    Assertions.assertEquals("5", DimensionValueSetCollector.canonicalLongValue(5, "tenant"));
    Assertions.assertEquals("1", DimensionValueSetCollector.canonicalLongValue(1.0, "tenant"));

    // Null, unparseable, whitespace-padded, and multi-value all become null (so IS NULL is not falsely pruned).
    Assertions.assertNull(DimensionValueSetCollector.canonicalLongValue(null, "tenant"));
    Assertions.assertNull(DimensionValueSetCollector.canonicalLongValue("abc", "tenant"));
    Assertions.assertNull(DimensionValueSetCollector.canonicalLongValue(" 5", "tenant"));
    Assertions.assertNull(DimensionValueSetCollector.canonicalLongValue(Arrays.asList(1, 2), "tenant"));
  }

  /**
   * A partition dimension declared as a {@link StringDimensionSchema} (or not declared at all, i.e. schemaless) is left
   * out of {@link DimensionValueSetShardSpec#getDimensionColumnTypes()}, but is still stamped in
   * {@link DimensionValueSetShardSpec#getPartitionDimensionValues()}.
   */
  @Test
  public void testAnnotateSegmentOmitsColumnTypeForStringOrSchemalessDimension() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        createDataSchemaWithPartitionDimensionSchemas(),
        null,
        null,
        null,
        Map.of("partition", "0"),
        Map.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("region", "unknown"))
    );

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();
    // Feed both tracked dimensions on each row: a row missing a tracked dimension records a null for it (for IS NULL),
    // which would leave a stray null in the other dimension's set here.
    collectRow(runner, lookupKey, Map.of("region", "us-west", "unknown", "1"));
    collectRow(runner, lookupKey, Map.of("region", "us-east", "unknown", "2"));

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    Assertions.assertFalse(
        shardSpec.getDimensionColumnTypes().containsKey("region"),
        "A StringDimensionSchema dimension must not get a column type stamp"
    );
    Assertions.assertFalse(
        shardSpec.getDimensionColumnTypes().containsKey("unknown"),
        "A dimension absent from the DimensionsSpec (schemaless) must not get a column type stamp"
    );
    // Both are still stamped with their observed values.
    Assertions.assertEquals(
        ImmutableSet.of("us-west", "us-east"),
        ImmutableSet.copyOf(shardSpec.getPartitionDimensionValues().get("region"))
    );
    Assertions.assertEquals(
        Arrays.asList("1", "2"),
        shardSpec.getPartitionDimensionValues().get("unknown")
    );
  }

  /**
   * A segment that spans a task restart has incomplete observed values, so it must NOT declare any partition filters
   * (no pruning), to avoid wrongly pruning pre-restart rows. It is still stamped with an empty-filter
   * {@link DimensionValueSetShardSpec} (not a bare {@link NumberedShardSpec}) so that all segments in an interval keep a
   * uniform shard-spec class for {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper}, which
   * rejects a publish batch mixing shard-spec classes within an interval.
   */
  @Test
  public void testRestartSpannedSegmentGetsEmptyFilterDimensionValueSetShardSpec() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"))
    );

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    // Post-restart, only tenant_c is observed; tenant_a/tenant_b live only in pre-restart hydrants.
    observe(runner, lookupKey, "tenant", "tenant_c");
    // The runner marks this segment as restored-from-disk (spans a restart).
    markRestartSpanned(runner, lookupKey);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(
        annotated.getShardSpec() instanceof DimensionValueSetShardSpec,
        "A restart-spanned segment must be stamped with a DimensionValueSetShardSpec (class-uniform with freshly-stamped "
        + "segments in the same interval) so SegmentPublisherHelper does not reject the publish"
    );
    Assertions.assertTrue(
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty(),
        "Its filters must be empty (no pruning) so incompletely-observed pre-restart rows are never pruned away"
    );
  }

  /**
   * A restart batch mixes a restart-spanned partition (empty-filter fallback) with a freshly-observed one in the same
   * interval. Both must keep a uniform shard-spec class so the publish isn't rejected.
   */
  @Test
  public void testRestartBatchMixingFallbackAndObservedSegmentsPublishesWithDimensionValueSetShardSpec()
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"))
    );

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

    Assertions.assertEquals(
        annotatedRestartSpanned.getShardSpec().getClass(),
        annotatedFreshlyObserved.getShardSpec().getClass()
    );
    Assertions.assertTrue(annotatedRestartSpanned.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assertions.assertTrue(
        ((DimensionValueSetShardSpec) annotatedRestartSpanned.getShardSpec()).getPartitionDimensionValues().isEmpty()
    );
    Assertions.assertEquals(
        List.of("tenant_a"),
        ((DimensionValueSetShardSpec) annotatedFreshlyObserved.getShardSpec()).getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * A dimension that ingested a null/missing value declares null (a null list element) alongside its non-null values,
   * so {@code IS NULL} queries are not pruned.
   */
  @Test
  public void testNullValuedDimensionDeclaresNullInPartitionDimensionValues() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant", "region"))
    );

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    // Row 1: tenant=tenant_a, region=us-west. Row 2: region=us-west but tenant missing (a null/missing tenant value).
    collectRow(runner, lookupKey, Map.of("tenant", "tenant_a", "region", "us-west"));
    collectRow(runner, lookupKey, Map.of("region", "us-west"));

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(
        annotated.getShardSpec() instanceof DimensionValueSetShardSpec
    );
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    // tenant declares both its non-null value AND null, so IS NULL queries are not pruned.
    Assertions.assertEquals(
        Arrays.asList(null, "tenant_a"),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
    Assertions.assertEquals(
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
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"))
    );

    final DataSegment segment = createSingleSegment();
    final SegmentId lookupKey = segment.getId();

    observe(runner, lookupKey, "tenant", (String) null);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    Assertions.assertEquals(
        Collections.singletonList(null),
        shardSpec.getPartitionDimensionValues().get("tenant")
    );
  }

  /**
   * Feature on, but a segment ingested no values for any tracked dimension (nothing recorded under its key). It still
   * gets an empty-filter {@link DimensionValueSetShardSpec} rather than being returned as a bare {@link NumberedShardSpec},
   * so it stays class-uniform with its interval siblings for
   * {@link org.apache.druid.segment.realtime.appenderator.SegmentPublisherHelper}.
   */
  @Test
  public void testSegmentWithNoObservedValuesGetsEmptyFilterDimensionValueSetShardSpec() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"))
    );

    // No observe(...) call: nothing was recorded for this segment.
    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(createSingleSegment());

    Assertions.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assertions.assertTrue(
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty(),
        "A segment with no observed values declares no filters (no pruning) but stays a DimensionValueSetShardSpec"
    );
  }

  /**
   * Feature off (no streamingPartitionsSpec): the segment is returned completely unchanged, retaining its original
   * shard spec.
   */
  @Test
  public void testFeatureOffReturnsSegmentUnchanged() throws Exception
  {
    // No streamingPartitionsSpec passed: the feature is off.
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100")
    );

    final DataSegment segment = createSingleSegment();
    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertSame(segment, annotated, "With the feature off the segment must be returned unchanged");
  }

  /** Boundary: observed values exactly equal the cap, dim must still stamp. */
  @Test
  public void testCapAtBoundaryStampsValuesNormally() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"), 3)
    );

    final DataSegment segment = createSingleSegment();
    observe(runner, segment.getId(), "tenant", "tenant_a", "tenant_b", "tenant_c");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assertions.assertEquals(
        Arrays.asList("tenant_a", "tenant_b", "tenant_c"),
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().get("tenant")
    );
  }

  /** Over-cap: dim is omitted from the filter map; segment still gets a DimensionValueSetShardSpec. */
  @Test
  public void testCapExceededOmitsDimensionFromFilterMap() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"), 2)
    );

    final DataSegment segment = createSingleSegment();
    observe(runner, segment.getId(), "tenant", "tenant_a", "tenant_b", "tenant_c");

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(annotated.getShardSpec() instanceof DimensionValueSetShardSpec);
    Assertions.assertTrue(
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty(),
        "Over-cap dimension must be absent from the filter map so possibleInDomain treats it as unconstrained"
    );
  }

  /** Per-dim independence: a runaway dim must not disable pruning on its under-cap siblings. */
  @Test
  public void testCapEnforcedPerDimensionIndependently() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant", "region"), 2)
    );

    final DataSegment segment = createSingleSegment();
    // Each row sets both tracked dims (collect evaluates all configured dims per row). tenant sees 3 distinct values
    // (over cap), region sees 2 (at cap).
    collectRow(runner, segment.getId(), Map.of("tenant", "tenant_a", "region", "us-west"));
    collectRow(runner, segment.getId(), Map.of("tenant", "tenant_b", "region", "us-east"));
    collectRow(runner, segment.getId(), Map.of("tenant", "tenant_c", "region", "us-west"));

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    final DimensionValueSetShardSpec shardSpec = (DimensionValueSetShardSpec) annotated.getShardSpec();
    Assertions.assertNull(
        shardSpec.getPartitionDimensionValues().get("tenant"),
        "Over-cap dim must be absent"
    );
    Assertions.assertEquals(
        Arrays.asList("us-east", "us-west"),
        shardSpec.getPartitionDimensionValues().get("region"),
        "Under-cap dim must be stamped normally"
    );
  }

  /** Null counts toward the cap like any other distinct value. */
  @Test
  public void testNullCountsTowardCap() throws Exception
  {
    final TestSeekableStreamIndexTaskRunner runner = createRunner(
        ImmutableMap.of("partition", "0"),
        ImmutableMap.of("partition", "100"),
        new DimensionValueSetPartitionsSpec(List.of("tenant"), 2)
    );

    final DataSegment segment = createSingleSegment();
    observe(runner, segment.getId(), "tenant", "tenant_a", "tenant_b", null);

    final DataSegment annotated = runner.annotateSegmentWithPartitionDimensionValues(segment);

    Assertions.assertTrue(
        ((DimensionValueSetShardSpec) annotated.getShardSpec()).getPartitionDimensionValues().isEmpty(),
        "Null counts toward the cap; over-cap dim must be omitted"
    );
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

  /**
   * Feeds the collector one row per value through its real {@link StreamingShardSpecCollector#collect} API. A
   * {@code null} value is sent as a row missing {@code dimension} (so {@code getDimension} returns empty and the
   * collector records a null), matching how a null/missing ingested value is observed in production.
   */
  private static void observe(
      SeekableStreamIndexTaskRunner runner,
      SegmentId segmentId,
      String dimension,
      String... values
  )
  {
    for (String value : values) {
      collectRow(runner, segmentId, value == null ? Map.of() : Map.of(dimension, value));
    }
  }

  /**
   * Feeds the collector a single row built from {@code event} through its real
   * {@link StreamingShardSpecCollector#collect} API. A dimension absent from {@code event} is observed as a
   * null/missing value.
   */
  private static void collectRow(
      SeekableStreamIndexTaskRunner runner,
      SegmentId segmentId,
      Map<String, Object> event
  )
  {
    final StreamingShardSpecCollector collector = Objects.requireNonNull(
        runner.getShardSpecCollector(),
        "streamingPartitionsSpec must be configured before collecting rows"
    );
    collector.collect(
        segmentId,
        new MapBasedInputRow(DateTimes.nowUtc(), new ArrayList<>(event.keySet()), event)
    );
  }

  private static void markRestartSpanned(SeekableStreamIndexTaskRunner runner, SegmentId segmentId)
  {
    Objects.requireNonNull(
        runner.getShardSpecCollector(),
        "streamingPartitionsSpec must be configured before marking restart-spanned segments"
    ).onSegmentsRestored(Collections.singletonList(segmentId));
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
    return createRunner(startOffsets, endOffsets, null);
  }

  private TestSeekableStreamIndexTaskRunner createRunner(
      Map<String, String> startOffsets,
      Map<String, String> endOffsets,
      @Nullable StreamingPartitionsSpec streamingPartitionsSpec
  )
  {
    return createRunner(createDataSchema(), null, null, null, startOffsets, endOffsets, streamingPartitionsSpec);
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
        ImmutableMap.of(),
        null
    );
  }

  private TestSeekableStreamIndexTaskRunner createRunner(
      DataSchema schema,
      @Nullable Long refreshRejectionPeriodsInMinutes,
      @Nullable DateTime minMessageTime,
      @Nullable DateTime maxMessageTime,
      Map<String, String> startOffsets,
      Map<String, String> endOffsets,
      @Nullable StreamingPartitionsSpec streamingPartitionsSpec
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
    Mockito.when(tuningConfig.getStreamingPartitionsSpec()).thenReturn(streamingPartitionsSpec);
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

  /**
   * A DataSchema declaring "tenant" as a {@link LongDimensionSchema} and "region" as a {@link StringDimensionSchema},
   * for exercising the dimensionColumnTypes stamping in {@code annotateSegmentWithPartitionDimensionValues}. Note
   * that "unknown" is deliberately absent, standing in for a schemaless/not-explicitly-declared dimension.
   */
  private static DataSchema createDataSchemaWithPartitionDimensionSchemas()
  {
    final DimensionsSpec dimensionsSpec = new DimensionsSpec(
        Arrays.asList(
            new LongDimensionSchema("tenant"),
            new StringDimensionSchema("region")
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
    Assertions.fail("Timed out waiting for status [" + status + "]");
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
      Assertions.assertTrue(possiblyPauseFuture.get(2, TimeUnit.SECONDS));
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
      // Offset ordering intentionally excludes boundary exclusivity, which value equality includes.
      // codeql[java/inconsistent-compareto-and-equals]
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
