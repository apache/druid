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

package org.apache.druid.segment;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.file.PartialSegmentDownloadListener;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;

class PartialQueryableIndexSegmentTest extends InitializedNullHandlingTest
{
  private static final ColumnConfig COLUMN_CONFIG = ColumnConfig.DEFAULT;
  private static final DateTime TIME = DateTimes.of("2025-01-01");

  // expected exact bounds: first row at TIME, last row at TIME + 3 minutes
  private static final DateTime EXPECTED_MIN_TIME = TIME;
  private static final DateTime EXPECTED_MAX_TIME = TIME.plusMinutes(3);

  private static final SegmentId SEGMENT_ID = SegmentId.of("test", Intervals.of("2025/2026"), "v1", 0);

  // The async cursor factory's executor doesn't matter for the segment-shape tests; use direct-execution so async
  // calls complete synchronously and tests stay simple. Cursor factory-specific async behavior is covered separately.
  private static final ListeningExecutorService DIRECT_EXEC =
      MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService());

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("metric1", ColumnType.LONG)
                                                                .build();

  private static final List<InputRow> ROWS = Arrays.asList(
      new ListBasedInputRow(ROW_SIGNATURE, TIME, ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", 1L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(1), ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", 2L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(2), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", 3L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(3), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", 4L))
  );

  @TempDir
  static File sharedTempDir;

  private static File segmentDir;

  @TempDir
  File perTestTempDir;

  @BeforeAll
  static void buildSegment()
  {
    final File tmpDir = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    segmentDir = IndexBuilder.create()
                             .useV10()
                             .tmpDir(tmpDir)
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(
                                 IncrementalIndexSchema.builder()
                                                       .withDimensionsSpec(
                                                           DimensionsSpec.builder()
                                                                         .setDimensions(
                                                                             List.of(
                                                                                 new StringDimensionSchema("dim1"),
                                                                                 new LongDimensionSchema("metric1")
                                                                             )
                                                                         )
                                                                         .build()
                                                       )
                                                       .withRollup(false)
                                                       .withMinTimestamp(TIME.getMillis())
                                                       .build()
                             )
                             .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                             .rows(ROWS)
                             .buildMMappedIndexFile();
  }

  private PartialQueryableIndex openIndex(CountingRangeReader rangeReader, String cacheName) throws IOException
  {
    final File cacheDir = new File(perTestTempDir, cacheName);
    FileUtils.mkdirp(cacheDir);
    final PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList(),
        PartialSegmentDownloadListener.NOOP,
        PartialSegmentFileMapperV10.DEFAULT_COALESCE_GAP_BYTES,
        PartialSegmentFileMapperV10.DEFAULT_MAX_FETCH_RUN_BYTES
    );
    return new PartialQueryableIndex(mapper.getSegmentFileMetadata(), mapper, COLUMN_CONFIG);
  }

  private static PartialQueryableIndexSegment makeSegment(PartialQueryableIndex index)
  {
    return new PartialQueryableIndexSegment(index, SEGMENT_ID, index::close, noOpAcquirer(DIRECT_EXEC));
  }

  /**
   * No-op bundle acquirer for tests: every {@code acquire(...)} returns a noop {@link Closeable};
   * {@code submitDownload(...)} runs the task on the supplied executor and wraps the future as an
   * {@link AsyncResource}. Production callers go through {@code PartialSegmentMetadataCacheEntry}'s bundle acquirer.
   */
  private static PartialBundleAcquirer noOpAcquirer(ListeningExecutorService downloadExec)
  {
    return new PartialBundleAcquirer()
    {
      @Override
      public Closeable acquire(String bundleName)
      {
        return () -> {};
      }

      @Override
      public <T> AsyncResource<T> submitDownload(Callable<T> task)
      {
        return AsyncResources.fromFutureUnmanaged(downloadExec.submit(task));
      }
    };
  }

  @Test
  void testAsTimeBoundaryInspectorReturnsV10Inspector() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final PartialQueryableIndex index = openIndex(rangeReader, "boundary");

    try (Segment segment = makeSegment(index)) {
      final TimeBoundaryInspector inspector = segment.as(TimeBoundaryInspector.class);
      Assertions.assertNotNull(inspector);
      Assertions.assertInstanceOf(V10TimeBoundaryInspector.class, inspector);
    }
  }

  @Test
  void testTimeBoundaryAnsweredFromMetadataWithoutColumnDownloads() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final PartialQueryableIndex index = openIndex(rangeReader, "no_download");
    // header fetch happens during mapper creation; reset so we observe only what time-boundary lookups trigger
    rangeReader.resetCount();

    try (Segment segment = makeSegment(index)) {
      final TimeBoundaryInspector inspector = segment.as(TimeBoundaryInspector.class);
      Assertions.assertTrue(inspector.isMinMaxExact());
      Assertions.assertEquals(EXPECTED_MIN_TIME, inspector.getMinTime());
      Assertions.assertEquals(EXPECTED_MAX_TIME, inspector.getMaxTime());
      Assertions.assertEquals(
          0,
          rangeReader.getReadCount(),
          "metadata-only inspector must not trigger any column downloads"
      );
      Assertions.assertFalse(
          rangeReader.getReadFilenames().contains(ColumnHolder.TIME_COLUMN_NAME),
          "must not download the __time column"
      );
    }
  }

  @Test
  void testAsRowCountInspectorReturnsV10Inspector() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final PartialQueryableIndex index = openIndex(rangeReader, "row_count");

    try (Segment segment = makeSegment(index)) {
      final RowCountInspector inspector = segment.as(RowCountInspector.class);
      Assertions.assertNotNull(inspector);
      Assertions.assertInstanceOf(V10RowCountInspector.class, inspector);
    }
  }

  @Test
  void testRowCountAnsweredFromMetadataWithoutColumnDownloads() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final PartialQueryableIndex index = openIndex(rangeReader, "row_count_no_download");
    // header fetch happens during mapper creation; reset so we observe only what the row-count lookup triggers
    rangeReader.resetCount();

    try (Segment segment = makeSegment(index)) {
      final RowCountInspector inspector = segment.as(RowCountInspector.class);
      Assertions.assertEquals(ROWS.size(), inspector.getNumRows());
      Assertions.assertEquals(
          0,
          rangeReader.getReadCount(),
          "metadata-only row count must not trigger any column downloads"
      );
    }
  }

  @Test
  void testAsQueryableIndexReturnsUnderlyingIndex() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final PartialQueryableIndex index = openIndex(rangeReader, "as_index");

    try (Segment segment = makeSegment(index)) {
      Assertions.assertSame(index, segment.as(QueryableIndex.class));
    }
  }

  @Test
  void testAsCursorFactoryReturnsNonNull() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final PartialQueryableIndex index = openIndex(rangeReader, "cursor");

    try (Segment segment = makeSegment(index)) {
      final CursorFactory factory = segment.as(CursorFactory.class);
      Assertions.assertNotNull(factory);
      Assertions.assertInstanceOf(PartialQueryableIndexCursorFactory.class, factory);
    }
  }

  @Test
  void testIdAndDataIntervalPassThrough() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final PartialQueryableIndex index = openIndex(rangeReader, "id");

    try (Segment segment = makeSegment(index)) {
      Assertions.assertEquals(SEGMENT_ID, segment.getId());
      Assertions.assertEquals(index.getDataInterval(), segment.getDataInterval());
    }
  }
}
