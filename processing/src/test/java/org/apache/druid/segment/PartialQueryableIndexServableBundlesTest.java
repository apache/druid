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
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Coverage for planning and serving a {@link PartialQueryableIndex} restricted to a set of servable bundles (what
 * {@link org.apache.druid.segment.loading.AcquireMode#PARTIAL_RULE_HELD} uses), over a plain (non-clustered) segment
 * with two aggregate projections that can both answer the same query. The clustered cases live in
 * {@link PartialQueryableIndexCursorFactoryClusteredTest} and
 * {@link PartialQueryableIndexCursorFactoryClusteredProjectionTest}.
 */
class PartialQueryableIndexServableBundlesTest extends PartialQueryableIndexCursorFactoryTestBase
{
  private static final DateTime TIME = DateTimes.of("2025-01-01");
  private static final String BASE = Projections.BASE_TABLE_PROJECTION_NAME;
  // 2 rows (one per dim1 value), so it sorts ahead of BIG_PROJECTION when both match
  private static final String SMALL_PROJECTION = "dim1_count";
  // 4 rows (one per dim1, metric1 pair)
  private static final String BIG_PROJECTION = "dim1_metric1_count";

  private static final RowSignature ROW_SIGNATURE = RowSignature.builder()
                                                                .add("dim1", ColumnType.STRING)
                                                                .add("metric1", ColumnType.LONG)
                                                                .build();

  private static final List<AggregateProjectionSpec> PROJECTIONS = List.of(
      AggregateProjectionSpec.builder(SMALL_PROJECTION)
                             .groupingColumns(new StringDimensionSchema("dim1"))
                             .aggregators(new CountAggregatorFactory("_count"))
                             .build(),
      AggregateProjectionSpec.builder(BIG_PROJECTION)
                             .groupingColumns(new StringDimensionSchema("dim1"), new LongDimensionSchema("metric1"))
                             .aggregators(new CountAggregatorFactory("_count"))
                             .build()
  );

  private static final List<InputRow> ROWS = Arrays.asList(
      new ListBasedInputRow(ROW_SIGNATURE, TIME, ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", 1L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(1), ROW_SIGNATURE.getColumnNames(), Arrays.asList("a", 2L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(2), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", 3L)),
      new ListBasedInputRow(ROW_SIGNATURE, TIME.plusMinutes(3), ROW_SIGNATURE.getColumnNames(), Arrays.asList("b", 4L))
  );

  // group by dim1 + count, which both projections can answer
  private static final CursorBuildSpec COUNT_BY_DIM1 =
      CursorBuildSpec.builder()
                     .setGroupingColumns(List.of("dim1"))
                     .setAggregators(List.of(new CountAggregatorFactory("_count")))
                     .setPhysicalColumns(Set.of("dim1"))
                     .build();

  @RegisterExtension
  public static final TemporaryFolderExtension SHARED_TEMPORARY_FOLDER = TemporaryFolderExtension.classScoped();

  private static File segmentDir;

  @BeforeAll
  static void buildSegment() throws IOException
  {
    final File tmpDir = SHARED_TEMPORARY_FOLDER.newFolder("build_" + ThreadLocalRandom.current().nextInt());
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
                                                       .withProjections(PROJECTIONS)
                                                       .build()
                             )
                             .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                             .rows(ROWS)
                             .buildMMappedIndexFile();
  }

  @Test
  void testUnrestrictedMatchesSmallestProjection() throws IOException
  {
    try (IndexAndMapper opened = openIndex(new CountingRangeReader(segmentDir), "unrestricted")) {
      final PartialQueryableIndex.CursorPrefetchPlan plan = opened.index().planCursorPrefetch(COUNT_BY_DIM1, null);
      Assertions.assertEquals(SMALL_PROJECTION, plan.matchedProjection().getName());
    }
  }

  @Test
  void testUnservedBestProjectionFallsBackToServedProjection() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "fallback_projection")) {
      final PartialQueryableIndex.CursorPrefetchPlan plan =
          opened.index().planCursorPrefetch(COUNT_BY_DIM1, BIG_PROJECTION::equals);
      Assertions.assertEquals(BIG_PROJECTION, plan.matchedProjection().getName());
      // the base parent of the dim1 projection column is a dependency of the projection, so it's planned even though
      // the base table itself is not servable
      Assertions.assertEquals(List.of(BIG_PROJECTION, BASE), bundleNames(plan));

      final RecordingAcquirer acquirer = new RecordingAcquirer(directExec());
      final PartialQueryableIndexCursorFactory factory = factory(opened.index(), acquirer, BIG_PROJECTION::equals);
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(COUNT_BY_DIM1);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertEquals(4, countRows(holder), "the big projection has one row per (dim1, metric1)");
      }
      Assertions.assertEquals(new TreeSet<>(Set.of(BIG_PROJECTION, BASE)), acquirer.acquired);
      final Set<String> downloaded = opened.mapper().getDownloadedFiles();
      Assertions.assertTrue(
          downloaded.stream().noneMatch(f -> f.startsWith(SMALL_PROJECTION + "/")),
          "the unserved projection must not download; got: " + downloaded
      );
    }
  }

  @Test
  void testNoServedProjectionFallsBackToServedBaseTable() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "fallback_base")) {
      final RecordingAcquirer acquirer = new RecordingAcquirer(directExec());
      final PartialQueryableIndexCursorFactory factory = factory(opened.index(), acquirer, BASE::equals);
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(COUNT_BY_DIM1);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertEquals(4, countRows(holder), "the base table has every ingested row");
      }
      Assertions.assertEquals(new TreeSet<>(Set.of(BASE)), acquirer.acquired);
      final Set<String> downloaded = opened.mapper().getDownloadedFiles();
      Assertions.assertTrue(
          downloaded.stream().allMatch(f -> f.startsWith(BASE + "/")),
          "only base table files may download; got: " + downloaded
      );
    }
  }

  @Test
  void testUnservedBaseTableReadsNothing() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "unserved_base")) {
      // a projection is servable, but a non-aggregate scan can only be answered by the base table, which is not
      Assertions.assertTrue(
          opened.index().planCursorPrefetch(CursorBuildSpec.FULL_SCAN, SMALL_PROJECTION::equals).bundles().isEmpty()
      );
      final RecordingAcquirer acquirer = new RecordingAcquirer(directExec());
      final PartialQueryableIndexCursorFactory factory = factory(opened.index(), acquirer, SMALL_PROJECTION::equals);
      rangeReader.resetCount();
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN)) {
        Assertions.assertTrue(asyncHolder.isReady(), "nothing to download, so the holder is ready immediately");
        try (CursorHolder holder = asyncHolder.release()) {
          Assertions.assertEquals(0, countRows(holder));
        }
      }
      Assertions.assertTrue(acquirer.acquired.isEmpty(), "got: " + acquirer.acquired);
      Assertions.assertEquals(0, rangeReader.getReadCount());
      Assertions.assertTrue(opened.mapper().getDownloadedFiles().isEmpty());
    }
  }

  @Test
  void testNothingServableReadsNothing() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "nothing_servable")) {
      final RecordingAcquirer acquirer = new RecordingAcquirer(directExec());
      final PartialQueryableIndexCursorFactory factory = factory(opened.index(), acquirer, bundle -> false);
      rangeReader.resetCount();
      for (CursorBuildSpec spec : List.of(COUNT_BY_DIM1, CursorBuildSpec.FULL_SCAN)) {
        try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
             CursorHolder holder = asyncHolder.release()) {
          Assertions.assertEquals(0, countRows(holder));
        }
      }
      Assertions.assertTrue(acquirer.acquired.isEmpty(), "got: " + acquirer.acquired);
      Assertions.assertEquals(0, rangeReader.getReadCount());
    }
  }

  @Test
  void testSyncPathHonorsServableBundles() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "sync_restricted")) {
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final PartialQueryableIndexCursorFactory factory =
          factory(opened.index(), new RecordingAcquirer(directExec()), BIG_PROJECTION::equals);

      // still refuses to download on the sync path
      Assertions.assertThrows(DruidException.class, () -> factory.makeCursorHolder(COUNT_BY_DIM1));

      mapper.fetchFiles(mapper.getSegmentFileMetadata().getFiles().keySet());
      Assertions.assertTrue(mapper.isFullyDownloaded());
      try (CursorHolder holder = factory.makeCursorHolder(COUNT_BY_DIM1)) {
        Assertions.assertEquals(4, countRows(holder), "served from the big projection, not the unserved small one");
      }
      try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Assertions.assertEquals(0, countRows(holder), "the base table is not servable, even though it is resident");
      }
    }
  }

  @Test
  void testUseProjectionNamingUnservedProjectionFails() throws IOException
  {
    try (IndexAndMapper opened = openIndex(new CountingRangeReader(segmentDir), "use_projection")) {
      final CursorBuildSpec spec = CursorBuildSpec.builder(COUNT_BY_DIM1)
                                                  .setQueryContext(
                                                      QueryContext.of(
                                                          Map.of(QueryContexts.USE_PROJECTION, SMALL_PROJECTION)
                                                      )
                                                  )
                                                  .build();
      Assertions.assertEquals(
          SMALL_PROJECTION,
          opened.index().planCursorPrefetch(spec, null).matchedProjection().getName()
      );
      final DruidException e = Assertions.assertThrows(
          DruidException.class,
          () -> opened.index().planCursorPrefetch(spec, BIG_PROJECTION::equals)
      );
      Assertions.assertTrue(e.getMessage().contains("does not satisfy query"), e.getMessage());
    }
  }

  private static PartialQueryableIndexCursorFactory factory(
      PartialQueryableIndex index,
      PartialBundleAcquirer acquirer,
      Predicate<String> servableBundles
  )
  {
    return new PartialQueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index),
        acquirer,
        servableBundles
    );
  }

  private static List<String> bundleNames(PartialQueryableIndex.CursorPrefetchPlan plan)
  {
    return plan.bundles()
               .stream()
               .map(PartialQueryableIndex.PrefetchBundle::bundleName)
               .collect(Collectors.toList());
  }

  private static int countRows(CursorHolder holder)
  {
    final Cursor cursor = holder.asCursor();
    if (cursor == null) {
      return 0;
    }
    int rows = 0;
    while (!cursor.isDone()) {
      rows++;
      cursor.advance();
    }
    return rows;
  }

  /**
   * Records the bundles the cursor factory acquires; holds are no-ops.
   */
  private static final class RecordingAcquirer implements PartialBundleAcquirer
  {
    private final ListeningExecutorService downloadExec;
    private final Set<String> acquired = new TreeSet<>();

    private RecordingAcquirer(ListeningExecutorService downloadExec)
    {
      this.downloadExec = downloadExec;
    }

    @Override
    public Closeable acquire(String bundleName)
    {
      acquired.add(bundleName);
      return () -> {};
    }

    @Override
    public <T> AsyncResource<T> submitDownload(Callable<T> task)
    {
      return AsyncResources.fromFutureUnmanaged(downloadExec.submit(task));
    }
  }
}
