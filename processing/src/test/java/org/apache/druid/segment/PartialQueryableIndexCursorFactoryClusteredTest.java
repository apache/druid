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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.ClusteredValueGroupsBaseTableProjectionSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Partial (on-demand) download coverage for a clustered base-table V10 segment. Cluster groups are resolved from
 * metadata only ({@code planClusterGroupQuery}); the async cursor factory downloads one bundle ({@code __base$<ids>})
 * per surviving group.
 */
class PartialQueryableIndexCursorFactoryClusteredTest extends PartialQueryableIndexCursorFactoryTestBase
{
  private static final long T0 = DateTimes.of("2025-01-01").getMillis();

  // tenants sort to dictionary ids acme=0, globex=1 → bundles __base$0 (2 rows) and __base$1 (1 row)
  private static final String ACME_BUNDLE = "__base$0";
  private static final String GLOBEX_BUNDLE = "__base$1";

  @TempDir
  static File sharedTempDir;

  private static File segmentDir;

  @BeforeAll
  static void buildSegment()
  {
    final ClusteredValueGroupsBaseTableProjectionSpec clusterSpec =
        ClusteredValueGroupsBaseTableProjectionSpec.builder()
            .columns(
                new StringDimensionSchema("tenant"),
                new StringDimensionSchema("region"),
                new LongDimensionSchema("__time")
            )
            .clusteringColumns("tenant")
            .build();
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withMinTimestamp(T0)
                              .withTimestampSpec(new TimestampSpec("ts", "millis", null))
                              .withQueryGranularity(Granularities.NONE)
                              .withDimensionsSpec(clusterSpec.getDimensionsSpec())
                              .withRollup(false)
                              .withClusterSpec(clusterSpec)
                              .build();
    final File tmpDir = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    segmentDir = IndexBuilder.create()
                             .useV10()
                             .tmpDir(tmpDir)
                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                             .schema(schema)
                             .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.NONE).build())
                             // ingest out of clustering order; the writer sorts groups by clustering value
                             .rows(List.of(
                                 row(T0 + 2, "globex", "eu-west-1"),
                                 row(T0, "acme", "us-east-1"),
                                 row(T0 + 1, "acme", "us-west-2")
                             ))
                             .buildMMappedIndexFile();
  }

  @Test
  void testFilterOnClusteringColumnDownloadsOnlySurvivingGroup() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "filter_acme")) {
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());
      final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                  .setFilter(new EqualityFilter("tenant", ColumnType.STRING, "acme", null))
                                                  .build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        // Only the acme group survives the clustering-column filter, so only its rows are returned.
        Assertions.assertEquals(
            List.of(List.of("acme", "us-east-1"), List.of("acme", "us-west-2")),
            scanTenantRegion(holder)
        );

        final Set<String> downloaded = opened.mapper().getDownloadedFiles();
        // The acme group's columns were materialized.
        Assertions.assertTrue(downloaded.contains(ACME_BUNDLE + "/region"), "got: " + downloaded);
        Assertions.assertTrue(downloaded.contains(ACME_BUNDLE + "/__time"), "got: " + downloaded);
        // The globex group bundle was pruned by metadata-only resolution and never touched.
        Assertions.assertTrue(
            downloaded.stream().noneMatch(f -> f.startsWith(GLOBEX_BUNDLE + "/")),
            "globex group must not be downloaded; got: " + downloaded
        );
      }
    }
  }

  @Test
  void testFilterMatchingNoGroupDownloadsNothing() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "filter_none")) {
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());
      final CursorBuildSpec spec = CursorBuildSpec.builder()
                                                  .setFilter(new EqualityFilter("tenant", ColumnType.STRING, "nobody", null))
                                                  .build();

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(spec);
           CursorHolder holder = asyncHolder.release()) {
        Assertions.assertTrue(scanTenantRegion(holder).isEmpty(), "no group should survive");
        Assertions.assertTrue(
            opened.mapper().getDownloadedFiles().isEmpty(),
            "no group bundle should be downloaded when nothing survives; got: " + opened.mapper().getDownloadedFiles()
        );
      }
    }
  }

  @Test
  void testFullScanConcatenatesAllGroups() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "full_scan")) {
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());

      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
           CursorHolder holder = asyncHolder.release()) {
        // Groups are concatenated in clustering (dictionary id) order: acme then globex, clustering constant injected.
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2"),
                List.of("globex", "eu-west-1")
            ),
            scanTenantRegion(holder)
        );

        final Set<String> downloaded = opened.mapper().getDownloadedFiles();
        Assertions.assertTrue(downloaded.contains(ACME_BUNDLE + "/region"), "got: " + downloaded);
        Assertions.assertTrue(downloaded.contains(GLOBEX_BUNDLE + "/region"), "got: " + downloaded);
      }
    }
  }

  @Test
  void testSyncMakeCursorHolderAfterFullDownload() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "sync_full")) {
      final PartialSegmentFileMapperV10 mapper = opened.mapper();
      final PartialQueryableIndexCursorFactory factory = factory(opened.index());

      // Sync path refuses until everything is resident.
      Assertions.assertThrows(
          DruidException.class,
          () -> factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)
      );

      // Eager-acquire equivalent: materialize every internal file, then the sync clustered path works via the delegate.
      mapper.fetchFiles(mapper.getSegmentFileMetadata().getFiles().keySet());
      Assertions.assertTrue(mapper.isFullyDownloaded());
      try (CursorHolder holder = factory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2"),
                List.of("globex", "eu-west-1")
            ),
            scanTenantRegion(holder)
        );
      }
    }
  }

  @Test
  void testAsyncMultiGroupDefersDownloadUntilExecutorRuns() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-clustered-defer-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "async_defer")) {
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          opened.index(),
          QueryableIndexTimeBoundaryInspector.create(opened.index()),
          noOpAcquirer(gatedExec)
      );
      rangeReader.resetCount();

      // Block the download executor so no group download can start yet.
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      // FULL_SCAN survives both groups → both bundles must download before the holder is ready.
      final AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
      Assertions.assertFalse(asyncHolder.isReady(), "must wait while the download executor is blocked");
      Assertions.assertEquals(0, rangeReader.getReadCount(), "no group download should have started yet");

      final CountDownLatch ready = new CountDownLatch(1);
      asyncHolder.addReadyCallback(ready::countDown);
      gate.countDown();
      Assertions.assertTrue(ready.await(15, TimeUnit.SECONDS), "ready callback must fire once downloads run");
      Assertions.assertTrue(asyncHolder.isReady());

      try (CursorHolder holder = asyncHolder.release()) {
        Assertions.assertEquals(
            List.of(
                List.of("acme", "us-east-1"),
                List.of("acme", "us-west-2"),
                List.of("globex", "eu-west-1")
            ),
            scanTenantRegion(holder)
        );
      }
    }
    finally {
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testMultiGroupSuccessCloseReleasesEveryBundleHold() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(directExec());
    try (IndexAndMapper opened = openIndex(rangeReader, "release_success")) {
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          opened.index(),
          QueryableIndexTimeBoundaryInspector.create(opened.index()),
          acquirer
      );

      // FULL_SCAN survives both groups → both bundles acquired during the build; with the direct executor the
      // downloads run inline so the holder is immediately ready.
      try (AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN)) {
        Assertions.assertTrue(asyncHolder.isReady());
        Assertions.assertEquals(1, acquirer.acquiredCount(ACME_BUNDLE), "acme group bundle acquired");
        Assertions.assertEquals(1, acquirer.acquiredCount(GLOBEX_BUNDLE), "globex group bundle acquired");

        final CursorHolder holder = asyncHolder.release();
        // The produced holder owns every group's hold; nothing is released until it closes.
        Assertions.assertEquals(0, acquirer.releasedCount(ACME_BUNDLE));
        Assertions.assertEquals(0, acquirer.releasedCount(GLOBEX_BUNDLE));

        holder.close();
        // Closing the holder fans out the release across all surviving groups' bundle holds.
        Assertions.assertEquals(1, acquirer.releasedCount(ACME_BUNDLE), "acme hold released on holder close");
        Assertions.assertEquals(1, acquirer.releasedCount(GLOBEX_BUNDLE), "globex hold released on holder close");
      }
    }
  }

  @Test
  void testMultiGroupCancelBeforeReadyReleasesEveryBundleHold() throws Exception
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    final CountDownLatch gate = new CountDownLatch(1);
    final ExecutorService rawExec = Execs.singleThreaded("partial-clustered-cancel-%d");
    final ListeningExecutorService gatedExec = MoreExecutors.listeningDecorator(rawExec);
    final CountingBundleAcquirer acquirer = new CountingBundleAcquirer(gatedExec);
    try (IndexAndMapper opened = openIndex(rangeReader, "release_cancel")) {
      final PartialQueryableIndexCursorFactory factory = new PartialQueryableIndexCursorFactory(
          opened.index(),
          QueryableIndexTimeBoundaryInspector.create(opened.index()),
          acquirer
      );

      // Block the download executor so the per-column download bodies stay queued and the holder never becomes ready.
      @SuppressWarnings("unused")
      ListenableFuture<?> unused = gatedExec.submit(() -> {
        try {
          gate.await();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });

      final AsyncCursorHolder asyncHolder = factory.makeCursorHolderAsync(CursorBuildSpec.FULL_SCAN);
      // Both surviving groups' bundles are acquired synchronously during the build, before any download runs.
      Assertions.assertEquals(1, acquirer.acquiredCount(ACME_BUNDLE), "acme group bundle acquired");
      Assertions.assertEquals(1, acquirer.acquiredCount(GLOBEX_BUNDLE), "globex group bundle acquired");
      Assertions.assertFalse(asyncHolder.isReady(), "must wait while the download executor is blocked");
      Assertions.assertEquals(0, acquirer.releasedCount(ACME_BUNDLE));
      Assertions.assertEquals(0, acquirer.releasedCount(GLOBEX_BUNDLE));

      // Cancel before the holder is ready (query cancel/timeout). No download body has run, so the per-bundle
      // handshake releases each hold immediately, and the canceler must fan the release out across every group.
      asyncHolder.close();
      Assertions.assertEquals(1, acquirer.releasedCount(ACME_BUNDLE), "acme hold released on cancel");
      Assertions.assertEquals(1, acquirer.releasedCount(GLOBEX_BUNDLE), "globex hold released on cancel");
    }
    finally {
      gate.countDown();
      gatedExec.shutdownNow();
      rawExec.shutdownNow();
    }
  }

  @Test
  void testClusteredColumnCapabilitiesAreMetadataOnly() throws IOException
  {
    final CountingRangeReader rangeReader = new CountingRangeReader(segmentDir);
    try (IndexAndMapper opened = openIndex(rangeReader, "capabilities")) {
      final PartialQueryableIndex index = opened.index();
      // clustering column: type comes from the clustered summary's typed signature
      Assertions.assertEquals(ValueType.STRING, index.getColumnCapabilities("tenant").getType());
      // per-group data columns: type comes from the first group's ColumnDescriptor, never a group sub-index download
      Assertions.assertEquals(ValueType.STRING, index.getColumnCapabilities("region").getType());
      Assertions.assertEquals(ValueType.LONG, index.getColumnCapabilities("__time").getType());
      // an unknown column resolves to null without touching the file mapper
      Assertions.assertNull(index.getColumnCapabilities("nonexistent"));

      Assertions.assertTrue(
          opened.mapper().getDownloadedFiles().isEmpty(),
          "column capabilities must be answered from metadata with no download; got: "
          + opened.mapper().getDownloadedFiles()
      );
    }
  }

  private PartialQueryableIndexCursorFactory factory(PartialQueryableIndex index)
  {
    return new PartialQueryableIndexCursorFactory(
        index,
        QueryableIndexTimeBoundaryInspector.create(index),
        noOpAcquirer(directExec())
    );
  }

  /**
   * Walk (tenant, region) pairs out of a clustered cursor holder. {@code tenant} is the clustering column, injected as
   * a constant by the cluster-group cursor path; {@code region} is the per-group physical column.
   */
  private static List<List<String>> scanTenantRegion(CursorHolder holder)
  {
    final Cursor cursor = holder.asCursor();
    final List<List<String>> out = new ArrayList<>();
    if (cursor == null) {
      return out;
    }
    final DimensionSelector tenantSel =
        cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("tenant"));
    final DimensionSelector regionSel =
        cursor.getColumnSelectorFactory().makeDimensionSelector(DefaultDimensionSpec.of("region"));
    while (!cursor.isDone()) {
      final String tenant = tenantSel.getRow().size() == 0 ? null : tenantSel.lookupName(tenantSel.getRow().get(0));
      final String region = regionSel.getRow().size() == 0 ? null : regionSel.lookupName(regionSel.getRow().get(0));
      out.add(Arrays.asList(tenant, region));
      cursor.advance();
    }
    return out;
  }

  private static InputRow row(long ts, String tenant, String region)
  {
    final Map<String, Object> event = new HashMap<>();
    event.put("ts", ts);
    event.put("tenant", tenant);
    event.put("region", region);
    return new MapBasedInputRow(ts, List.of("tenant", "region"), event);
  }

  /**
   * A {@link PartialBundleAcquirer} that counts how many times each bundle is acquired and released, so a test can
   * assert the cursor factory holds (and ultimately releases) one hold per surviving cluster group. Downloads are
   * submitted to the supplied executor (direct for the success path, gated for the cancel path).
   */
  private static final class CountingBundleAcquirer implements PartialBundleAcquirer
  {
    private final ListeningExecutorService downloadExec;
    private final Map<String, Integer> acquired = new ConcurrentHashMap<>();
    private final Map<String, Integer> released = new ConcurrentHashMap<>();

    private CountingBundleAcquirer(ListeningExecutorService downloadExec)
    {
      this.downloadExec = downloadExec;
    }

    @Override
    public Closeable acquire(String bundleName)
    {
      acquired.merge(bundleName, 1, Integer::sum);
      return () -> released.merge(bundleName, 1, Integer::sum);
    }

    @Override
    public <T> AsyncResource<T> submitDownload(Callable<T> task)
    {
      return AsyncResources.fromFutureUnmanaged(downloadExec.submit(task));
    }

    private int acquiredCount(String bundleName)
    {
      return acquired.getOrDefault(bundleName, 0);
    }

    private int releasedCount(String bundleName)
    {
      return released.getOrDefault(bundleName, 0);
    }
  }
}
