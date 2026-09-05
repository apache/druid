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

package org.apache.druid.msq.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.RowCountInspector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.loading.AcquireMode;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.LeastBytesUsedStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLoadingThreadPool;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests for {@link RegularLoadableSegment}.
 */
class RegularLoadableSegmentTest extends InitializedNullHandlingTest
{
  private static final String DATASOURCE = "foo";
  private static final int NUM_SEGMENTS = 10;
  private static final int THREADS = 8;
  private static File SEGMENT_ZIP_FILE;

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  private Path tempDir;

  private List<DataSegment> segments;
  private File cacheDir;
  private File preLoadCacheDir;
  private SegmentManager segmentManagerDynamic;
  private SegmentManager segmentManagerPreLoad;
  private ListeningExecutorService exec;

  @BeforeAll
  public static void setupStatic(@TempDir Path tempDir) throws IOException
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    final File segmentFile = TestIndex.persist(
        TestIndex.getIncrementalTestIndex(),
        IndexSpec.getDefault(),
        tempDir.resolve("segment").toFile()
    );
    final File zipPath = tempDir.resolve("zip").toFile();
    FileUtils.mkdirp(zipPath);
    SEGMENT_ZIP_FILE = new File(zipPath, "index.zip");
    CompressionUtils.zip(segmentFile, SEGMENT_ZIP_FILE);
  }

  @BeforeEach
  public void setUp() throws Exception
  {
    tempDir = temporaryFolder.getRoot().toPath();
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    jsonMapper.registerSubtypes(TestLoadSpec.class);
    jsonMapper.registerModule(new SegmentizerModule());
    jsonMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), jsonMapper)
            .addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
            .addValue(IndexIO.class, TestIndex.INDEX_IO)
    );

    segments = new ArrayList<>();

    for (int i = 0; i < NUM_SEGMENTS; i++) {
      // Two segments per interval; helps verify that directory creation + deletion does not include races.
      final DateTime startTime = DateTimes.of("2000").plusDays(i / 2);
      final int partitionNum = i % 2;

      segments.add(
          DataSegment.builder()
                     .dataSource(DATASOURCE)
                     .interval(
                         Intervals.utc(
                             startTime.getMillis(),
                             startTime.plusDays(1).getMillis()
                         )
                     )
                     .version("0")
                     .shardSpec(new NumberedShardSpec(partitionNum, 2))
                     .loadSpec(
                         jsonMapper.convertValue(
                             new TestLoadSpec(i),
                             JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
                         )
                     )
                     .size(1)
                     .build()
      );
    }

    // SegmentManager with virtualStorage for dynamically-loaded data tests
    cacheDir = tempDir.resolve("cache").toFile();
    final SegmentLoaderConfig virtualLoaderConfig = SegmentLoaderConfig.builder()
        .locations(new StorageLocationConfig(cacheDir, 10_000_000_000L, null))
        .virtualStorage(true)
        .virtualStorageIsEphemeral(true)
        .virtualStoragePartialDownloadsEnabled(true)
        .build();
    final List<StorageLocation> virtualLocations = virtualLoaderConfig.toStorageLocations();
    segmentManagerDynamic = new SegmentManager(
        new SegmentLocalCacheManager(
            virtualLocations,
            virtualLoaderConfig,
            StorageLoadingThreadPool.createFromConfig(virtualLoaderConfig),
            new LeastBytesUsedStorageLocationSelectorStrategy(virtualLocations),
            TestIndex.INDEX_IO,
            jsonMapper
        )
    );

    // SegmentManager without virtualStorage for pre-loaded data tests
    preLoadCacheDir = tempDir.resolve("localCache").toFile();
    final SegmentLoaderConfig localLoaderConfig = SegmentLoaderConfig.builder()
        .locations(new StorageLocationConfig(preLoadCacheDir, 10_000_000_000L, null))
        .build();
    final List<StorageLocation> localLocations = localLoaderConfig.toStorageLocations();
    segmentManagerPreLoad = new SegmentManager(
        new SegmentLocalCacheManager(
            localLocations,
            localLoaderConfig,
            StorageLoadingThreadPool.createFromConfig(localLoaderConfig),
            new LeastBytesUsedStorageLocationSelectorStrategy(localLocations),
            TestIndex.INDEX_IO,
            jsonMapper
        )
    );

    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(THREADS, getClass().getSimpleName() + "-%s"));
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    if (exec != null) {
      exec.shutdownNow();
      if (!exec.awaitTermination(1, TimeUnit.MINUTES)) {
        throw new ISE("exec termination timed out");
      }
    }
  }

  @Test
  public void test_concurrency_dynamic()
  {
    final int iterations = 1000;
    final List<ListenableFuture<Boolean>> testFutures = new ArrayList<>();
    final TestCoordinatorClientImpl coordinatorClient = new TestCoordinatorClientImpl();

    for (int i = 0; i < iterations; i++) {
      final int expectedSegmentNumber = i % NUM_SEGMENTS;
      final DataSegment segment = segments.get(expectedSegmentNumber);

      // Create RegularLoadableSegment with CoordinatorClient (no local timeline)
      final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
          segmentManagerDynamic,
          segment.getId(),
          segment.toDescriptor(),
          null,
          coordinatorClient,
          false
      );

      final ListenableFuture<LoadableSegment> f = exec.submit(() -> loadableSegment);

      testFutures.add(
          FutureUtils.transform(
              FutureUtils.transformAsync(
                  f,
                  ls -> {
                    final AcquireSegmentAction acquireAction = ls.acquire(AcquireMode.PARTIAL);
                    final SettableFuture<AcquireSegmentAction> ready = SettableFuture.create();
                    acquireAction.addReadyCallback(() -> ready.set(acquireAction));
                    return ready;
                  }
              ),
              (AcquireSegmentAction action) -> {
                final AcquireSegmentResult acquireResult = action.release();
                final Optional<Segment> acquiredSegmentOptional = acquireResult.getSegment();
                Assertions.assertTrue(acquiredSegmentOptional.isPresent());

                // closing the action after release is a no-op; the segment carries all cleanup
                try (final AcquireSegmentAction ignored = action;
                     final Segment acquiredSegment = acquiredSegmentOptional.get()) {
                  Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
                  RowCountInspector gadget = acquiredSegment.as(RowCountInspector.class);
                  Assertions.assertNotNull(gadget);
                  Assertions.assertEquals(1209, gadget.getNumRows());
                  return true;
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
          )
      );
    }

    Assertions.assertEquals(iterations, testFutures.size());
    for (int i = 0; i < iterations; i++) {
      ListenableFuture<Boolean> testFuture = testFutures.get(i);
      Assertions.assertTrue(FutureUtils.getUnchecked(testFuture, false), "Test iteration #" + i);
    }

    // Cache dir should exist, but be (mostly) empty, since we've closed all segments.
    Assertions.assertTrue(cacheDir.exists());
    Assertions.assertEquals(Set.of("info_dir", "__drop"), Set.of(cacheDir.list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(cacheDir, "__drop").list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(cacheDir, "info_dir").list()));
  }

  @Test
  public void test_concurrency_preLoaded() throws SegmentLoadingException, IOException
  {
    // First, add all segments to the SegmentManager's timeline
    for (DataSegment segment : segments) {
      segmentManagerPreLoad.loadSegment(segment);
    }

    final int iterations = 1000;
    final List<ListenableFuture<Boolean>> testFutures = new ArrayList<>();

    for (int i = 0; i < iterations; i++) {
      final int expectedSegmentNumber = i % NUM_SEGMENTS;
      final DataSegment segment = segments.get(expectedSegmentNumber);

      // Create RegularLoadableSegment without CoordinatorClient (using local timeline)
      final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
          segmentManagerPreLoad,
          segment.getId(),
          segment.toDescriptor(),
          null,
          null,
          false
      );

      final ListenableFuture<LoadableSegment> f = exec.submit(() -> loadableSegment);

      testFutures.add(
          FutureUtils.transform(
              FutureUtils.transformAsync(
                  f,
                  ls -> {
                    final AcquireSegmentAction acquireAction = ls.acquire(AcquireMode.PARTIAL);
                    final SettableFuture<AcquireSegmentAction> ready = SettableFuture.create();
                    acquireAction.addReadyCallback(() -> ready.set(acquireAction));
                    return ready;
                  }
              ),
              (AcquireSegmentAction action) -> {
                final AcquireSegmentResult acquireResult = action.release();
                final Optional<Segment> acquiredSegmentOptional = acquireResult.getSegment();
                Assertions.assertTrue(acquiredSegmentOptional.isPresent());

                // closing the action after release is a no-op; the segment carries all cleanup
                try (final AcquireSegmentAction ignored = action;
                     final Segment acquiredSegment = acquiredSegmentOptional.get()) {
                  Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
                  RowCountInspector gadget = acquiredSegment.as(RowCountInspector.class);
                  Assertions.assertNotNull(gadget);
                  Assertions.assertEquals(1209, gadget.getNumRows());
                  return true;
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
          )
      );
    }

    Assertions.assertEquals(iterations, testFutures.size());
    for (int i = 0; i < iterations; i++) {
      ListenableFuture<Boolean> testFuture = testFutures.get(i);
      Assertions.assertTrue(FutureUtils.getUnchecked(testFuture, false), "Test iteration #" + i);
    }

    // Drop all segments from the SegmentManager
    for (DataSegment segment : segments) {
      segmentManagerPreLoad.dropSegment(segment);
    }

    // Cache dir should exist, but be (mostly) empty, since we've dropped all segments.
    Assertions.assertTrue(preLoadCacheDir.exists());
    Assertions.assertEquals(Set.of("info_dir", "__drop"), Set.of(preLoadCacheDir.list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(preLoadCacheDir, "__drop").list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(preLoadCacheDir, "info_dir").list()));
  }

  /**
   * Tests acquireIfCached() with locally-cached DataSegment.
   */
  @Test
  public void test_acquireIfCached_preLoaded() throws SegmentLoadingException, IOException
  {
    final DataSegment segment = segments.get(0);

    // Load segment into SegmentManager's timeline
    segmentManagerPreLoad.loadSegment(segment);

    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerPreLoad,
        segment.getId(),
        segment.toDescriptor(),
        null,
        null,
        false
    );

    // acquireIfCached should return a segment since it's loaded
    final Optional<Segment> cachedSegment = loadableSegment.acquireIfCached(AcquireMode.PARTIAL);
    Assertions.assertTrue(cachedSegment.isPresent());

    try (final Segment acquiredSegment = cachedSegment.get()) {
      Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
      final RowCountInspector gadget = acquiredSegment.as(RowCountInspector.class);
      Assertions.assertNotNull(gadget);
      Assertions.assertEquals(1209, gadget.getNumRows());
    }

    segmentManagerPreLoad.dropSegment(segment);
  }

  /**
   * Tests acquireIfCached() when DataSegment is not in local timeline.
   */
  @Test
  public void test_acquireIfCached_dynamic()
  {
    final DataSegment segment = segments.get(0);
    final TestCoordinatorClientImpl coordinatorClient = new TestCoordinatorClientImpl();

    // Don't load segment into SegmentManager's timeline
    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerDynamic,
        segment.getId(),
        segment.toDescriptor(),
        null,
        coordinatorClient,
        false
    );

    // acquireIfCached should return empty since it's not loaded locally
    final Optional<Segment> cachedSegment = loadableSegment.acquireIfCached(AcquireMode.PARTIAL);
    Assertions.assertFalse(cachedSegment.isPresent());
  }

  /**
   * Tests fetching a single segment with CoordinatorClient.
   */
  @Test
  public void test_fetchSegment_dynamic() throws IOException, InterruptedException
  {
    final DataSegment segment = segments.get(0);
    final TestCoordinatorClientImpl coordinatorClient = new TestCoordinatorClientImpl();

    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerDynamic,
        segment.getId(),
        segment.toDescriptor(),
        null,
        coordinatorClient,
        false
    );

    // Verify that dataSegmentFuture() returns the correct DataSegment
    final DataSegment fetchedDataSegment = FutureUtils.getUnchecked(loadableSegment.dataSegmentFuture(), false);
    Assertions.assertEquals(segment, fetchedDataSegment);

    // Verify segment acquisition works.
    final AcquireSegmentAction acquireAction = loadableSegment.acquire(AcquireMode.PARTIAL);
    acquireAction.await();
    final AcquireSegmentResult acquireResult = acquireAction.release();
    final Optional<Segment> acquiredSegmentOptional = acquireResult.getSegment();
    Assertions.assertTrue(acquiredSegmentOptional.isPresent());

    try (final AcquireSegmentAction ignored = acquireAction;
         final Segment acquiredSegment = acquiredSegmentOptional.get()) {
      Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
      final RowCountInspector gadget = acquiredSegment.as(RowCountInspector.class);
      Assertions.assertNotNull(gadget);
      Assertions.assertEquals(1209, gadget.getNumRows());
    }
  }

  /**
   * Tests fetching a single segment with locally-cached DataSegment.
   */
  @Test
  public void test_fetchSegment_preLoaded() throws IOException, SegmentLoadingException, InterruptedException
  {
    final DataSegment segment = segments.get(0);

    // Load segment into SegmentManager's timeline
    segmentManagerPreLoad.loadSegment(segment);

    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerPreLoad,
        segment.getId(),
        segment.toDescriptor(),
        null,
        null,
        false
    );

    // Verify that dataSegmentFuture() returns the correct DataSegment
    final DataSegment fetchedDataSegment = FutureUtils.getUnchecked(loadableSegment.dataSegmentFuture(), false);
    Assertions.assertEquals(segment, fetchedDataSegment);

    // Verify segment acquisition works.
    final AcquireSegmentAction acquireAction = loadableSegment.acquire(AcquireMode.PARTIAL);
    acquireAction.await();
    final AcquireSegmentResult acquireResult = acquireAction.release();
    final Optional<Segment> acquiredSegmentOptional = acquireResult.getSegment();
    Assertions.assertTrue(acquiredSegmentOptional.isPresent());

    try (final AcquireSegmentAction ignored = acquireAction;
         final Segment acquiredSegment = acquiredSegmentOptional.get()) {
      Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
      final RowCountInspector gadget = acquiredSegment.as(RowCountInspector.class);
      Assertions.assertNotNull(gadget);
      Assertions.assertEquals(1209, gadget.getNumRows());
    }

    segmentManagerPreLoad.dropSegment(segment);
  }

  @JsonTypeName("test")
  private static class TestLoadSpec implements LoadSpec
  {
    private final int uniqueId;

    @JsonCreator
    public TestLoadSpec(@JsonProperty("uniqueId") int uniqueId)
    {
      this.uniqueId = uniqueId;
    }

    @JsonProperty
    public int getUniqueId()
    {
      return uniqueId;
    }

    @Override
    public LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException
    {
      try {
        CompressionUtils.unzip(SEGMENT_ZIP_FILE, destDir);
        return new LoadSpecResult(1);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to load segment in location [%s]", destDir);
      }
    }
  }

  /**
   * Deferred chain, stage-1 cancel: closing the outer handle before the Coordinator fetch resolves must cancel the
   * fetch and acquire nothing.
   */
  @Test
  public void test_deferredAcquire_closeBeforeCoordinatorFetch()
  {
    final DataSegment segment = segments.get(0);
    final SettableFuture<DataSegment> gate = SettableFuture.create();
    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerDynamic,
        segment.getId(),
        segment.toDescriptor(),
        null,
        new GatedCoordinatorClient(gate),
        false
    );

    final AcquireSegmentAction action = loadableSegment.acquire(AcquireMode.PARTIAL);
    Assertions.assertFalse(action.isReady());
    action.close();

    Assertions.assertTrue(gate.isCancelled(), "closing the outer handle must cancel the Coordinator fetch");
    assertCacheDirClean();
  }

  /**
   * Deferred chain, closed-before-stage-2 guard: when the Coordinator fetch completes after the outer handle was
   * closed (simulated with a cancel-resistant future), the inner acquire must never start.
   */
  @Test
  public void test_deferredAcquire_fetchCompletesAfterClose()
  {
    final DataSegment segment = segments.get(0);
    final UncancellableFuture<DataSegment> gate = new UncancellableFuture<>();
    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerDynamic,
        segment.getId(),
        segment.toDescriptor(),
        null,
        new GatedCoordinatorClient(gate),
        false
    );

    final AcquireSegmentAction action = loadableSegment.acquire(AcquireMode.PARTIAL);
    action.close();

    // the fetch survives the cancel attempt and completes; the stage guard must prevent the inner acquire
    Assertions.assertTrue(gate.setValue(segment));
    assertCacheDirClean();
  }

  /**
   * Deferred chain, stage-2 synchronous throw: if segmentManager.acquireSegment() throws synchronously after the
   * Coordinator fetch resolves, the failure must surface from the outer handle rather than being swallowed by the
   * direct-executor callback (which would hang the consumer forever).
   */
  @Test
  public void test_deferredAcquire_innerAcquireThrowIsSurfacedNotSwallowed()
  {
    final DataSegment segment = segments.get(0);
    final DruidException boom = DruidException.forPersona(DruidException.Persona.USER)
                                             .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                                             .build("no room for segment[%s]", segment.getId());
    final SegmentManager throwingManager = new SegmentManager(null)
    {
      @Override
      public Optional<VersionedIntervalTimeline<String, DataSegment>> getTimeline(TableDataSource dataSource)
      {
        // force the deferred (cachedDataSegment == null) path
        return Optional.empty();
      }

      @Override
      public AcquireSegmentAction acquireSegment(DataSegment dataSegment, AcquireMode acquireMode)
      {
        throw boom;
      }
    };
    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        throwingManager,
        segment.getId(),
        segment.toDescriptor(),
        null,
        new TestCoordinatorClientImpl(),
        false
    );

    final AcquireSegmentAction action = loadableSegment.acquire(AcquireMode.PARTIAL);
    // bounded await: a hang (swallowed throw) would surface as TimeoutException and fail the test
    final DruidException thrown = Assertions.assertThrows(DruidException.class, () -> action.await(10_000L));
    Assertions.assertTrue(thrown.getMessage().contains("no room for segment["), thrown.getMessage());
    action.close();
  }

  /**
   * Deferred chain, close-after-ready without release: closing the outer handle discards the delivered result and
   * releases everything the acquire placed.
   */
  @Test
  public void test_deferredAcquire_closeAfterReadyUnreleased() throws Exception
  {
    final DataSegment segment = segments.get(0);
    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerDynamic,
        segment.getId(),
        segment.toDescriptor(),
        null,
        new TestCoordinatorClientImpl(),
        false
    );

    final AcquireSegmentAction action = loadableSegment.acquire(AcquireMode.PARTIAL);
    action.await();
    action.close();
    assertCacheDirClean();
  }

  /**
   * Counter accounting happens at delivery via {@link LoadableSegment#countDelivered}: addLoad (when bytes were
   * loaded) + one addFile with the segment's row count.
   */
  @Test
  public void test_countDelivered_accounting() throws Exception
  {
    final DataSegment segment = segments.get(0);
    final ChannelCounters counters = new ChannelCounters();
    final RegularLoadableSegment loadableSegment = new RegularLoadableSegment(
        segmentManagerDynamic,
        segment.getId(),
        segment.toDescriptor(),
        counters,
        new TestCoordinatorClientImpl(),
        false
    );

    final AcquireSegmentAction action = loadableSegment.acquire(AcquireMode.PARTIAL);
    action.await();
    final AcquireSegmentResult result = action.release();
    loadableSegment.countDelivered(result);

    final ChannelCounters.Snapshot snapshot = counters.snapshot();
    Assertions.assertNotNull(snapshot.getFiles());
    Assertions.assertEquals(1, snapshot.getFiles()[snapshot.getFiles().length - 1], "exactly one file per delivery");
    Assertions.assertNotNull(snapshot.getRows());
    Assertions.assertEquals(1209, snapshot.getRows()[snapshot.getRows().length - 1]);

    try (Segment acquiredSegment = result.getSegment().orElseThrow()) {
      Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
    }
    action.close();
  }

  /**
   * Ephemeral virtual storage deletes segment files once all holds are released, so a clean cache dir proves the
   * acquire leaked nothing.
   */
  private void assertCacheDirClean()
  {
    if (!cacheDir.exists()) {
      // nothing was ever downloaded, which is as clean as it gets
      return;
    }
    Assertions.assertEquals(Set.of("info_dir", "__drop"), Set.of(cacheDir.list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(cacheDir, "__drop").list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(cacheDir, "info_dir").list()));
  }

  private static class GatedCoordinatorClient extends NoopCoordinatorClient
  {
    private final ListenableFuture<DataSegment> future;

    GatedCoordinatorClient(ListenableFuture<DataSegment> future)
    {
      this.future = future;
    }

    @Override
    public ListenableFuture<DataSegment> fetchSegment(String dataSource, String segmentId, boolean includeUnused)
    {
      return future;
    }
  }

  private static class UncancellableFuture<T> extends AbstractFuture<T>
  {
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      return false;
    }

    boolean setValue(T value)
    {
      return set(value);
    }
  }

  private class TestCoordinatorClientImpl extends NoopCoordinatorClient
  {
    @Override
    public ListenableFuture<DataSegment> fetchSegment(String dataSource, String segmentId, boolean includeUnused)
    {
      for (final DataSegment segment : segments) {
        if (segment.getDataSource().equals(dataSource) && segment.getId().toString().equals(segmentId)) {
          return Futures.immediateFuture(segment);
        }
      }

      return Futures.immediateFailedFuture(new ISE("No such segment[%s] for dataSource[%s]", segmentId, dataSource));
    }
  }
}
