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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.jackson.SegmentizerModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.input.LoadableSegment;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.loading.AcquireSegmentAction;
import org.apache.druid.segment.loading.AcquireSegmentResult;
import org.apache.druid.segment.loading.LeastBytesUsedStorageLocationSelectorStrategy;
import org.apache.druid.segment.loading.LoadSpec;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

class DataSegmentProviderImplTest extends InitializedNullHandlingTest
{
  private static final String DATASOURCE = "foo";
  private static final int NUM_SEGMENTS = 10;
  private static final int THREADS = 8;
  private static File SEGMENT_ZIP_FILE;
  @TempDir
  public Path tempDir;
  private List<DataSegment> segments;
  private File cacheDir;
  private DataSegmentProviderImpl provider;
  private ListeningExecutorService exec;

  @BeforeAll
  public static void setupStatic(@TempDir Path tempDir) throws IOException
  {
    File segDir = tempDir.resolve("segment").toFile();
    File segmentFile = TestIndex.persist(TestIndex.getIncrementalTestIndex(), IndexSpec.getDefault(), segDir);
    File zipPath = tempDir.resolve("zip").toFile();
    FileUtils.mkdirp(zipPath);
    SEGMENT_ZIP_FILE = new File(zipPath, "index.zip");
    CompressionUtils.zip(segmentFile, SEGMENT_ZIP_FILE);
  }

  @BeforeEach
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    final ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;
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
      // Two segments per interval; helps verify that direction creation + deletion does not include races.
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

    cacheDir = tempDir.resolve("cache").toFile();
    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
        .setLocations(ImmutableList.of(new StorageLocationConfig(cacheDir, 10_000_000_000L, null)))
        .setVirtualStorage(true, true);
    final List<StorageLocation> locations = loaderConfig.toStorageLocations();
    final SegmentManager segmentManager = new SegmentManager(
        new SegmentLocalCacheManager(
            locations,
            loaderConfig,
            new LeastBytesUsedStorageLocationSelectorStrategy(locations),
            TestIndex.INDEX_IO,
            jsonMapper
        )
    );

    provider = new DataSegmentProviderImpl(
        segmentManager,
        new TestCoordinatorClientImpl()
    );

    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(THREADS, getClass().getSimpleName() + "-%s"));
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    if (exec != null) {
      exec.shutdownNow();
      exec.awaitTermination(1, TimeUnit.MINUTES);
    }
  }

  @Test
  public void testConcurrency()
  {
    final int iterations = 1000;
    final List<ListenableFuture<Boolean>> testFutures = new ArrayList<>();

    for (int i = 0; i < iterations; i++) {
      final int expectedSegmentNumber = i % NUM_SEGMENTS;
      final DataSegment segment = segments.get(expectedSegmentNumber);
      final ListenableFuture<LoadableSegment> f =
          exec.submit(() -> provider.fetchSegment(segment.getId(), segment.toDescriptor(), null, false));

      testFutures.add(
          FutureUtils.transform(
              FutureUtils.transformAsync(
                  f,
                  loadableSegment -> {
                    final AcquireSegmentAction acquireAction = loadableSegment.acquire();
                    return FutureUtils.transform(acquireAction.getSegmentFuture(), f2 -> Pair.of(acquireAction, f2));
                  }
              ),
              pair -> {
                final AcquireSegmentResult acquireResult = pair.rhs;
                final Optional<Segment> acquiredSegmentOptional =
                    acquireResult.getReferenceProvider().acquireReference();
                Assertions.assertTrue(acquiredSegmentOptional.isPresent());

                try (final AcquireSegmentAction ignored = pair.lhs; // Close action after this try block
                     final Segment acquiredSegment = acquiredSegmentOptional.get()) {
                  Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
                  PhysicalSegmentInspector gadget = acquiredSegment.as(PhysicalSegmentInspector.class);
                  Assertions.assertNotNull(gadget);
                  Assertions.assertEquals(1209, gadget.getNumRows());
                  acquiredSegment.close();
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
    Assertions.assertEquals(List.of("info_dir", "__drop"), Arrays.asList(cacheDir.list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(cacheDir, "__drop").list()));
    Assertions.assertEquals(Collections.emptyList(), Arrays.asList(new File(cacheDir, "info_dir").list()));
  }

  @Test
  public void testFetchSegment() throws IOException
  {
    final DataSegment segment = segments.get(0);
    final LoadableSegment loadableSegment =
        provider.fetchSegment(segment.getId(), segment.toDescriptor(), null, false);

    // Verify that the DataSegment is set properly.
    Assertions.assertEquals(segment, loadableSegment.dataSegment());

    // Verify segment acquisition works.
    final AcquireSegmentAction acquireAction = loadableSegment.acquire();
    final AcquireSegmentResult acquireResult = FutureUtils.getUnchecked(acquireAction.getSegmentFuture(), false);
    final Optional<Segment> acquiredSegmentOptional = acquireResult.getReferenceProvider().acquireReference();
    Assertions.assertTrue(acquiredSegmentOptional.isPresent());

    try (final AcquireSegmentAction ignored = acquireAction;
         final Segment acquiredSegment = acquiredSegmentOptional.get()) {
      Assertions.assertEquals(segment.getId(), acquiredSegment.getId());
      final PhysicalSegmentInspector gadget = acquiredSegment.as(PhysicalSegmentInspector.class);
      Assertions.assertNotNull(gadget);
      Assertions.assertEquals(1209, gadget.getNumRows());
    }
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
