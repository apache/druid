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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.PhysicalSegmentInspector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

class SegmentLocalCacheManagerConcurrencyTest
{
  private final ObjectMapper jsonMapper;
  private final String dataSource = "test_ds";
  private final String segmentVersion;

  @TempDir
  File tempDir;

  private File localSegmentCacheFolder;
  private File otherLocalSegmentCacheFolder;
  private SegmentLocalCacheManager manager;
  private SegmentLocalCacheManager virtualStorageFabricManager;
  private StorageLocation location;
  private StorageLocation location2;
  private ExecutorService executorService;
  private List<DataSegment> segmentsToLoad;
  private List<DataSegment> segmentsToWeakLoad;

  public SegmentLocalCacheManagerConcurrencyTest()
  {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.registerSubtypes(new NamedType(LocalLoadSpec.class, "local"));
    jsonMapper.setInjectableValues(
        new InjectableValues.Std().addValue(
            LocalDataSegmentPuller.class,
            new LocalDataSegmentPuller()
        ).addValue(
            IndexIO.class,
            TestHelper.getTestIndexIO()
        )
    );
    segmentVersion = DateTimes.nowUtc().toString();
  }

  @BeforeEach
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());
    localSegmentCacheFolder = new File(tempDir, "segment_cache_folder");
    otherLocalSegmentCacheFolder = new File(tempDir, "other_segment_cache_folder");
    segmentsToLoad = new ArrayList<>();
    segmentsToWeakLoad = new ArrayList<>();

    final List<StorageLocationConfig> locations = new ArrayList<>();
    // Each segment has the synthetic size of 1000 bytes. This segment cache is capable of storing up to 8 segments,
    // 4 per storage location
    final StorageLocationConfig locationConfig = new StorageLocationConfig(
        localSegmentCacheFolder,
        4000,
        null
    );
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(
        otherLocalSegmentCacheFolder,
        4000,
        null
    );
    locations.add(locationConfig);
    locations.add(locationConfig2);

    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig().withLocations(locations);
    final SegmentLoaderConfig vsfLoaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return locations;
      }

      @Override
      public boolean isVirtualStorageFabric()
      {
        return true;
      }

      @Override
      public int getVirtualStorageFabricLoadThreads()
      {
        return Runtime.getRuntime().availableProcessors();
      }
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
    location = storageLocations.get(0);
    location2 = storageLocations.get(1);
    manager = new SegmentLocalCacheManager(
        storageLocations,
        loaderConfig,
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    virtualStorageFabricManager = new SegmentLocalCacheManager(
        storageLocations,
        vsfLoaderConfig,
        new RoundRobinStorageLocationSelectorStrategy(storageLocations),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    executorService = Execs.multiThreaded(
        10,
        "segment-loader-local-cache-manager-concurrency-test-%d"
    );
  }

  @AfterEach
  public void tearDown()
  {
    executorService.shutdownNow();
    for (DataSegment segment : segmentsToLoad) {
      manager.drop(segment);
    }
    for (DataSegment segment : segmentsToWeakLoad) {
      virtualStorageFabricManager.drop(segment);
    }
    for (StorageLocation location : virtualStorageFabricManager.getLocations()) {
      location.resetStats();
    }
  }

  @Test
  public void testAcquireSegment() throws IOException, ExecutionException, InterruptedException
  {
    final File localStorageFolder = new File(tempDir, "local_storage_folder");


    final Interval interval = Intervals.of("2019-01-01/P1D");
    makeSegmentsToLoad(8, localStorageFolder, interval, segmentsToLoad);

    final List<Future<?>> futures = segmentsToLoad
        .stream()
        .map(segment -> executorService.submit(new Load(manager, segment)))
        .collect(Collectors.toList());

    for (Future<?> future : futures) {
      future.get();
    }
    Assertions.assertTrue(true);
  }

  @Test
  public void testAcquireSegmentFailTooManySegments() throws IOException
  {
    final File localStorageFolder = new File("local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");
    makeSegmentsToLoad(20, localStorageFolder, interval, segmentsToLoad);

    final List<Future<?>> futures = segmentsToLoad
        .stream()
        .map(segment -> executorService.submit(new Load(manager, segment)))
        .collect(Collectors.toList());

    Throwable t = Assertions.assertThrows(
        ExecutionException.class,
        () -> {
          for (Future<?> future : futures) {
            future.get();
          }
        }
    );
    Assertions.assertInstanceOf(SegmentLoadingException.class, t.getCause());
    Assertions.assertTrue(t.getCause().getMessage().contains("Failed to load segment"));
    Assertions.assertTrue(t.getCause().getMessage().contains("in all locations."));
  }

  @Test
  public void testAcquireSegmentBulkFailTooManySegments() throws IOException
  {
    final File localStorageFolder = new File("local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");
    makeSegmentsToLoad(30, localStorageFolder, interval, segmentsToLoad);

    final List<WeakLoad> weakLoads = segmentsToLoad
        .stream()
        .map(segment -> new WeakLoad(virtualStorageFabricManager, segment, 10, 100, 1000L, false))
        .collect(Collectors.toList());
    final List<Future<Integer>> futures = new ArrayList<>();
    for (WeakLoad weakLoad : weakLoads) {
      futures.add(executorService.submit(weakLoad));
    }
    Throwable t = Assertions.assertThrows(
        ExecutionException.class,
        () -> {
          for (Future<?> future : futures) {
            future.get();
          }
        }
    );
    Assertions.assertInstanceOf(SegmentLoadingException.class, t.getCause());
    Assertions.assertTrue(t.getCause().getMessage().contains("Unable to load segment"));
    Assertions.assertTrue(t.getCause().getMessage().contains("on demand, ensure enough disk space has been allocated"));
  }

  @Test
  public void testAcquireSegmentOnDemand() throws IOException
  {
    final int segmentCount = 100;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");
    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    for (boolean sleepy : new boolean[]{true, false}) {
      testWeakLoad(iterations, segmentCount, concurrentReads, false, sleepy, false);
    }
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegment() throws IOException
  {
    // moderate number of segments compared to threads, expect to have a decent hit rate
    final int segmentCount = 24;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    for (boolean sleepy : new boolean[]{true, false}) {
      testWeakLoad(iterations, segmentCount, concurrentReads, true, sleepy, true);
    }
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegmentHighHitRate() throws IOException
  {
    // low number of total segments so expect many cache hits and few evictions
    final int segmentCount = 10;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    for (boolean sleepy : new boolean[]{true, false}) {
      testWeakLoad(iterations, segmentCount, concurrentReads, true, sleepy, true);
    }
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegmentNoEvictions() throws IOException
  {
    // low number of total segments so expect many cache hits and few evictions
    final int segmentCount = 8;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    for (boolean sleepy : new boolean[]{true, false}) {
      testWeakLoad(iterations, segmentCount, concurrentReads, true, sleepy, true);
    }
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegmentWithTimeoutBeforeAcquire() throws IOException, InterruptedException
  {
    final int segmentCount = 24;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    final List<DataSegment> currentBatch = new ArrayList<>();
    for (int i = 0; i < iterations; i++) {
      currentBatch.add(segmentsToWeakLoad.get(ThreadLocalRandom.current().nextInt(segmentCount)));
      // process batches of 10 requests at a time
      if (currentBatch.size() == concurrentReads) {
        final List<WeakLoad> weakLoads = currentBatch
            .stream()
            .map(segment -> new WeakLoad(virtualStorageFabricManager, segment, 0, 0, 1L, true))
            .collect(Collectors.toList());
        final List<Future<Integer>> futures = new ArrayList<>();
        for (WeakLoad weakLoad : weakLoads) {
          futures.add(executorService.submit(weakLoad));
        }
        List<Throwable> exceptions = new ArrayList<>();
        for (Future<Integer> future : futures) {
          try {
            future.get();
          }
          catch (Throwable t) {
            exceptions.add(t);
          }
        }
        Assertions.assertFalse(exceptions.isEmpty());
        for (Throwable t : exceptions) {
          Assertions.assertTrue(t instanceof TimeoutException || t instanceof ExecutionException, t.toString());
        }
        Thread.sleep(20);
        Assertions.assertEquals(0, location.getActiveWeakHolds());
        Assertions.assertEquals(0, location2.getActiveWeakHolds());

        currentBatch.clear();
      }
    }

    Assertions.assertEquals(0, location.getActiveWeakHolds());
    Assertions.assertEquals(0, location2.getActiveWeakHolds());
    Assertions.assertTrue(4 >= location.getWeakEntryCount());
    Assertions.assertTrue(4 >= location2.getWeakEntryCount());
    Assertions.assertTrue(4 >= location.getPath().listFiles().length);
    Assertions.assertTrue(4 >= location2.getPath().listFiles().length);
    Assertions.assertEquals(location.getStats().getEvictionCount(), location.getStats().getUnmountCount());
    Assertions.assertEquals(location2.getStats().getEvictionCount(), location2.getStats().getUnmountCount());
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegmentWithTimeoutAfterAcquire() throws IOException, InterruptedException
  {
    final int segmentCount = 24;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    final List<DataSegment> currentBatch = new ArrayList<>();
    for (int i = 0; i < iterations; i++) {
      currentBatch.add(segmentsToWeakLoad.get(ThreadLocalRandom.current().nextInt(segmentCount)));
      // process batches of 10 requests at a time
      if (currentBatch.size() == concurrentReads) {
        final List<WeakLoad> weakLoads = currentBatch
            .stream()
            .map(segment -> new WeakLoad(virtualStorageFabricManager, segment, 10, 100, Long.MAX_VALUE, false))
            .collect(Collectors.toList());
        final List<Future<Integer>> futures = new ArrayList<>();
        for (WeakLoad weakLoad : weakLoads) {
          futures.add(executorService.submit(weakLoad));
        }
        List<Throwable> exceptions = new ArrayList<>();
        for (Future<Integer> future : futures) {
          try {
            future.get(20L, TimeUnit.MILLISECONDS);
          }
          catch (Throwable t) {
            exceptions.add(t);
          }
        }
        Assertions.assertFalse(exceptions.isEmpty());
        for (Throwable t : exceptions) {
          Assertions.assertTrue(t instanceof TimeoutException || t instanceof ExecutionException, t.toString());
        }
        while (true) {
          boolean allDone = true;
          for (Future<?> f : futures) {
            allDone = allDone && f.isDone();
          }
          if (allDone) {
            break;
          }
          Thread.sleep(5);
        }
        Assertions.assertEquals(0, location.getActiveWeakHolds());
        Assertions.assertEquals(0, location2.getActiveWeakHolds());
        currentBatch.clear();
      }
    }

    Assertions.assertTrue(location.getStats().getHitCount() >= 0);
    Assertions.assertTrue(location2.getStats().getHitCount() >= 0);
    assertNoLooseEnds();
  }

  @Test
  public void testMixedWeakLoadAndAcquireCachedSegmentOfWeakLoads() throws IOException, InterruptedException
  {
    final int segmentCount = 10;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    final List<DataSegment> currentBatch = new ArrayList<>();
    final int minLoadCount = 5;
    int totalSuccess = 0;
    int totalEmpty = 0;
    int totalRows = 0;
    for (int i = 0; i < iterations; i++) {
      currentBatch.add(segmentsToWeakLoad.get(ThreadLocalRandom.current().nextInt(segmentCount)));
      // process batches of 10 requests at a time
      if (currentBatch.size() == concurrentReads) {
        int weakLoadCount = 0;
        List<Callable<Integer>> callables = new ArrayList<>();
        for (DataSegment segment : currentBatch) {
          // do a weak load for at most minLoadCount segments to populate the cache (and occasionally evict stuff as we
          // progress through iterations)
          if (weakLoadCount < minLoadCount && ThreadLocalRandom.current().nextBoolean()) {
            callables.add(new WeakLoad(virtualStorageFabricManager, segment, 0, 50, Long.MAX_VALUE, false));
            weakLoadCount++;
          } else {
            // do the rest as calls that only acquire reference to segments that are already loaded
            callables.add(new LoadCached(virtualStorageFabricManager, segment, 50, 50));
          }
        }
        final List<Future<Integer>> futures = new ArrayList<>();
        for (Callable<Integer> load : callables) {
          futures.add(executorService.submit(load));
        }
        int success = 0;
        int rows = 0;
        int empty = 0;
        for (Future<Integer> future : futures) {
          try {
            Integer s = future.get();
            success++;
            if (s != null) {
              rows += s;
            } else {
              empty++;
            }
          }
          catch (Throwable t) {
            Assertions.fail();
          }
        }

        Assertions.assertEquals(0, location.getActiveWeakHolds());
        Assertions.assertEquals(0, location2.getActiveWeakHolds());
        totalSuccess += success;
        totalEmpty += empty;
        totalRows += rows;
        currentBatch.clear();
      }
    }

    Assertions.assertEquals(iterations, totalSuccess);
    Assertions.assertEquals(totalRows, (totalSuccess * 1209) - (totalEmpty * 1209));
    // expect at least some of the load cached calls to succeed
    Assertions.assertTrue(totalSuccess > ((iterations / 10) * minLoadCount));
    // expect at least some empties from the segment not being cached
    Assertions.assertTrue(totalEmpty > 0);
    Assertions.assertTrue(location.getStats().getHitCount() >= 0);
    Assertions.assertTrue(location2.getStats().getHitCount() >= 0);

    assertNoLooseEnds();
  }

  private void testWeakLoad(
      int iterations,
      int segmentCount,
      int concurrentReads,
      boolean random,
      boolean sleepy,
      boolean expectHits
  )
  {
    int totalSuccess = 0;
    int totalFailures = 0;
    final List<DataSegment> currentBatch = new ArrayList<>();
    // start fresh
    for (DataSegment segment : segmentsToWeakLoad) {
      virtualStorageFabricManager.drop(segment);
    }
    location.resetStats();
    location2.resetStats();
    for (int i = 0; i < iterations; i++) {
      int segment = random ? ThreadLocalRandom.current().nextInt(segmentCount) : i % segmentCount;
      currentBatch.add(segmentsToWeakLoad.get(segment));
      // process batches of 10 requests at a time
      if (currentBatch.size() == concurrentReads) {

        BatchResult result = testWeakBatch(i, currentBatch, sleepy);
        totalSuccess += result.success;
        totalFailures += result.exceptions.size();
        Assertions.assertEquals(
            totalSuccess,
            location.getStats().getLoadCount() +
            location.getStats().getHitCount() +
            location2.getStats().getLoadCount() +
            location2.getStats().getHitCount(),
            StringUtils.format(
                "iteration[%s] - loc1: loads[%s] hits[%s] loc2: loads[%s] hits[%s]",
                i,
                location.getStats().getLoadCount(),
                location.getStats().getHitCount(),
                location2.getStats().getLoadCount(),
                location2.getStats().getHitCount()
            )
        );

        currentBatch.clear();
      }
    }

    if (!currentBatch.isEmpty()) {
      BatchResult result = testWeakBatch(iterations, currentBatch, sleepy);
      totalSuccess += result.success;
      totalFailures += result.exceptions.size();
    }

    Assertions.assertEquals(iterations, totalSuccess + totalFailures);
    Assertions.assertEquals(
        totalSuccess,
        location.getStats().getLoadCount()
        + location.getStats().getHitCount()
        + location2.getStats().getLoadCount()
        + location2.getStats().getHitCount()
    );
    Assertions.assertTrue(totalFailures <= location.getStats().getRejectCount() + location2.getStats()
                                                                                           .getRejectCount());

    if (expectHits) {
      Assertions.assertTrue(location.getStats().getHitCount() >= 0);
      Assertions.assertTrue(location2.getStats().getHitCount() >= 0);
    } else {
      Assertions.assertEquals(0, location.getStats().getHitCount());
      Assertions.assertEquals(0, location2.getStats().getHitCount());
    }

    assertNoLooseEnds();
  }

  private BatchResult testWeakBatch(int iteration, List<DataSegment> currentBatch, boolean sleepy)
  {
    final List<WeakLoad> weakLoads = new ArrayList<>();
    Set<SegmentId> segments = new HashSet<>();
    for (DataSegment segment : currentBatch) {
      weakLoads.add(new WeakLoad(virtualStorageFabricManager, segment, 0, sleepy ? 20 : 0, Long.MAX_VALUE, false));
      segments.add(segment.getId());
    }
    final List<Future<Integer>> futures = new ArrayList<>();
    for (WeakLoad weakLoad : weakLoads) {
      futures.add(executorService.submit(weakLoad));
    }
    List<Throwable> exceptions = new ArrayList<>();
    int success = 0;
    int rows = 0;
    for (Future<Integer> future : futures) {
      try {
        Integer s = future.get();
        if (s != null) {
          success++;
          rows += s;
        }
      }
      catch (Throwable t) {
        exceptions.add(t);
      }
    }

    // cache has enough space to store 8 segments, so we should always be able to handle 8, but also sometimes more,
    // such as if multiple threads have the same segment
    final int expectedSuccess = Math.min(8, segments.size());
    // expect at most 2 failures if there are more unique segments than cache slots
    final int maxExpectedFailures = segments.size() > 8 ? 2 : 0;
    Assertions.assertEquals(success * 1209, rows);
    Assertions.assertTrue(
        expectedSuccess <= success,
        "iteration " + iteration + " expected " + expectedSuccess + " tasks to succeed but got " + success
    );
    Assertions.assertTrue(
        maxExpectedFailures >= exceptions.size(),
        "iteration " + iteration + " expected " + maxExpectedFailures + " tasks to fail but got " + exceptions.size()
    );
    // we only expect SegmentLoadingException in happy path tests
    for (Throwable t : exceptions) {
      Assertions.assertInstanceOf(SegmentLoadingException.class, t.getCause());
      Assertions.assertTrue(t.getCause().getMessage().contains("Unable to load segment["));
      Assertions.assertTrue(t.getCause()
                             .getMessage()
                             .contains(
                                 "on demand, ensure enough disk space has been allocated to load all segments involved in the query"));
    }

    return new BatchResult(exceptions, success, rows);
  }



  private void assertNoLooseEnds()
  {
    Assertions.assertEquals(0, location.getActiveWeakHolds());
    Assertions.assertEquals(0, location2.getActiveWeakHolds());
    Assertions.assertTrue(4 >= location.getWeakEntryCount());
    Assertions.assertTrue(4 >= location2.getWeakEntryCount());
    Assertions.assertTrue(4 >= location.getPath().listFiles().length);
    Assertions.assertTrue(4 >= location2.getPath().listFiles().length);
    Assertions.assertTrue(location.getStats().getLoadCount() >= 4);
    Assertions.assertTrue(location2.getStats().getLoadCount() >= 4);
    Assertions.assertEquals(location.getStats().getEvictionCount(), location.getStats().getUnmountCount());
    Assertions.assertEquals(location2.getStats().getEvictionCount(), location2.getStats().getUnmountCount());
    Assertions.assertEquals(location.getStats().getLoadCount() - 4, location.getStats().getEvictionCount());
    Assertions.assertEquals(location2.getStats().getLoadCount() - 4, location2.getStats().getEvictionCount());
    Assertions.assertEquals(location.getStats().getLoadCount() - 4, location.getStats().getUnmountCount());
    Assertions.assertEquals(location2.getStats().getLoadCount() - 4, location2.getStats().getUnmountCount());
  }

  private void makeSegmentsToLoad(
      int segmentCount,
      File localStorageFolder,
      Interval interval,
      List<DataSegment> segmentsToLoad
  )
      throws IOException
  {
    segmentsToLoad.clear();
    for (int partitionId = 0; partitionId < segmentCount; partitionId++) {
      final String segmentPath = Paths.get(
          localStorageFolder.getCanonicalPath(),
          dataSource,
          StringUtils.format("%s_%s", interval.getStart().toString(), interval.getEnd().toString()),
          segmentVersion,
          String.valueOf(partitionId)
      ).toString();
      final File localSegmentFile = new File(
          localStorageFolder,
          segmentPath + "_build"
      );
      final File indexZip = new File(new File(localStorageFolder, segmentPath), "index.zip");
      SegmentLocalCacheManagerTest.makeSegmentZip(localSegmentFile, indexZip);

      final DataSegment segment =
          newSegment(interval, partitionId, 1000).withLoadSpec(
              ImmutableMap.of(
                  "type",
                  "local",
                  "path",
                  indexZip.getAbsolutePath()
              )
          );
      segmentsToLoad.add(segment);
    }
  }

  private DataSegment newSegment(Interval interval, int partitionId, long size)
  {
    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(interval)
                      .loadSpec(
                          ImmutableMap.of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version(segmentVersion)
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(new NumberedShardSpec(partitionId, 0))
                      .binaryVersion(9)
                      .size(size)
                      .build();
  }

  private static class BatchResult
  {
    public final List<Throwable> exceptions;
    public final int success;
    public final int rows;

    public BatchResult(List<Throwable> exceptions, int success, int rows)
    {
      this.exceptions = exceptions;
      this.success = success;
      this.rows = rows;
    }
  }

  private static class Load implements Callable<Void>
  {
    private final SegmentLocalCacheManager segmentManager;
    private final DataSegment segment;

    private Load(SegmentLocalCacheManager segmentManager, DataSegment segment)
    {
      this.segmentManager = segmentManager;
      this.segment = segment;
    }

    @Override
    public Void call() throws SegmentLoadingException
    {
      segmentManager.load(segment);
      return null;
    }
  }

  private static class WeakLoad implements Callable<Integer>
  {
    private final int delayMin;
    private final int delayMax;
    private final long timeout;
    private final SegmentLocalCacheManager segmentManager;
    private final DataSegment segment;
    private final boolean expectTimeout;

    private WeakLoad(
        SegmentLocalCacheManager segmentManager,
        DataSegment segment,
        int delayMin,
        int delayMax,
        long timeout,
        boolean expectTimeout
    )
    {
      this.segmentManager = segmentManager;
      this.segment = segment;
      this.delayMin = delayMin;
      this.delayMax = delayMax;
      this.timeout = timeout;
      this.expectTimeout = expectTimeout;
    }

    @Override
    public Integer call() throws SegmentLoadingException
    {
      final Closer closer = Closer.create();
      final AcquireSegmentAction action = closer.register(
          segmentManager.acquireSegment(segment, segment.toDescriptor())
      );
      try {
        final Optional<Segment> segment =
            action.getSegmentFuture().get(timeout, TimeUnit.MILLISECONDS).map(closer::register);
        if (segment.isPresent()) {
          PhysicalSegmentInspector gadget = segment.get().as(PhysicalSegmentInspector.class);
          if (delayMin >= 0 && delayMax > 0) {
            Thread.sleep(ThreadLocalRandom.current().nextInt(delayMin, delayMax));
          }
          return gadget.getNumRows();
        }
        return null;
      }
      catch (Throwable t) {
        Futures.addCallback(
            action.getSegmentFuture(),
            AcquireSegmentAction.releaseCallback(action),
            Execs.directExecutor()
        );
        throw new RuntimeException(t);
      }
      finally {
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    }
  }

  private static class LoadCached implements Callable<Integer>
  {
    private final int maxDelayBefore;
    private final int maxDelayAfter;
    private final SegmentLocalCacheManager segmentManager;
    private final DataSegment segment;

    private LoadCached(
        SegmentLocalCacheManager segmentManager,
        DataSegment segment,
        int maxDelayBefore,
        int maxDelayAfter
    )
    {
      this.segmentManager = segmentManager;
      this.segment = segment;
      this.maxDelayBefore = maxDelayBefore;
      this.maxDelayAfter = maxDelayAfter;
    }

    @Override
    public Integer call()
    {
      final Closer closer = Closer.create();
      try {
        if (maxDelayBefore > 0) {
          Thread.sleep(ThreadLocalRandom.current().nextInt(maxDelayBefore));
        }
        final Optional<Segment> segmentReference = segmentManager.acquireCachedSegment(segment).map(closer::register);
        if (segmentReference.isPresent()) {
          PhysicalSegmentInspector gadget = segmentReference.get().as(PhysicalSegmentInspector.class);
          if (maxDelayAfter > 0) {
            Thread.sleep(ThreadLocalRandom.current().nextInt(maxDelayAfter));
          }
          return gadget.getNumRows();
        }
        return null;
      }
      catch (Throwable t) {
        throw new RuntimeException(t);
      }
      finally {
        CloseableUtils.closeAndWrapExceptions(closer);
      }
    }
  }
}
