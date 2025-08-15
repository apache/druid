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
import org.apache.druid.java.util.common.FileUtils;
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
import java.util.List;
import java.util.Optional;
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
  private static final int APPROX_SEGMENT_SIZE = 182100;
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
    // Each segment has the size of 182100 bytes. This segment cache is capable of storing up to 8 segments, 4 per storage location
    final StorageLocationConfig locationConfig = new StorageLocationConfig(
        localSegmentCacheFolder,
        APPROX_SEGMENT_SIZE * 4,
        null
    );
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(
        otherLocalSegmentCacheFolder,
        APPROX_SEGMENT_SIZE * 4,
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
    makeSegmentsToLoad(9, localStorageFolder, interval, segmentsToLoad);

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
      testWeakLoad(iterations, segmentCount, concurrentReads, sleepy, false);
    }
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegment() throws IOException
  {
    final int segmentCount = 24;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    for (boolean sleepy : new boolean[]{true, false}) {
      testWeakLoad(iterations, segmentCount, concurrentReads, sleepy, true);
    }
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegmentHighContention() throws IOException
  {
    final int segmentCount = 10;
    final int iterations = 2000;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    for (boolean sleepy : new boolean[]{true, false}) {
      testWeakLoad(iterations, segmentCount, concurrentReads, sleepy, true);
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
      // process batches of 10 requests at a time
      if ((i > 0 && i % concurrentReads == 0) || (i == iterations - 1)) {
        final List<WeakLoad> weakLoads = currentBatch
            .stream()
            .map(segment -> new WeakLoad(virtualStorageFabricManager, segment, 0, 0, 1L, true))
            .collect(Collectors.toList());
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
        for (Throwable t : exceptions) {
          Assertions.assertTrue(t instanceof TimeoutException || t instanceof ExecutionException, t.toString());
        }
        currentBatch.clear();
        exceptions.clear();
      }
      currentBatch.add(segmentsToWeakLoad.get(ThreadLocalRandom.current().nextInt(segmentCount)));
    }

    Thread.sleep(1000);
    Assertions.assertEquals(0, location.getActiveWeakHolds());
    Assertions.assertEquals(0, location2.getActiveWeakHolds());
    Assertions.assertTrue(4 >= location.getPath().listFiles().length);
    Assertions.assertTrue(4 >= location2.getPath().listFiles().length);
    Assertions.assertEquals(location.getWeakEvictions(), location.getWeakUnmounts());
    Assertions.assertEquals(location2.getWeakEvictions(), location2.getWeakUnmounts());
    Assertions.assertEquals(location.getWeakLoads() - 4, location.getWeakUnmounts());
    Assertions.assertEquals(location.getWeakLoads() - 4, location.getWeakEvictions());
    Assertions.assertEquals(location2.getWeakLoads() - 4, location2.getWeakUnmounts());
    Assertions.assertEquals(location2.getWeakLoads() - 4, location2.getWeakEvictions());
  }

  @Test
  public void testAcquireSegmentOnDemandRandomSegmentWithTimeoutAfterAcquire() throws IOException, InterruptedException
  {
    final int segmentCount = 24;
    final int iterations = 2001;
    final int concurrentReads = 10;
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");

    makeSegmentsToLoad(segmentCount, localStorageFolder, interval, segmentsToWeakLoad);

    final List<DataSegment> currentBatch = new ArrayList<>();
    for (int i = 0; i < iterations; i++) {
      // process batches of 10 requests at a time
      if (i > 0 && i % concurrentReads == 0) {
        final List<WeakLoad> weakLoads = currentBatch
            .stream()
            .map(segment -> new WeakLoad(virtualStorageFabricManager, segment, 20, 120, 1000L, false))
            .collect(Collectors.toList());
        final List<Future<Integer>> futures = new ArrayList<>();
        for (WeakLoad weakLoad : weakLoads) {
          futures.add(executorService.submit(weakLoad));
        }
        List<Throwable> exceptions = new ArrayList<>();
        int success = 0;
        int rows = 0;
        for (Future<Integer> future : futures) {
          try {
            Integer s = future.get(20L, TimeUnit.MILLISECONDS);
            if (s != null) {
              success++;
              rows += s;
            }
          }
          catch (Throwable t) {
            exceptions.add(t);
          }
        }
        for (Throwable t : exceptions) {
          Assertions.assertTrue(t instanceof TimeoutException || t instanceof ExecutionException, t.toString());
        }
        currentBatch.clear();
        exceptions.clear();
      }
      currentBatch.add(segmentsToWeakLoad.get(ThreadLocalRandom.current().nextInt(segmentCount)));
    }

    Thread.sleep(250);

    Assertions.assertTrue(4 >= location.getPath().listFiles().length);
    Assertions.assertTrue(4 >= location2.getPath().listFiles().length);
    Assertions.assertEquals(0, location.getActiveWeakHolds());
    Assertions.assertEquals(0, location2.getActiveWeakHolds());
    Assertions.assertEquals(location.getWeakLoads() - 4, location.getWeakUnmounts());
    Assertions.assertEquals(location2.getWeakLoads() - 4, location2.getWeakUnmounts());
    Assertions.assertTrue(location.getWeakHits() >= 0);
    Assertions.assertTrue(location2.getWeakHits() >= 0);
  }

  private void testWeakLoad(
      int iterations,
      int segmentCount,
      int concurrentReads,
      boolean sleepy,
      boolean expectHits
  )
  {
    int totalSuccess = 0;
    int totalFailures = 0;
    final List<DataSegment> currentBatch = new ArrayList<>();
    for (int i = 0; i < iterations; i++) {
      currentBatch.add(segmentsToWeakLoad.get(i % segmentCount));
      // process batches of 10 requests at a time
      if (currentBatch.size() == concurrentReads) {
        final List<WeakLoad> weakLoads = currentBatch
            .stream()
            .map(segment -> new WeakLoad(virtualStorageFabricManager, segment, 0, sleepy ? 20 : 0, 1000L, false))
            .collect(Collectors.toList());
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
        // cache has enough space to store 8 segments, so we should always be able to handle 8, but also sometimes more
        Assertions.assertTrue(success >= 8);
        Assertions.assertTrue(exceptions.size() <= 2);
        Assertions.assertEquals(success * 1209, rows);
        for (Throwable t : exceptions) {
          Assertions.assertInstanceOf(SegmentLoadingException.class, t.getCause());
          Assertions.assertTrue(t.getCause().getMessage().contains("Unable to load segment["));
          Assertions.assertTrue(t.getCause()
                                 .getMessage()
                                 .contains(
                                     "on demand, ensure enough disk space has been allocated to load all segments involved in the query"));
        }
        totalSuccess += success;
        totalFailures += exceptions.size();
        Assertions.assertEquals(
            totalSuccess,
            location.getWeakLoads() + location.getWeakHits() + location2.getWeakLoads() + location2.getWeakHits()
        );
        currentBatch.clear();
        exceptions.clear();
      }
    }

    try {
      Thread.sleep(500);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    Assertions.assertEquals(0, location.getActiveWeakHolds());
    Assertions.assertEquals(0, location2.getActiveWeakHolds());
    Assertions.assertEquals(4, location.getWeakEntryCount());
    Assertions.assertEquals(4, location2.getWeakEntryCount());
    Assertions.assertEquals(4, location.getPath().listFiles().length);
    Assertions.assertEquals(4, location2.getPath().listFiles().length);
    Assertions.assertEquals(iterations, totalSuccess + totalFailures);
    Assertions.assertEquals(
        totalSuccess,
        location.getWeakLoads() + location.getWeakHits() + location2.getWeakLoads() + location2.getWeakHits()
    );
    Assertions.assertTrue(totalFailures <= location.getWeakRejections() + location2.getWeakRejections());
    Assertions.assertEquals(location.getWeakLoads() - 4, location.getWeakUnmounts());
    Assertions.assertEquals(location2.getWeakLoads() - 4, location2.getWeakUnmounts());
    if (expectHits) {
      Assertions.assertTrue(location.getWeakHits() >= 0);
      Assertions.assertTrue(location2.getWeakHits() >= 0);
    } else {
      Assertions.assertEquals(0, location.getWeakHits());
      Assertions.assertEquals(0, location2.getWeakHits());
    }
  }

  private void makeSegmentsToLoad(
      int segmentCount,
      File localStorageFolder,
      Interval interval,
      List<DataSegment> segmentsToLoad
  )
      throws IOException
  {
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
          newSegment(interval, partitionId, FileUtils.getFileSize(localSegmentFile)).withLoadSpec(
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
      boolean timedOut = false;
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
      catch (ExecutionException | InterruptedException | TimeoutException e) {
        if (e instanceof TimeoutException) {
          timedOut = true;
          Futures.addCallback(
              action.getSegmentFuture(),
              AcquireSegmentAction.releaseCallback(action),
              Execs.directExecutor()
          );
        }
        throw new RuntimeException(e);
      }
      finally {
        CloseableUtils.closeAndWrapExceptions(closer);
        Assertions.assertEquals(expectTimeout, timedOut);
      }
    }
  }
}
