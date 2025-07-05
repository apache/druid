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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.SegmentDescriptor;
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
import java.util.stream.Collectors;

class SegmentLocalCacheManagerConcurrencyTest
{
  private final ObjectMapper jsonMapper;
  private final String dataSource = "test_ds";
  private final String segmentVersion;

  @TempDir
  File tempDir;

  private File localSegmentCacheFolder;
  private SegmentLocalCacheManager manager;
  private SegmentLocalCacheManager virtualStorageFabricManager;
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
    segmentsToLoad = new ArrayList<>();
    segmentsToWeakLoad = new ArrayList<>();

    final List<StorageLocationConfig> locations = new ArrayList<>();
    // Each segment has the size of 1000 bytes. This deep storage is capable of storing up to 8 segments.
    final StorageLocationConfig locationConfig = new StorageLocationConfig(localSegmentCacheFolder, 8000L, null);
    locations.add(locationConfig);

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
    };
    final List<StorageLocation> storageLocations = loaderConfig.toStorageLocations();
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
        new LeastBytesUsedStorageLocationSelectorStrategy(storageLocations),
        TestIndex.INDEX_IO,
        jsonMapper
    );
    executorService = Execs.multiThreaded(10, "segment-loader-local-cache-manager-concurrency-test-%d");
  }

  @AfterEach
  public void tearDown()
  {
    for (DataSegment segment : segmentsToLoad) {
      manager.drop(segment);
    }
    for (DataSegment segment : segmentsToWeakLoad) {
      virtualStorageFabricManager.drop(segment);
    }
    executorService.shutdownNow();
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
    makeSegmentsToLoad(9, localStorageFolder, interval, segmentsToLoad);

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
  public void testAcquireSegmentOnDemand() throws IOException
  {
    final File localStorageFolder = new File(tempDir, "local_storage_folder");

    final Interval interval = Intervals.of("2019-01-01/P1D");
    makeSegmentsToLoad(100, localStorageFolder, interval, segmentsToWeakLoad);


    List<DataSegment> currentBatch = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      // process batches of 10 requests at a time
      if (i > 0 && i % 10 == 0) {
        final List<Future<Integer>> futures = currentBatch
            .stream()
            .map(segment -> executorService.submit(new WeakLoad(virtualStorageFabricManager, segment)))
            .collect(Collectors.toList());
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
          catch (InterruptedException | ExecutionException e) {
            exceptions.add(e);
          }
        }
        // cache has enough space to store 8 segments, so we should always be able to handle 8, but also sometimes more
        Assertions.assertTrue(success >= 8);
        Assertions.assertTrue(exceptions.size() <= 2);
        Assertions.assertTrue(rows >= 9672);
        for (Throwable t : exceptions) {
          Assertions.assertInstanceOf(SegmentLoadingException.class, t.getCause());
          Assertions.assertTrue(t.getCause().getMessage().contains("Unable to load segment["));
          Assertions.assertTrue(t.getCause().getMessage().contains("on demand, ensure enough disk space has been allocated to load all segments involved in the query"));
        }
        currentBatch.clear();
        exceptions.clear();
      } else {
        currentBatch.add(segmentsToWeakLoad.get(i));
      }
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
          segmentPath
      );
      final File indexZip = new File(new File(localStorageFolder, segmentPath), "index.zip");
      SegmentLocalCacheManagerTest.makeSegmentZip(localSegmentFile, indexZip);

      final DataSegment segment = newSegment(interval, partitionId).withLoadSpec(
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

  private DataSegment newSegment(Interval interval, int partitionId)
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
                      .size(1000L)
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
    private final SegmentLocalCacheManager segmentManager;
    private final DataSegment segment;

    private WeakLoad(SegmentLocalCacheManager segmentManager, DataSegment segment)
    {
      this.segmentManager = segmentManager;
      this.segment = segment;
    }

    @Override
    public Integer call() throws SegmentLoadingException
    {
      final AcquireSegmentAction action = segmentManager.acquireSegment(
          segment,
          new SegmentDescriptor(segment.getId().getInterval(), segment.getId().getVersion(), segment.getId().getPartitionNum())
      );
      try {
        final Optional<Segment> segment = action.getSegmentFuture().get();
        if (segment.isPresent()) {
          try {
            PhysicalSegmentInspector gadget = segment.get().as(PhysicalSegmentInspector.class);
            // sleep to make tests well behaved and simulate doing query stuff or whatever
            Thread.sleep(1000);
            return gadget.getNumRows();
          }
          finally {
            CloseableUtils.closeAndWrapExceptions(segment.get());
          }
        }
        return null;
      }
      catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
      finally {
        CloseableUtils.closeAndWrapExceptions(action);
      }
    }
  }
}
