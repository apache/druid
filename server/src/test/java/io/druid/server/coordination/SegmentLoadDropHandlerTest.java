/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.IndexIO;
import io.druid.segment.loading.CacheTestSegmentLoader;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.server.SegmentManager;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class SegmentLoadDropHandlerTest
{
  public static final int COUNT = 50;

  private static final Logger log = new Logger(ZkCoordinatorTest.class);

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final DruidServerMetadata me = new DruidServerMetadata(
      "dummyServer",
      "dummyHost",
      null,
      0,
      ServerType.HISTORICAL,
      "normal",
      0
  );

  private SegmentLoadDropHandler segmentLoadDropHandler;
  private DataSegmentAnnouncer announcer;
  private File infoDir;
  private AtomicInteger announceCount;
  private ConcurrentSkipListSet<DataSegment> segmentsAnnouncedByMe;
  private CacheTestSegmentLoader segmentLoader;
  private SegmentManager segmentManager;
  private List<Runnable> scheduledRunnable;

  @Before
  public void setUp() throws Exception
  {
    try {
      infoDir = new File(File.createTempFile("blah", "blah2").getParent(), "ZkCoordinatorTest");
      infoDir.mkdirs();
      for (File file : infoDir.listFiles()) {
        file.delete();
      }
      log.info("Creating tmp test files in [%s]", infoDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    scheduledRunnable = Lists.newArrayList();

    segmentLoader = new CacheTestSegmentLoader();
    segmentManager = new SegmentManager(segmentLoader);
    final ZkPathsConfig zkPaths = new ZkPathsConfig()
    {
      @Override
      public String getBase()
      {
        return "/druid";
      }
    };

    segmentsAnnouncedByMe = new ConcurrentSkipListSet<>();
    announceCount = new AtomicInteger(0);

    announcer = new DataSegmentAnnouncer()
    {
      @Override
      public void announceSegment(DataSegment segment) throws IOException
      {
        segmentsAnnouncedByMe.add(segment);
        announceCount.incrementAndGet();
      }

      @Override
      public void unannounceSegment(DataSegment segment) throws IOException
      {
        segmentsAnnouncedByMe.remove(segment);
        announceCount.decrementAndGet();
      }

      @Override
      public void announceSegments(Iterable<DataSegment> segments) throws IOException
      {
        for (DataSegment segment : segments) {
          segmentsAnnouncedByMe.add(segment);
        }
        announceCount.addAndGet(Iterables.size(segments));
      }

      @Override
      public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
      {
        for (DataSegment segment : segments) {
          segmentsAnnouncedByMe.remove(segment);
        }
        announceCount.addAndGet(-Iterables.size(segments));
      }
    };

    segmentLoadDropHandler = new SegmentLoadDropHandler(
        jsonMapper,
        new SegmentLoaderConfig()
        {
          @Override
          public File getInfoDir()
          {
            return infoDir;
          }

          @Override
          public int getNumLoadingThreads()
          {
            return 5;
          }

          @Override
          public int getAnnounceIntervalMillis()
          {
            return 50;
          }

          @Override
          public int getDropSegmentDelayMillis()
          {
            return 0;
          }
        },
        announcer,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        segmentManager,
        new ScheduledExecutorFactory()
        {
          @Override
          public ScheduledExecutorService create(int corePoolSize, String nameFormat)
          {
            /*
               Override normal behavoir by adding the runnable to a list so that you can make sure
               all the shceduled runnables are executed by explicitly calling run() on each item in the list
             */
            return new ScheduledThreadPoolExecutor(
                corePoolSize, new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build()
            )
            {
              @Override
              public ScheduledFuture<?> schedule(
                  Runnable command, long delay, TimeUnit unit
              )
              {
                scheduledRunnable.add(command);
                return null;
              }
            };
          }
        }.create(5, "SegmentLoadDropHandlerTest-[%d]")
    );
  }

  /**
   * Steps:
   * 1. removeSegment() schedules a delete runnable that deletes segment files,
   * 2. addSegment() succesfully loads the segment and annouces it
   * 3. scheduled delete task executes and realizes it should not delete the segment files.
   */
  @Test
  public void testSegmentLoading1() throws Exception
  {
    segmentLoadDropHandler.start();

    final DataSegment segment = makeSegment("test", "1", Intervals.of("P1d/2011-04-01"));

    segmentLoadDropHandler.removeSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertFalse(segmentsAnnouncedByMe.contains(segment));

    segmentLoadDropHandler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    /*
       make sure the scheduled runnable that "deletes" segment files has been executed.
       Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
       ZkCoordinator, the scheduled runnable will not actually delete segment files.
     */
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    Assert.assertTrue(segmentsAnnouncedByMe.contains(segment));
    Assert.assertFalse("segment files shouldn't be deleted", segmentLoader.getSegmentsInTrash().contains(segment));

    segmentLoadDropHandler.stop();
  }

  /**
   * Steps:
   * 1. addSegment() succesfully loads the segment and annouces it
   * 2. removeSegment() unannounces the segment and schedules a delete runnable that deletes segment files
   * 3. addSegment() calls loadSegment() and annouces it again
   * 4. scheduled delete task executes and realizes it should not delete the segment files.
   */
  @Test
  public void testSegmentLoading2() throws Exception
  {
    segmentLoadDropHandler.start();

    final DataSegment segment = makeSegment("test", "1", Intervals.of("P1d/2011-04-01"));

    segmentLoadDropHandler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertTrue(segmentsAnnouncedByMe.contains(segment));

    segmentLoadDropHandler.removeSegment(segment, DataSegmentChangeCallback.NOOP);

    Assert.assertFalse(segmentsAnnouncedByMe.contains(segment));

    segmentLoadDropHandler.addSegment(segment, DataSegmentChangeCallback.NOOP);

    /*
       make sure the scheduled runnable that "deletes" segment files has been executed.
       Because another addSegment() call is executed, which removes the segment from segmentsToDelete field in
       ZkCoordinator, the scheduled runnable will not actually delete segment files.
     */
    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    Assert.assertTrue(segmentsAnnouncedByMe.contains(segment));
    Assert.assertFalse("segment files shouldn't be deleted", segmentLoader.getSegmentsInTrash().contains(segment));

    segmentLoadDropHandler.stop();
  }

  @Test
  public void testLoadCache() throws Exception
  {
    List<DataSegment> segments = Lists.newLinkedList();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-03")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-04")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-05")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T01")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T03")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T05")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("PT1h/2011-04-04T06")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }
    Collections.sort(segments);

    for (DataSegment segment : segments) {
      writeSegmentToCache(segment);
    }

    checkCache(segments);
    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());
    segmentLoadDropHandler.start();
    Assert.assertTrue(!segmentManager.getDataSourceCounts().isEmpty());
    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(11L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(13 * COUNT, announceCount.get());
    segmentLoadDropHandler.stop();

    for (DataSegment segment : segments) {
      deleteSegmentFromCache(segment);
    }

    Assert.assertEquals(0, infoDir.listFiles().length);
    Assert.assertTrue(infoDir.delete());
  }

  private DataSegment makeSegment(String dataSource, String version, Interval interval)
  {
    return new DataSegment(
        dataSource,
        interval,
        version,
        ImmutableMap.<String, Object>of("version", version, "interval", interval, "cacheDir", infoDir),
        Arrays.asList("dim1", "dim2", "dim3"),
        Arrays.asList("metric1", "metric2"),
        NoneShardSpec.instance(),
        IndexIO.CURRENT_VERSION_ID,
        123L
    );
  }

  private void writeSegmentToCache(final DataSegment segment) throws IOException
  {
    if (!infoDir.exists()) {
      infoDir.mkdir();
    }

    File segmentInfoCacheFile = new File(
        infoDir,
        segment.getIdentifier()
    );
    try {
      jsonMapper.writeValue(segmentInfoCacheFile, segment);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    Assert.assertTrue(segmentInfoCacheFile.exists());
  }

  private void deleteSegmentFromCache(final DataSegment segment) throws IOException
  {
    File segmentInfoCacheFile = new File(
        infoDir,
        segment.getIdentifier()
    );
    if (segmentInfoCacheFile.exists()) {
      segmentInfoCacheFile.delete();
    }

    Assert.assertTrue(!segmentInfoCacheFile.exists());
  }

  private void checkCache(List<DataSegment> segments) throws IOException
  {
    Assert.assertTrue(infoDir.exists());
    File[] files = infoDir.listFiles();

    List<File> sortedFiles = Lists.newArrayList(files);
    Collections.sort(sortedFiles);

    Assert.assertEquals(segments.size(), sortedFiles.size());
    for (int i = 0; i < sortedFiles.size(); i++) {
      DataSegment segment = jsonMapper.readValue(sortedFiles.get(i), DataSegment.class);
      Assert.assertEquals(segments.get(i), segment);
    }
  }

  @Test
  public void testStartStop() throws Exception
  {
    SegmentLoadDropHandler handler = new SegmentLoadDropHandler(
        jsonMapper,
        new SegmentLoaderConfig()
        {
          @Override
          public File getInfoDir()
          {
            return infoDir;
          }

          @Override
          public int getNumLoadingThreads()
          {
            return 5;
          }

          @Override
          public int getAnnounceIntervalMillis()
          {
            return 50;
          }
        },
        announcer, EasyMock.createNiceMock(DataSegmentServerAnnouncer.class), segmentManager
    );

    List<DataSegment> segments = Lists.newLinkedList();
    for (int i = 0; i < COUNT; ++i) {
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test" + i, "1", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test" + i, "2", Intervals.of("P1d/2011-04-02")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-01")));
      segments.add(makeSegment("test_two" + i, "1", Intervals.of("P1d/2011-04-02")));
    }
    Collections.sort(segments);

    for (DataSegment segment : segments) {
      writeSegmentToCache(segment);
    }

    checkCache(segments);
    Assert.assertTrue(segmentManager.getDataSourceCounts().isEmpty());

    handler.start();
    Assert.assertTrue(!segmentManager.getDataSourceCounts().isEmpty());
    for (int i = 0; i < COUNT; ++i) {
      Assert.assertEquals(3L, segmentManager.getDataSourceCounts().get("test" + i).longValue());
      Assert.assertEquals(2L, segmentManager.getDataSourceCounts().get("test_two" + i).longValue());
    }
    Assert.assertEquals(5 * COUNT, announceCount.get());
    handler.stop();

    for (DataSegment segment : segments) {
      deleteSegmentFromCache(segment);
    }

    Assert.assertEquals(0, infoDir.listFiles().length);
    Assert.assertTrue(infoDir.delete());
  }

  @Test(timeout = 1000L)
  public void testProcessBatch() throws Exception
  {
    segmentLoadDropHandler.start();

    DataSegment segment1 = makeSegment("batchtest1", "1", Intervals.of("P1d/2011-04-01"));
    DataSegment segment2 = makeSegment("batchtest2", "1", Intervals.of("P1d/2011-04-01"));

    List<DataSegmentChangeRequest> batch = ImmutableList.of(
        new SegmentChangeRequestLoad(segment1),
        new SegmentChangeRequestDrop(segment2)
    );

    ListenableFuture<List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus>> future = segmentLoadDropHandler
        .processBatch(batch);

    List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> result = future.get();
    Assert.assertEquals(SegmentLoadDropHandler.Status.PENDING, result.get(0).getStatus());
    Assert.assertEquals(SegmentLoadDropHandler.Status.SUCCESS, result.get(1).getStatus());

    for (Runnable runnable : scheduledRunnable) {
      runnable.run();
    }

    result = segmentLoadDropHandler.processBatch(batch).get();
    Assert.assertEquals(SegmentLoadDropHandler.Status.SUCCESS, result.get(0).getStatus());
    Assert.assertEquals(SegmentLoadDropHandler.Status.SUCCESS, result.get(1).getStatus());


    for (SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus e : segmentLoadDropHandler.processBatch(batch).get()) {
      Assert.assertEquals(SegmentLoadDropHandler.Status.SUCCESS, e.getStatus());
    }

    segmentLoadDropHandler.stop();
  }
}
