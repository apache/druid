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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.curator.CuratorTestBase;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.DataSegmentChangeCallback;
import org.apache.druid.server.coordination.DataSegmentChangeHandler;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentChangeRequestDrop;
import org.apache.druid.server.coordination.SegmentChangeRequestLoad;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class LoadQueuePeonTest extends CuratorTestBase
{
  private static final String LOAD_QUEUE_PATH = "/druid/loadqueue/localhost:1234";

  private final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

  private LoadQueuePeon loadQueuePeon;
  private PathChildrenCache loadQueueCache;

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    curator.create().creatingParentsIfNeeded().forPath(LOAD_QUEUE_PATH);

    loadQueueCache = new PathChildrenCache(
        curator,
        LOAD_QUEUE_PATH,
        true,
        true,
        Execs.singleThreaded("load_queue_cache-%d")
    );
  }

  @Test
  public void testMultipleLoadDropSegments() throws Exception
  {
    loadQueuePeon = new CuratorLoadQueuePeon(
        curator,
        LOAD_QUEUE_PATH,
        jsonMapper,
        Execs.scheduledSingleThreaded("test_load_queue_peon_scheduled-%d"),
        Execs.singleThreaded("test_load_queue_peon-%d"),
        new TestDruidCoordinatorConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            10,
            null,
            Duration.millis(0)
        )
    );

    loadQueuePeon.start();

    ConcurrentMap<SegmentId, CountDownLatch> loadRequestSignals = new ConcurrentHashMap<>(5);
    ConcurrentMap<SegmentId, CountDownLatch> dropRequestSignals = new ConcurrentHashMap<>(5);
    ConcurrentMap<SegmentId, CountDownLatch> segmentLoadedSignals = new ConcurrentHashMap<>(5);
    ConcurrentMap<SegmentId, CountDownLatch> segmentDroppedSignals = new ConcurrentHashMap<>(5);

    final List<DataSegment> segmentToDrop = Lists.transform(
        ImmutableList.of(
            "2014-10-26T00:00:00Z/P1D",
            "2014-10-25T00:00:00Z/P1D",
            "2014-10-24T00:00:00Z/P1D",
            "2014-10-23T00:00:00Z/P1D",
            "2014-10-22T00:00:00Z/P1D"
        ), new Function<String, DataSegment>()
        {
          @Override
          public DataSegment apply(String intervalStr)
          {
            DataSegment dataSegment = dataSegmentWithInterval(intervalStr);
            return dataSegment;
          }
        }
    );

    final CountDownLatch[] dropRequestLatches = new CountDownLatch[5];
    final CountDownLatch[] dropSegmentLatches = new CountDownLatch[5];
    for (int i = 0; i < 5; i++) {
      dropRequestLatches[i] = new CountDownLatch(1);
      dropSegmentLatches[i] = new CountDownLatch(1);
    }
    int i = 0;
    for (DataSegment s : segmentToDrop) {
      dropRequestSignals.put(s.getId(), dropRequestLatches[i]);
      segmentDroppedSignals.put(s.getId(), dropSegmentLatches[i++]);
    }

    final List<DataSegment> segmentToLoad = Lists.transform(
        ImmutableList.of(
            "2014-10-27T00:00:00Z/P1D",
            "2014-10-29T00:00:00Z/P1M",
            "2014-10-31T00:00:00Z/P1D",
            "2014-10-30T00:00:00Z/P1D",
            "2014-10-28T00:00:00Z/P1D"
        ), new Function<String, DataSegment>()
        {
          @Override
          public DataSegment apply(String intervalStr)
          {
            DataSegment dataSegment = dataSegmentWithInterval(intervalStr);
            loadRequestSignals.put(dataSegment.getId(), new CountDownLatch(1));
            segmentLoadedSignals.put(dataSegment.getId(), new CountDownLatch(1));
            return dataSegment;
          }
        }
    );

    final CountDownLatch[] loadRequestLatches = new CountDownLatch[5];
    final CountDownLatch[] segmentLoadedLatches = new CountDownLatch[5];
    for (i = 0; i < 5; i++) {
      loadRequestLatches[i] = new CountDownLatch(1);
      segmentLoadedLatches[i] = new CountDownLatch(1);
    }
    i = 0;
    for (DataSegment s : segmentToDrop) {
      loadRequestSignals.put(s.getId(), loadRequestLatches[i]);
      segmentLoadedSignals.put(s.getId(), segmentLoadedLatches[i++]);
    }

    // segment with latest interval should be loaded first
    final List<DataSegment> expectedLoadOrder = Lists.transform(
        ImmutableList.of(
            "2014-10-29T00:00:00Z/P1M",
            "2014-10-31T00:00:00Z/P1D",
            "2014-10-30T00:00:00Z/P1D",
            "2014-10-28T00:00:00Z/P1D",
            "2014-10-27T00:00:00Z/P1D"
        ), intervalStr -> dataSegmentWithInterval(intervalStr)
    );

    final DataSegmentChangeHandler handler = new DataSegmentChangeHandler()
    {
      @Override
      public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
      {
        loadRequestSignals.get(segment.getId()).countDown();
      }

      @Override
      public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
      {
        dropRequestSignals.get(segment.getId()).countDown();
      }
    };

    loadQueueCache.getListenable().addListener(
        (client, event) -> {
          if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
            DataSegmentChangeRequest request = jsonMapper.readValue(
                event.getData().getData(),
                DataSegmentChangeRequest.class
            );
            request.go(handler, null);
          }
        }
    );
    loadQueueCache.start();

    for (final DataSegment segment : segmentToDrop) {
      loadQueuePeon.dropSegment(
          segment,
          () -> segmentDroppedSignals.get(segment.getId()).countDown()
      );
    }

    for (final DataSegment segment : segmentToLoad) {
      loadQueuePeon.loadSegment(
          segment,
          () -> segmentLoadedSignals.get(segment.getId()).countDown()
      );
    }

    Assert.assertEquals(6000, loadQueuePeon.getLoadQueueSize());
    Assert.assertEquals(5, loadQueuePeon.getSegmentsToLoad().size());
    Assert.assertEquals(5, loadQueuePeon.getSegmentsToDrop().size());

    for (DataSegment segment : segmentToDrop) {
      String dropRequestPath = ZKPaths.makePath(LOAD_QUEUE_PATH, segment.getId().toString());
      Assert.assertTrue(
          "Latch not counted down for " + dropRequestSignals.get(segment.getId()),
          dropRequestSignals.get(segment.getId()).await(10, TimeUnit.SECONDS)
      );
      Assert.assertNotNull(
          "Path " + dropRequestPath + " doesn't exist",
          curator.checkExists().forPath(dropRequestPath)
      );
      Assert.assertEquals(
          segment,
          ((SegmentChangeRequestDrop) jsonMapper.readValue(
              curator.getData()
                     .decompressed()
                     .forPath(dropRequestPath), DataSegmentChangeRequest.class
          )).getSegment()
      );

      // simulate completion of drop request by historical
      curator.delete().guaranteed().forPath(dropRequestPath);
      Assert.assertTrue(timing.forWaiting().awaitLatch(segmentDroppedSignals.get(segment.getId())));
    }

    for (DataSegment segment : expectedLoadOrder) {
      String loadRequestPath = ZKPaths.makePath(LOAD_QUEUE_PATH, segment.getId().toString());
      Assert.assertTrue(timing.forWaiting().awaitLatch(loadRequestSignals.get(segment.getId())));
      Assert.assertNotNull(curator.checkExists().forPath(loadRequestPath));
      Assert.assertEquals(
          segment,
          ((SegmentChangeRequestLoad) jsonMapper
              .readValue(curator.getData().decompressed().forPath(loadRequestPath), DataSegmentChangeRequest.class))
              .getSegment()
      );

      // simulate completion of load request by historical
      curator.delete().guaranteed().forPath(loadRequestPath);
      Assert.assertTrue(timing.forWaiting().awaitLatch(segmentLoadedSignals.get(segment.getId())));
    }
  }

  @Test
  public void testFailAssign() throws Exception
  {
    final DataSegment segment = dataSegmentWithInterval("2014-10-22T00:00:00Z/P1D");

    final CountDownLatch loadRequestSignal = new CountDownLatch(1);
    final CountDownLatch segmentLoadedSignal = new CountDownLatch(1);

    loadQueuePeon = new CuratorLoadQueuePeon(
        curator,
        LOAD_QUEUE_PATH,
        jsonMapper,
        Execs.scheduledSingleThreaded("test_load_queue_peon_scheduled-%d"),
        Execs.singleThreaded("test_load_queue_peon-%d"),
        // set time-out to 1 ms so that LoadQueuePeon will fail the assignment quickly
        new TestDruidCoordinatorConfig(
            null,
            null,
            null,
            new Duration(1),
            null,
            null,
            10,
            null,
            new Duration("PT1s")
        )
    );

    loadQueuePeon.start();

    loadQueueCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
          {
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
              loadRequestSignal.countDown();
            }
          }
        }
    );
    loadQueueCache.start();

    loadQueuePeon.loadSegment(
        segment,
        new LoadPeonCallback()
        {
          @Override
          public void execute()
          {
            segmentLoadedSignal.countDown();
          }
        }
    );

    String loadRequestPath = ZKPaths.makePath(LOAD_QUEUE_PATH, segment.getId().toString());
    Assert.assertTrue(timing.forWaiting().awaitLatch(loadRequestSignal));
    Assert.assertNotNull(curator.checkExists().forPath(loadRequestPath));
    Assert.assertEquals(
        segment,
        ((SegmentChangeRequestLoad) jsonMapper
            .readValue(curator.getData().decompressed().forPath(loadRequestPath), DataSegmentChangeRequest.class))
            .getSegment()
    );

    // don't simulate completion of load request here
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentLoadedSignal));
    Assert.assertEquals(0, loadQueuePeon.getSegmentsToLoad().size());
    Assert.assertEquals(0L, loadQueuePeon.getLoadQueueSize());
  }

  private DataSegment dataSegmentWithInterval(String intervalStr)
  {
    return DataSegment.builder()
                      .dataSource("test_load_queue_peon")
                      .interval(Intervals.of(intervalStr))
                      .loadSpec(ImmutableMap.of())
                      .version("2015-05-27T03:38:35.683Z")
                      .dimensions(ImmutableList.of())
                      .metrics(ImmutableList.of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(1200)
                      .build();
  }

  @After
  public void tearDown() throws Exception
  {
    loadQueueCache.close();
    loadQueuePeon.stop();
    tearDownServerAndCurator();
  }
}
