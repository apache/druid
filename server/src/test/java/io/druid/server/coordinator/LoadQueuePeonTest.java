/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.guava.Comparators;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorTestBase;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestDrop;
import io.druid.server.coordination.SegmentChangeRequestLoad;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class LoadQueuePeonTest extends CuratorTestBase
{
  private LoadQueuePeon peon;
  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private PathChildrenCache loadQueueCache;

  @Before
  public void setup() throws Exception
  {
    setupServerAndCurator();

    curator.start();
    curator.create().forPath("/test");
    loadQueueCache = new PathChildrenCache(
        curator,
        "/test",
        true,
        true,
        Execs.singleThreaded("cache")
    );
    peon = new LoadQueuePeon(
        curator,
        "/test/",
        jsonMapper,
        Execs.scheduledSingleThreaded("test-loadQueuePeon-scheduled-%s"),
        Execs.singleThreaded(
            "test-loadQueuePeon-%s"
        ),
        new DruidCoordinatorConfig()
        {
          @Override
          public Duration getCoordinatorStartDelay()
          {
            return null;
          }

          @Override
          public Duration getCoordinatorPeriod()
          {
            return null;
          }

          @Override
          public Duration getCoordinatorIndexingPeriod()
          {
            return null;
          }

          @Override
          public int getLoadQueueSize()
          {
            return 2;
          }
        }
    );
    loadQueueCache.start();
  }

  @After
  public void tearDown() throws IOException
  {
    loadQueueCache.close();
    peon.stop();
    tearDownServerAndCurator();
  }

  @Test
  public void testLoadSegmentSanity() throws Exception
  {
    final DataSegment foo = makeSegment("foo", 20, new Interval("2012-01-01/P1D"));
    final CountDownLatch announcementLatch = new CountDownLatch(1);
    final CountDownLatch callbackLatch = new CountDownLatch(1);

    loadQueueCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
          {
            final ChildData child = event.getData();
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
              if (child.getPath().equals(peon.pathFor(foo))) {
                announcementLatch.countDown();
              }
            }
          }
        }
    );

    peon.loadSegment(
        foo, new LoadPeonCallback()
        {
          @Override
          public void execute()
          {
            callbackLatch.countDown();
          }
        }
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(announcementLatch));
    Assert.assertEquals(
        new SegmentChangeRequestLoad(foo),
        jsonMapper.readValue(
            curator.getData().decompressed().forPath(peon.pathFor(foo)),
            DataSegmentChangeRequest.class
        )
    );
    Assert.assertEquals(20L, peon.getLoadQueueSize());
    markRequestComplete(foo);
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackLatch));
    Assert.assertEquals(0L, peon.getLoadQueueSize());
    loadQueueCache.close();
  }


  @Test
  public void testDropSegmentSanity() throws Exception
  {
    final DataSegment foo = makeSegment("foo", 20, new Interval("2012-01-01/P1D"));
    final CountDownLatch announcementLatch = new CountDownLatch(1);
    final CountDownLatch callbackLatch = new CountDownLatch(1);


    loadQueueCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
          {
            final ChildData child = event.getData();
            if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
              if (child.getPath().equals(peon.pathFor(foo))) {
                announcementLatch.countDown();
              }
            }
          }
        }
    );

    peon.dropSegment(
        foo, new LoadPeonCallback()
        {
          @Override
          public void execute()
          {
            callbackLatch.countDown();
          }
        }
    );

    Assert.assertTrue(timing.forWaiting().awaitLatch(announcementLatch));
    Assert.assertEquals(
        new SegmentChangeRequestDrop(foo),
        jsonMapper.readValue(
            curator.getData().decompressed().forPath(peon.pathFor(foo)),
            DataSegmentChangeRequest.class
        )
    );
    Assert.assertEquals(0, peon.getLoadQueueSize());
    markRequestComplete(foo);
    Assert.assertTrue(timing.forWaiting().awaitLatch(callbackLatch));

    loadQueueCache.close();
  }

  @Test
  public void testParallelLoadingAndDrop() throws Exception
  {
    final DataSegment[] foo = new DataSegment[]{
        makeSegment("foo1", 40, new Interval("2012-04-01/P1D")),
        makeSegment("foo2", 50, new Interval("2012-03-01/P1D")),
        makeSegment("foo3", 60, new Interval("2012-02-01/P1D")),
        makeSegment("foo4", 70, new Interval("2012-01-01/P1D"))
    };

    Comparator<DataSegment> segmentComparator = Comparators.inverse(DataSegment.bucketMonthComparator());
    ConcurrentSkipListMap<DataSegment, Object> test = new ConcurrentSkipListMap<>(
        segmentComparator
    );
    for (int i = 0; i < foo.length; i++) {
      test.put(foo[i], new Object());
    }
    System.out.println(test.keySet());

    final CountDownLatch[] loadRequest = new CountDownLatch[foo.length];
    final CountDownLatch[] dropRequest = new CountDownLatch[foo.length];
    for (int i = 0; i < foo.length; i++) {
      loadRequest[i] = new CountDownLatch(1);
      dropRequest[i] = new CountDownLatch(1);
    }
    final AtomicInteger loadRequestCount = new AtomicInteger();
    for (int i = 0; i < foo.length; i++) {
      final int index = i;
      peon.loadSegment(
          foo[i], new LoadPeonCallback()
          {
            @Override
            public void execute()
            {
            }
          }
      );
      loadQueueCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
              ChildData data = event.getData();
              if (event.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED && data.getPath()
                                                                                    .equals(peon.pathFor(foo[index]))) {
                DataSegmentChangeRequest request = jsonMapper.readValue(
                    data.getData(),
                    DataSegmentChangeRequest.class
                );
                if (request instanceof SegmentChangeRequestLoad) {
                  loadRequestCount.incrementAndGet();
                  loadRequest[index].countDown();
                } else if (request instanceof SegmentChangeRequestDrop) {
                  dropRequest[index].countDown();
                }
              }
            }
          }
      );
    }
    // only two parallel loaded requests since size of queue is 2
    Assert.assertTrue(timing.forWaiting().awaitLatch(loadRequest[0]));
    Assert.assertTrue(timing.forWaiting().awaitLatch(loadRequest[1]));
    Assert.assertEquals(2, loadRequestCount.get());
    markRequestComplete(foo[1]);
    // 3rd segment load request should get queued
    Assert.assertTrue(timing.forWaiting().awaitLatch(loadRequest[2]));
    Assert.assertEquals(3, loadRequestCount.get());
    // drop should get priority over already queued load request
    peon.dropSegment(
        foo[1], new LoadPeonCallback()
        {
          @Override
          public void execute()
          {
          }
        }
    );
    markRequestComplete(foo[0]);
    Assert.assertTrue(timing.forWaiting().awaitLatch(dropRequest[1]));
    Assert.assertEquals(3, loadRequestCount.get());

  }


  public DataSegment makeSegment(String dataSource, long size, Interval interval)
  {
    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(interval)
                      .version("2")
                      .size(size)
                      .build();
  }

  private void markRequestComplete(DataSegment foo) throws Exception
  {
    curator.delete().forPath(peon.pathFor(foo));
  }
}
