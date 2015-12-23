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

package io.druid.client.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import io.druid.client.BatchServerInventoryView;
import io.druid.client.DruidServer;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.announcement.Announcer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.BatchDataSegmentAnnouncer;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.test.Timing;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class BatchServerInventoryViewTest
{
  private static final String testBasePath = "/test";
  public static final DateTime SEGMENT_INTERVAL_START = new DateTime("2013-01-01");
  public static final int INITIAL_SEGMENTS = 100;
  private static final Timing timing = new Timing();

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private ObjectMapper jsonMapper;
  private Announcer announcer;
  private BatchDataSegmentAnnouncer segmentAnnouncer;
  private Set<DataSegment> testSegments;
  private BatchServerInventoryView batchServerInventoryView;
  private BatchServerInventoryView filteredBatchServerInventoryView;
  private final AtomicInteger inventoryUpdateCounter = new AtomicInteger();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(true))
                                .build();
    cf.start();
    cf.blockUntilConnected();
    cf.create().creatingParentsIfNeeded().forPath(testBasePath);

    jsonMapper = new DefaultObjectMapper();

    announcer = new Announcer(
        cf,
        MoreExecutors.sameThreadExecutor()
    );
    announcer.start();

    segmentAnnouncer = new BatchDataSegmentAnnouncer(
        new DruidServerMetadata(
            "id",
            "host",
            Long.MAX_VALUE,
            "type",
            "tier",
            0
        ),
        new BatchDataSegmentAnnouncerConfig()
        {
          @Override
          public int getSegmentsPerNode()
          {
            return 50;
          }
        },
        new ZkPathsConfig()
        {
          @Override
          public String getBase()
          {
            return testBasePath;
          }
        },
        announcer,
        jsonMapper
    );
    segmentAnnouncer.start();

    testSegments = Sets.newConcurrentHashSet();
    for (int i = 0; i < INITIAL_SEGMENTS; i++) {
      testSegments.add(makeSegment(i));
    }

    batchServerInventoryView = new BatchServerInventoryView(
        new ZkPathsConfig()
        {
          @Override
          public String getBase()
          {
            return testBasePath;
          }
        },
        cf,
        jsonMapper
    );

    batchServerInventoryView.start();
    inventoryUpdateCounter.set(0);
    filteredBatchServerInventoryView = new BatchServerInventoryView(
        new ZkPathsConfig()
        {
          @Override
          public String getBase()
          {
            return testBasePath;
          }
        },
        cf,
        jsonMapper
    )
    {
      @Override
      protected DruidServer addInnerInventory(
          DruidServer container, String inventoryKey, Set<DataSegment> inventory
      )
      {
        DruidServer server = super.addInnerInventory(container, inventoryKey, inventory);
        inventoryUpdateCounter.incrementAndGet();
        return server;
      }
    };
    filteredBatchServerInventoryView.start();
  }

  @After
  public void tearDown() throws Exception
  {
    batchServerInventoryView.stop();
    filteredBatchServerInventoryView.stop();
    segmentAnnouncer.stop();
    announcer.stop();
    cf.close();
    testingCluster.stop();
  }

  @Test
  public void testRun() throws Exception
  {
    segmentAnnouncer.announceSegments(testSegments);

    waitForSync(batchServerInventoryView, testSegments);

    DruidServer server = Iterables.get(batchServerInventoryView.getInventory(), 0);
    Set<DataSegment> segments = Sets.newHashSet(server.getSegments().values());

    Assert.assertEquals(testSegments, segments);

    DataSegment segment1 = makeSegment(101);
    DataSegment segment2 = makeSegment(102);

    segmentAnnouncer.announceSegment(segment1);
    segmentAnnouncer.announceSegment(segment2);
    testSegments.add(segment1);
    testSegments.add(segment2);

    waitForSync(batchServerInventoryView, testSegments);

    Assert.assertEquals(testSegments, Sets.newHashSet(server.getSegments().values()));

    segmentAnnouncer.unannounceSegment(segment1);
    segmentAnnouncer.unannounceSegment(segment2);
    testSegments.remove(segment1);
    testSegments.remove(segment2);

    waitForSync(batchServerInventoryView, testSegments);

    Assert.assertEquals(testSegments, Sets.newHashSet(server.getSegments().values()));
  }

  private DataSegment makeSegment(int offset)
  {
    return DataSegment.builder()
                      .dataSource("foo")
                      .interval(
                          new Interval(
                              SEGMENT_INTERVAL_START.plusDays(offset),
                              SEGMENT_INTERVAL_START.plusDays(offset + 1)
                          )
                      )
                      .version(new DateTime().toString())
                      .build();
  }

  private static void waitForSync(BatchServerInventoryView batchServerInventoryView, Set<DataSegment> testSegments)
      throws Exception
  {
    final Timing forWaitingTiming = timing.forWaiting();
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (Iterables.isEmpty(batchServerInventoryView.getInventory())
           || Iterables.get(batchServerInventoryView.getInventory(), 0).getSegments().size() != testSegments.size()) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > forWaitingTiming.milliseconds()) {
        throw new ISE("BatchServerInventoryView is not updating");
      }
    }
  }

  private void waitForUpdateEvents(int count)
      throws Exception
  {
    final Timing forWaitingTiming = timing.forWaiting();
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (inventoryUpdateCounter.get() != count) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > forWaitingTiming.milliseconds()) {
        throw new ISE(
            "BatchServerInventoryView is not updating counter expected[%d] value[%d]",
            count,
            inventoryUpdateCounter.get()
        );
      }
    }
  }

  @Test
  public void testSameTimeZnode() throws Exception
  {
    final int numThreads = INITIAL_SEGMENTS / 10;
    final ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads));

    segmentAnnouncer.announceSegments(testSegments);

    waitForSync(batchServerInventoryView, testSegments);

    DruidServer server = Iterables.get(batchServerInventoryView.getInventory(), 0);
    final Set<DataSegment> segments = Sets.newHashSet(server.getSegments().values());

    Assert.assertEquals(testSegments, segments);

    final CountDownLatch latch = new CountDownLatch(numThreads);

    final List<ListenableFuture<BatchDataSegmentAnnouncer>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; ++i) {
      final int ii = i;
      futures.add(
          executor.submit(
              new Callable<BatchDataSegmentAnnouncer>()
              {
                @Override
                public BatchDataSegmentAnnouncer call()
                {
                  BatchDataSegmentAnnouncer segmentAnnouncer = new BatchDataSegmentAnnouncer(
                      new DruidServerMetadata(
                          "id",
                          "host",
                          Long.MAX_VALUE,
                          "type",
                          "tier",
                          0
                      ),
                      new BatchDataSegmentAnnouncerConfig()
                      {
                        @Override
                        public int getSegmentsPerNode()
                        {
                          return 50;
                        }
                      },
                      new ZkPathsConfig()
                      {
                        @Override
                        public String getBase()
                        {
                          return testBasePath;
                        }
                      },
                      announcer,
                      jsonMapper
                  );
                  segmentAnnouncer.start();
                  List<DataSegment> segments = new ArrayList<DataSegment>();
                  try {
                    for (int j = 0; j < INITIAL_SEGMENTS / numThreads; ++j) {
                      segments.add(makeSegment(INITIAL_SEGMENTS + ii + numThreads * j));
                    }
                    latch.countDown();
                    latch.await();
                    segmentAnnouncer.announceSegments(segments);
                    testSegments.addAll(segments);
                  }
                  catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                  return segmentAnnouncer;
                }
              }
          )
      );
    }
    final List<BatchDataSegmentAnnouncer> announcers = Futures.<BatchDataSegmentAnnouncer>allAsList(futures).get();
    Assert.assertEquals(INITIAL_SEGMENTS * 2, testSegments.size());
    waitForSync(batchServerInventoryView, testSegments);

    Assert.assertEquals(testSegments, Sets.newHashSet(server.getSegments().values()));

    for (int i = 0; i < INITIAL_SEGMENTS; ++i) {
      final DataSegment segment = makeSegment(100 + i);
      segmentAnnouncer.unannounceSegment(segment);
      testSegments.remove(segment);
    }

    waitForSync(batchServerInventoryView, testSegments);

    Assert.assertEquals(testSegments, Sets.newHashSet(server.getSegments().values()));
  }
}
