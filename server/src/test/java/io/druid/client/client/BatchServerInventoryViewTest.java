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

package io.druid.client.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import io.druid.client.BatchServerInventoryView;
import io.druid.client.DruidServer;
import io.druid.client.ServerView;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.announcement.Announcer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.Segment;
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
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.LogicalOperator;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    cf.create().creatingParentsIfNeeded().forPath(testBasePath);

    jsonMapper = new DefaultObjectMapper();

    announcer = new Announcer(
        cf,
        "batchServerInventoryViewTestAnnouncer-%s"
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
        jsonMapper,
        Predicates.<DataSegment>alwaysTrue()
    );

    batchServerInventoryView.start();

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
        jsonMapper,
        new Predicate<DataSegment>()
        {
          @Override
          public boolean apply(@Nullable DataSegment dataSegment)
          {
            return dataSegment.getInterval().getStart().isBefore(SEGMENT_INTERVAL_START.plusDays(INITIAL_SEGMENTS));
          }
        }
    );

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

  @Test
  public void testRunWithFilter() throws Exception
  {
    segmentAnnouncer.announceSegments(testSegments);

    waitForSync(filteredBatchServerInventoryView, testSegments);

    DruidServer server = Iterables.get(filteredBatchServerInventoryView.getInventory(), 0);
    Set<DataSegment> segments = Sets.newHashSet(server.getSegments().values());

    Assert.assertEquals(testSegments, segments);

    // segment outside the range of default filter
    DataSegment segment1 = makeSegment(101);
    segmentAnnouncer.announceSegment(segment1);
    testSegments.add(segment1);

    exception.expect(ISE.class);
    waitForSync(filteredBatchServerInventoryView, testSegments);
  }

  @Test
  public void testRunWithFilterCallback() throws Exception
  {
    final CountDownLatch removeCallbackLatch = new CountDownLatch(1);

    segmentAnnouncer.announceSegments(testSegments);

    waitForSync(filteredBatchServerInventoryView, testSegments);

    DruidServer server = Iterables.get(filteredBatchServerInventoryView.getInventory(), 0);
    Set<DataSegment> segments = Sets.newHashSet(server.getSegments().values());

    Assert.assertEquals(testSegments, segments);

    ServerView.SegmentCallback callback = EasyMock.createStrictMock(ServerView.SegmentCallback.class);
    Comparator<DataSegment> dataSegmentComparator = new Comparator<DataSegment>()
    {
      @Override
      public int compare(DataSegment o1, DataSegment o2)
      {
        return o1.getInterval().equals(o2.getInterval()) ? 0 : -1;
      }
    };

    EasyMock
        .expect(
            callback.segmentAdded(
                EasyMock.<DruidServerMetadata>anyObject(),
                EasyMock.cmp(makeSegment(INITIAL_SEGMENTS + 2), dataSegmentComparator, LogicalOperator.EQUAL)
            )
        )
        .andReturn(ServerView.CallbackAction.CONTINUE)
        .times(1);

    EasyMock
        .expect(
            callback.segmentRemoved(
                EasyMock.<DruidServerMetadata>anyObject(),
                EasyMock.cmp(makeSegment(INITIAL_SEGMENTS + 2), dataSegmentComparator, LogicalOperator.EQUAL)
            )
        )
        .andAnswer(
            new IAnswer<ServerView.CallbackAction>()
            {
              @Override
              public ServerView.CallbackAction answer() throws Throwable
              {
                removeCallbackLatch.countDown();
                return ServerView.CallbackAction.CONTINUE;
              }
            }
        )
        .times(1);


    EasyMock.replay(callback);

    filteredBatchServerInventoryView.registerSegmentCallback(
        MoreExecutors.sameThreadExecutor(),
        callback,
        new Predicate<DataSegment>()
        {
          @Override
          public boolean apply(@Nullable DataSegment dataSegment)
          {
            return dataSegment.getInterval().getStart().equals(SEGMENT_INTERVAL_START.plusDays(INITIAL_SEGMENTS + 2));
          }
        }
    );

    DataSegment segment2 = makeSegment(INITIAL_SEGMENTS + 2);
    segmentAnnouncer.announceSegment(segment2);
    testSegments.add(segment2);

    DataSegment oldSegment = makeSegment(-1);
    segmentAnnouncer.announceSegment(oldSegment);
    testSegments.add(oldSegment);

    segmentAnnouncer.unannounceSegment(oldSegment);
    testSegments.remove(oldSegment);

    waitForSync(filteredBatchServerInventoryView, testSegments);

    segmentAnnouncer.unannounceSegment(segment2);
    testSegments.remove(segment2);

    waitForSync(filteredBatchServerInventoryView, testSegments);
    timing.forWaiting().awaitLatch(removeCallbackLatch);

    EasyMock.verify(callback);
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
      Thread.sleep(500);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > forWaitingTiming.milliseconds()) {
        throw new ISE("BatchServerInventoryView is not updating");
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
                      segments.add(makeSegment(INITIAL_SEGMENTS + ii  + numThreads * j));
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
