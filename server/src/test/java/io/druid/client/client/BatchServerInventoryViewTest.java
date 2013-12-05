/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
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
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 */
public class BatchServerInventoryViewTest
{
  private static final String testBasePath = "/test";
  private static final Joiner joiner = Joiner.on("/");

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private ObjectMapper jsonMapper;
  private Announcer announcer;
  private BatchDataSegmentAnnouncer segmentAnnouncer;
  private Set<DataSegment> testSegments;
  private BatchServerInventoryView batchServerInventoryView;

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
        MoreExecutors.sameThreadExecutor()
    );
    announcer.start();

    segmentAnnouncer = new BatchDataSegmentAnnouncer(
        new DruidServerMetadata(
            "id",
            "host",
            Long.MAX_VALUE,
            "type",
            "tier"
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
          public String getZkBasePath()
          {
            return testBasePath;
          }
        },
        announcer,
        jsonMapper
    );
    segmentAnnouncer.start();

    testSegments = Sets.newHashSet();
    for (int i = 0; i < 100; i++) {
      testSegments.add(makeSegment(i));
    }

    batchServerInventoryView = new BatchServerInventoryView(
        new ZkPathsConfig()
        {
          @Override
          public String getZkBasePath()
          {
            return testBasePath;
          }
        },
        cf,
        jsonMapper
    );

    batchServerInventoryView.start();
  }

  @After
  public void tearDown() throws Exception
  {
    batchServerInventoryView.stop();
    segmentAnnouncer.stop();
    announcer.stop();
    cf.close();
    testingCluster.stop();
  }

  @Test
  public void testRun() throws Exception
  {
    segmentAnnouncer.announceSegments(testSegments);

    waitForSync();

    DruidServer server = Iterables.get(batchServerInventoryView.getInventory(), 0);
    Set<DataSegment> segments = Sets.newHashSet(server.getSegments().values());

    Assert.assertEquals(testSegments, segments);

    DataSegment segment1 = makeSegment(101);
    DataSegment segment2 = makeSegment(102);

    segmentAnnouncer.announceSegment(segment1);
    segmentAnnouncer.announceSegment(segment2);
    testSegments.add(segment1);
    testSegments.add(segment2);

    waitForSync();

    Assert.assertEquals(testSegments, Sets.newHashSet(server.getSegments().values()));

    segmentAnnouncer.unannounceSegment(segment1);
    segmentAnnouncer.unannounceSegment(segment2);
    testSegments.remove(segment1);
    testSegments.remove(segment2);

    waitForSync();

    Assert.assertEquals(testSegments, Sets.newHashSet(server.getSegments().values()));
  }

  private DataSegment makeSegment(int offset)
  {
    return DataSegment.builder()
                      .dataSource("foo")
                      .interval(
                          new Interval(
                              new DateTime("2013-01-01").plusDays(offset),
                              new DateTime("2013-01-02").plusDays(offset)
                          )
                      )
                      .version(new DateTime().toString())
                      .build();
  }

  private void waitForSync() throws Exception
  {
    Stopwatch stopwatch = new Stopwatch().start();
    while (!Iterables.isEmpty(batchServerInventoryView.getInventory())
           && Iterables.get(batchServerInventoryView.getInventory(), 0).getSegments().size() != testSegments.size()) {
      Thread.sleep(500);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 5000) {
        throw new ISE("BatchServerInventoryView is not updating");
      }
    }
  }
}
