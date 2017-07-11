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

package io.druid.server.coordination.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.announcement.Announcer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.BatchDataSegmentAnnouncer;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.SegmentChangeRequestHistory;
import io.druid.server.coordination.SegmentChangeRequestsSnapshot;
import io.druid.server.coordination.ServerType;
import io.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class BatchDataSegmentAnnouncerTest
{
  private static final String testBasePath = "/test";
  private static final String testSegmentsPath = "/test/segments/id";
  private static final Joiner joiner = Joiner.on("/");

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private ObjectMapper jsonMapper;
  private Announcer announcer;
  private SegmentReader segmentReader;
  private BatchDataSegmentAnnouncer segmentAnnouncer;
  private Set<DataSegment> testSegments;

  private final AtomicInteger maxBytesPerNode = new AtomicInteger(512 * 1024);
  private Boolean skipDimensionsAndMetrics;
  private Boolean skipLoadSpec;


  @Before
  public void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
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

    segmentReader = new SegmentReader(cf, jsonMapper);
    skipDimensionsAndMetrics = false;
    skipLoadSpec = false;
    segmentAnnouncer = new BatchDataSegmentAnnouncer(
        new DruidServerMetadata(
            "id",
            "host",
            null,
            Long.MAX_VALUE,
            ServerType.HISTORICAL,
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

          @Override
          public long getMaxBytesPerNode()
          {
            return maxBytesPerNode.get();
          }

          @Override
          public boolean isSkipDimensionsAndMetrics()
          {
            return skipDimensionsAndMetrics;
          }

          @Override
          public boolean isSkipLoadSpec()
          {
            return skipLoadSpec;
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

    testSegments = Sets.newHashSet();
    for (int i = 0; i < 100; i++) {
      testSegments.add(makeSegment(i));
    }
  }

  @After
  public void tearDown() throws Exception
  {
    announcer.stop();
    cf.close();
    testingCluster.stop();
  }

  @Test
  public void testSingleAnnounce() throws Exception
  {
    Iterator<DataSegment> segIter = testSegments.iterator();
    DataSegment firstSegment = segIter.next();
    DataSegment secondSegment = segIter.next();

    segmentAnnouncer.announceSegment(firstSegment);

    List<String> zNodes = cf.getChildren().forPath(testSegmentsPath);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(joiner.join(testSegmentsPath, zNode));
      Assert.assertEquals(segments.iterator().next(), firstSegment);
    }

    segmentAnnouncer.announceSegment(secondSegment);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(joiner.join(testSegmentsPath, zNode));
      Assert.assertEquals(Sets.newHashSet(firstSegment, secondSegment), segments);
    }

    SegmentChangeRequestsSnapshot snapshot = segmentAnnouncer.getSegmentChangesSince(
        new SegmentChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(2, snapshot.getCounter().getCounter());

    segmentAnnouncer.unannounceSegment(firstSegment);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(joiner.join(testSegmentsPath, zNode));
      Assert.assertEquals(segments.iterator().next(), secondSegment);
    }

    segmentAnnouncer.unannounceSegment(secondSegment);

    Assert.assertTrue(cf.getChildren().forPath(testSegmentsPath).isEmpty());

    snapshot = segmentAnnouncer.getSegmentChangesSince(
        snapshot.getCounter()
    ).get();
    Assert.assertEquals(2, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());

    snapshot = segmentAnnouncer.getSegmentChangesSince(
        new SegmentChangeRequestHistory.Counter(-1, -1)
    ).get();
    Assert.assertEquals(0, snapshot.getRequests().size());
    Assert.assertEquals(4, snapshot.getCounter().getCounter());
  }

  @Test
  public void testSkipDimensions() throws Exception
  {
    skipDimensionsAndMetrics = true;
    Iterator<DataSegment> segIter = testSegments.iterator();
    DataSegment firstSegment = segIter.next();

    segmentAnnouncer.announceSegment(firstSegment);

    List<String> zNodes = cf.getChildren().forPath(testSegmentsPath);

    for (String zNode : zNodes) {
      DataSegment announcedSegment = Iterables.getOnlyElement(segmentReader.read(joiner.join(testSegmentsPath, zNode)));
      Assert.assertEquals(announcedSegment, firstSegment);
      Assert.assertTrue(announcedSegment.getDimensions().isEmpty());
      Assert.assertTrue(announcedSegment.getMetrics().isEmpty());
    }

    segmentAnnouncer.unannounceSegment(firstSegment);

    Assert.assertTrue(cf.getChildren().forPath(testSegmentsPath).isEmpty());
  }

  @Test
  public void testSkipLoadSpec() throws Exception
  {
    skipLoadSpec = true;
    Iterator<DataSegment> segIter = testSegments.iterator();
    DataSegment firstSegment = segIter.next();

    segmentAnnouncer.announceSegment(firstSegment);

    List<String> zNodes = cf.getChildren().forPath(testSegmentsPath);

    for (String zNode : zNodes) {
      DataSegment announcedSegment = Iterables.getOnlyElement(segmentReader.read(joiner.join(testSegmentsPath, zNode)));
      Assert.assertEquals(announcedSegment, firstSegment);
      Assert.assertNull(announcedSegment.getLoadSpec());
    }

    segmentAnnouncer.unannounceSegment(firstSegment);

    Assert.assertTrue(cf.getChildren().forPath(testSegmentsPath).isEmpty());
  }

  @Test
  public void testSingleAnnounceManyTimes() throws Exception
  {
    int prevMax = maxBytesPerNode.get();
    maxBytesPerNode.set(2048);
    // each segment is about 348 bytes long and that makes 2048 / 348 = 5 segments included per node
    // so 100 segments makes 100 / 5 = 20 nodes
    try {
      for (DataSegment segment : testSegments) {
        segmentAnnouncer.announceSegment(segment);
      }
    }
    finally {
      maxBytesPerNode.set(prevMax);
    }

    List<String> zNodes = cf.getChildren().forPath(testSegmentsPath);
    Assert.assertEquals(20, zNodes.size());

    Set<DataSegment> segments = Sets.newHashSet(testSegments);
    for (String zNode : zNodes) {
      for (DataSegment segment : segmentReader.read(joiner.join(testSegmentsPath, zNode))) {
        Assert.assertTrue("Invalid segment " + segment, segments.remove(segment));
      }
    }
    Assert.assertTrue("Failed to find segments " + segments, segments.isEmpty());
  }

  @Test
  public void testBatchAnnounce() throws Exception
  {
    testBatchAnnounce(true);
  }

  private void testBatchAnnounce(boolean testHistory) throws Exception
  {
    segmentAnnouncer.announceSegments(testSegments);

    List<String> zNodes = cf.getChildren().forPath(testSegmentsPath);

    Assert.assertEquals(2, zNodes.size());

    Set<DataSegment> allSegments = Sets.newHashSet();
    for (String zNode : zNodes) {
      allSegments.addAll(segmentReader.read(joiner.join(testSegmentsPath, zNode)));
    }
    Assert.assertEquals(allSegments, testSegments);

    SegmentChangeRequestsSnapshot snapshot = null;

    if (testHistory) {
      snapshot = segmentAnnouncer.getSegmentChangesSince(
          new SegmentChangeRequestHistory.Counter(-1, -1)
      ).get();
      Assert.assertEquals(testSegments.size(), snapshot.getRequests().size());
    Assert.assertEquals(testSegments.size(), snapshot.getCounter().getCounter());
  }

    segmentAnnouncer.unannounceSegments(testSegments);

    Assert.assertTrue(cf.getChildren().forPath(testSegmentsPath).isEmpty());

    if (testHistory) {
      snapshot = segmentAnnouncer.getSegmentChangesSince(
          snapshot.getCounter()
      ).get();
      Assert.assertEquals(testSegments.size(), snapshot.getRequests().size());
      Assert.assertEquals(2 * testSegments.size(), snapshot.getCounter().getCounter());

      snapshot = segmentAnnouncer.getSegmentChangesSince(
          new SegmentChangeRequestHistory.Counter(-1, -1)
      ).get();
      Assert.assertEquals(0, snapshot.getRequests().size());
      Assert.assertEquals(2 * testSegments.size(), snapshot.getCounter().getCounter());
    }
  }

  @Test
  public void testMultipleBatchAnnounce() throws Exception
  {
    for (int i = 0; i < 10; i++) {
      testBatchAnnounce(false);
    }
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
                      .dimensions(ImmutableList.<String>of("dim1", "dim2"))
                      .metrics(ImmutableList.<String>of("met1", "met2"))
                      .loadSpec(ImmutableMap.<String, Object>of("type", "local"))
                      .build();
  }

  private static class SegmentReader
  {
    private final CuratorFramework cf;
    private final ObjectMapper jsonMapper;

    public SegmentReader(CuratorFramework cf, ObjectMapper jsonMapper)
    {
      this.cf = cf;
      this.jsonMapper = jsonMapper;
    }

    public Set<DataSegment> read(String path)
    {
      try {
        if (cf.checkExists().forPath(path) != null) {
          return jsonMapper.readValue(
              cf.getData().forPath(path), new TypeReference<Set<DataSegment>>()
              {
              }
          );
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      return Sets.newHashSet();
    }
  }
}
