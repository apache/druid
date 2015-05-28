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

package io.druid.server.coordination.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
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

import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
    cf.create().creatingParentsIfNeeded().forPath(testBasePath);

    jsonMapper = new DefaultObjectMapper();

    announcer = new Announcer(
        cf,
        "BatchDataAnnouncerTest-%s"
    );
    announcer.start();

    segmentReader = new SegmentReader(cf, jsonMapper);
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

    testSegments = Sets.newHashSet();
    for (int i = 0; i < 100; i++) {
      testSegments.add(makeSegment(i));
    }
  }

  @After
  public void tearDown() throws Exception
  {
    segmentAnnouncer.stop();
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

    segmentAnnouncer.unannounceSegment(firstSegment);

    for (String zNode : zNodes) {
      Set<DataSegment> segments = segmentReader.read(joiner.join(testSegmentsPath, zNode));
      Assert.assertEquals(segments.iterator().next(), secondSegment);
    }

    segmentAnnouncer.unannounceSegment(secondSegment);

    Assert.assertTrue(cf.getChildren().forPath(testSegmentsPath).isEmpty());
  }

  @Test
  public void testBatchAnnounce() throws Exception
  {
    segmentAnnouncer.announceSegments(testSegments);

    List<String> zNodes = cf.getChildren().forPath(testSegmentsPath);

    Assert.assertTrue(zNodes.size() == 2);

    Set<DataSegment> allSegments = Sets.newHashSet();
    for (String zNode : zNodes) {
      allSegments.addAll(segmentReader.read(joiner.join(testSegmentsPath, zNode)));
    }
    Assert.assertEquals(allSegments, testSegments);

    segmentAnnouncer.unannounceSegments(testSegments);

    Assert.assertTrue(cf.getChildren().forPath(testSegmentsPath).isEmpty());
  }

  @Test
  public void testMultipleBatchAnnounce() throws Exception
  {
    for (int i = 0; i < 10; i++) {
       testBatchAnnounce();
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
                      .build();
  }

  private class SegmentReader
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
