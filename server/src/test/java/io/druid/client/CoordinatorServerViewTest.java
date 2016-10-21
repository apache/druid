/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import io.druid.curator.CuratorTestBase;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;
import io.druid.query.TableDataSource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineLookup;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.PartitionHolder;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class CoordinatorServerViewTest extends CuratorTestBase
{
  private final ObjectMapper jsonMapper;
  private final ZkPathsConfig zkPathsConfig;
  private final String inventoryPath;

  private CountDownLatch segmentViewInitLatch;
  private CountDownLatch segmentAddedLatch;
  private CountDownLatch segmentRemovedLatch;

  private ServerInventoryView baseView;
  private CoordinatorServerView overlordServerView;

  public CoordinatorServerViewTest()
  {
    jsonMapper = new DefaultObjectMapper();
    zkPathsConfig = new ZkPathsConfig();
    inventoryPath = zkPathsConfig.getLiveSegmentsPath();
  }

  @Before
  public void setUp() throws Exception
  {
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
  }

  @Test
  public void testSingleServerAddedRemovedSegment() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(1);
    segmentRemovedLatch = new CountDownLatch(1);

    setupViews();

    final DruidServer druidServer = new DruidServer(
        "localhost:1234",
        "localhost:1234",
        10000000L,
        "historical",
        "default_tier",
        0
    );

    setupZNodeForServer(druidServer, zkPathsConfig, jsonMapper);

    final DataSegment segment = dataSegmentWithIntervalAndVersion("2014-10-20T00:00:00Z/P1D", "v1");
    announceSegmentForServer(druidServer, segment, zkPathsConfig, jsonMapper);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineLookup timeline = overlordServerView.getTimeline(new TableDataSource("test_overlord_server_view"));
    List<TimelineObjectHolder> serverLookupRes = (List<TimelineObjectHolder>) timeline.lookup(
        new Interval(
            "2014-10-20T00:00:00Z/P1D"
        )
    );
    Assert.assertEquals(1, serverLookupRes.size());

    TimelineObjectHolder<String, SegmentLoadInfo> actualTimelineObjectHolder = serverLookupRes.get(0);
    Assert.assertEquals(new Interval("2014-10-20T00:00:00Z/P1D"), actualTimelineObjectHolder.getInterval());
    Assert.assertEquals("v1", actualTimelineObjectHolder.getVersion());

    PartitionHolder<SegmentLoadInfo> actualPartitionHolder = actualTimelineObjectHolder.getObject();
    Assert.assertTrue(actualPartitionHolder.isComplete());
    Assert.assertEquals(1, Iterables.size(actualPartitionHolder));

    SegmentLoadInfo segmentLoadInfo = actualPartitionHolder.iterator().next().getObject();
    Assert.assertFalse(segmentLoadInfo.isEmpty());
    Assert.assertEquals(
        druidServer.getMetadata(),
        Iterables.getOnlyElement(segmentLoadInfo.toImmutableSegmentLoadInfo().getServers())
    );

    unannounceSegmentForServer(druidServer, segment);
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    Assert.assertEquals(
        0,
        ((List<TimelineObjectHolder>) timeline.lookup(new Interval("2014-10-20T00:00:00Z/P1D"))).size()
    );
    Assert.assertNull(timeline.findEntry(new Interval("2014-10-20T00:00:00Z/P1D"), "v1"));
  }

  @Test
  public void testMultipleServerAddedRemovedSegment() throws Exception
  {
    segmentViewInitLatch = new CountDownLatch(1);
    segmentAddedLatch = new CountDownLatch(5);

    // temporarily set latch count to 1
    segmentRemovedLatch = new CountDownLatch(1);

    setupViews();

    final List<DruidServer> druidServers = Lists.transform(
        ImmutableList.<String>of("locahost:0", "localhost:1", "localhost:2", "localhost:3", "localhost:4"),
        new Function<String, DruidServer>()
        {
          @Override
          public DruidServer apply(String input)
          {
            return new DruidServer(
                input,
                input,
                10000000L,
                "historical",
                "default_tier",
                0
            );
          }
        }
    );

    for (DruidServer druidServer : druidServers) {
      setupZNodeForServer(druidServer, zkPathsConfig, jsonMapper);
    }

    final List<DataSegment> segments = Lists.transform(
        ImmutableList.<Pair<String, String>>of(
            Pair.of("2011-04-01/2011-04-03", "v1"),
            Pair.of("2011-04-03/2011-04-06", "v1"),
            Pair.of("2011-04-01/2011-04-09", "v2"),
            Pair.of("2011-04-06/2011-04-09", "v3"),
            Pair.of("2011-04-01/2011-04-02", "v3")
        ), new Function<Pair<String, String>, DataSegment>()
        {
          @Override
          public DataSegment apply(Pair<String, String> input)
          {
            return dataSegmentWithIntervalAndVersion(input.lhs, input.rhs);
          }
        }
    );

    for (int i = 0; i < 5; ++i) {
      announceSegmentForServer(druidServers.get(i), segments.get(i), zkPathsConfig, jsonMapper);
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentViewInitLatch));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentAddedLatch));

    TimelineLookup timeline = overlordServerView.getTimeline(new TableDataSource("test_overlord_server_view"));
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", druidServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-06", "v2", druidServers.get(2), segments.get(2)),
            createExpected("2011-04-06/2011-04-09", "v3", druidServers.get(3), segments.get(3))
        ),
        (List<TimelineObjectHolder>) timeline.lookup(
            new Interval(
                "2011-04-01/2011-04-09"
            )
        )
    );

    // unannounce the segment created by dataSegmentWithIntervalAndVersion("2011-04-01/2011-04-09", "v2")
    unannounceSegmentForServer(druidServers.get(2), segments.get(2));
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    // renew segmentRemovedLatch since we still have 4 segments to unannounce
    segmentRemovedLatch = new CountDownLatch(4);

    timeline = overlordServerView.getTimeline(new TableDataSource("test_overlord_server_view"));
    assertValues(
        Arrays.asList(
            createExpected("2011-04-01/2011-04-02", "v3", druidServers.get(4), segments.get(4)),
            createExpected("2011-04-02/2011-04-03", "v1", druidServers.get(0), segments.get(0)),
            createExpected("2011-04-03/2011-04-06", "v1", druidServers.get(1), segments.get(1)),
            createExpected("2011-04-06/2011-04-09", "v3", druidServers.get(3), segments.get(3))
        ),
        (List<TimelineObjectHolder>) timeline.lookup(
            new Interval(
                "2011-04-01/2011-04-09"
            )
        )
    );

    // unannounce all the segments
    for (int i = 0; i < 5; ++i) {
      // skip the one that was previously unannounced
      if (i != 2) {
        unannounceSegmentForServer(druidServers.get(i), segments.get(i));
      }
    }
    Assert.assertTrue(timing.forWaiting().awaitLatch(segmentRemovedLatch));

    Assert.assertEquals(
        0,
        ((List<TimelineObjectHolder>) timeline.lookup(new Interval("2011-04-01/2011-04-09"))).size()
    );
  }

  private void unannounceSegmentForServer(DruidServer druidServer, DataSegment segment) throws Exception
  {
    curator.delete().guaranteed().forPath(
        ZKPaths.makePath(
            ZKPaths.makePath(inventoryPath, druidServer.getHost()),
            segment.getIdentifier()
        )
    );
  }

  private Pair<Interval, Pair<String, Pair<DruidServer, DataSegment>>> createExpected(
      String intervalStr,
      String version,
      DruidServer druidServer,
      DataSegment segment
  )
  {
    return Pair.of(new Interval(intervalStr), Pair.of(version, Pair.of(druidServer, segment)));
  }

  private void assertValues(
      List<Pair<Interval, Pair<String, Pair<DruidServer, DataSegment>>>> expected, List<TimelineObjectHolder> actual
  )
  {
    Assert.assertEquals(expected.size(), actual.size());

    for (int i = 0; i < expected.size(); ++i) {
      Pair<Interval, Pair<String, Pair<DruidServer, DataSegment>>> expectedPair = expected.get(i);
      TimelineObjectHolder<String, SegmentLoadInfo> actualTimelineObjectHolder = actual.get(i);

      Assert.assertEquals(expectedPair.lhs, actualTimelineObjectHolder.getInterval());
      Assert.assertEquals(expectedPair.rhs.lhs, actualTimelineObjectHolder.getVersion());

      PartitionHolder<SegmentLoadInfo> actualPartitionHolder = actualTimelineObjectHolder.getObject();
      Assert.assertTrue(actualPartitionHolder.isComplete());
      Assert.assertEquals(1, Iterables.size(actualPartitionHolder));

      SegmentLoadInfo segmentLoadInfo = actualPartitionHolder.iterator().next().getObject();
      Assert.assertFalse(segmentLoadInfo.isEmpty());
      Assert.assertEquals(expectedPair.rhs.rhs.lhs.getMetadata(),
                          Iterables.getOnlyElement(segmentLoadInfo.toImmutableSegmentLoadInfo().getServers()));
    }
  }

  private void setupViews() throws Exception
  {
    baseView = new BatchServerInventoryView(
        zkPathsConfig,
        curator,
        jsonMapper,
        Predicates.<Pair<DruidServerMetadata, DataSegment>>alwaysTrue()
    )
    {
      @Override
      public void registerSegmentCallback(Executor exec, final SegmentCallback callback)
      {
        super.registerSegmentCallback(
            exec, new SegmentCallback()
            {
              @Override
              public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentAdded(server, segment);
                segmentAddedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
              {
                CallbackAction res = callback.segmentRemoved(server, segment);
                segmentRemovedLatch.countDown();
                return res;
              }

              @Override
              public CallbackAction segmentViewInitialized()
              {
                CallbackAction res = callback.segmentViewInitialized();
                segmentViewInitLatch.countDown();
                return res;
              }
            }
        );
      }
    };

    overlordServerView = new CoordinatorServerView(
        baseView
    );

    baseView.start();
  }

  private DataSegment dataSegmentWithIntervalAndVersion(String intervalStr, String version)
  {
    return DataSegment.builder()
                      .dataSource("test_overlord_server_view")
                      .interval(new Interval(intervalStr))
                      .loadSpec(
                          ImmutableMap.<String, Object>of(
                              "type",
                              "local",
                              "path",
                              "somewhere"
                          )
                      )
                      .version(version)
                      .dimensions(ImmutableList.<String>of())
                      .metrics(ImmutableList.<String>of())
                      .shardSpec(NoneShardSpec.instance())
                      .binaryVersion(9)
                      .size(0)
                      .build();
  }

  @After
  public void tearDown() throws Exception
  {
    baseView.stop();
    tearDownServerAndCurator();
  }
}
