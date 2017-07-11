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

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.ImmutableDruidServer;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 */
public class DruidCoordinatorBalancerTest
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private DruidCoordinator coordinator;
  private ImmutableDruidServer druidServer1;
  private ImmutableDruidServer druidServer2;
  private ImmutableDruidServer druidServer3;
  private ImmutableDruidServer druidServer4;
  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;
  Map<String, DataSegment> segments;

  @Before
  public void setUp() throws Exception
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    druidServer1 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer2 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer3 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer4 = EasyMock.createMock(ImmutableDruidServer.class);
    segment1 = EasyMock.createMock(DataSegment.class);
    segment2 = EasyMock.createMock(DataSegment.class);
    segment3 = EasyMock.createMock(DataSegment.class);
    segment4 = EasyMock.createMock(DataSegment.class);

    DateTime start1 = new DateTime("2012-01-01");
    DateTime start2 = new DateTime("2012-02-01");
    DateTime version = new DateTime("2012-03-01");
    segment1 = new DataSegment(
        "datasource1",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        11L
    );
    segment2 = new DataSegment(
        "datasource1",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        7L
    );
    segment3 = new DataSegment(
        "datasource2",
        new Interval(start1, start1.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        4L
    );
    segment4 = new DataSegment(
        "datasource2",
        new Interval(start2, start2.plusHours(1)),
        version.toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        8L
    );

    segments = new HashMap<String, DataSegment>();
    segments.put("datasource1_2012-01-01T00:00:00.000Z_2012-01-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment1);
    segments.put("datasource1_2012-02-01T00:00:00.000Z_2012-02-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment2);
    segments.put("datasource2_2012-01-01T00:00:00.000Z_2012-01-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment3);
    segments.put("datasource2_2012-02-01T00:00:00.000Z_2012-02-01T01:00:00.000Z_2012-03-01T00:00:00.000Z", segment4);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(coordinator);
    EasyMock.verify(druidServer1);
    EasyMock.verify(druidServer2);
    EasyMock.verify(druidServer3);
    EasyMock.verify(druidServer4);
  }


  @Test
  public void testMoveToEmptyServerBalancer() throws IOException
  {
    EasyMock.expect(druidServer1.getName()).andReturn("from").atLeastOnce();
    EasyMock.expect(druidServer1.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer1.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer1.getSegments()).andReturn(segments).anyTimes();
    EasyMock.expect(druidServer1.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getName()).andReturn("to").atLeastOnce();
    EasyMock.expect(druidServer2.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer2.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServer2.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer2.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
    EasyMock.expect(druidServer2.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer2);

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    // Mock stuff that the coordinator needs
    coordinator.moveSegment(
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);

    LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
    LoadQueuePeonTester toPeon = new LoadQueuePeonTester();
    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                .withDruidCluster(
                                    new DruidCluster(
                                        null,
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                                                               .create(
                                                                   Arrays.asList(
                                                                       new ServerHolder(druidServer1, fromPeon),
                                                                       new ServerHolder(druidServer2, toPeon)
                                                                   )
                                                               )
                                        )
                                    )
                                )
                                .withLoadManagementPeons(
                                    ImmutableMap.<String, LoadQueuePeon>of(
                                        "from",
                                        fromPeon,
                                        "to",
                                        toPeon
                                    )
                                )
                                .withAvailableSegments(segments.values())
                                .withDynamicConfigs(
                                    new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                                        MAX_SEGMENTS_TO_MOVE
                                    ).build()
                                )
                                     .withBalancerStrategy(balancerStrategy)
                                     .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") < segments.size());
    exec.shutdown();
  }


  @Test
  public void testRun1() throws IOException
  {
    // Mock some servers of different usages

    EasyMock.expect(druidServer1.getName()).andReturn("from").atLeastOnce();
    EasyMock.expect(druidServer1.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer1.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer1.getSegments()).andReturn(segments).anyTimes();
    EasyMock.expect(druidServer1.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getName()).andReturn("to").atLeastOnce();
    EasyMock.expect(druidServer2.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer2.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServer2.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer2.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
    EasyMock.expect(druidServer2.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer2);

    EasyMock.replay(druidServer3);
    EasyMock.replay(druidServer4);

    // Mock stuff that the coordinator needs
    coordinator.moveSegment(
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
    LoadQueuePeonTester toPeon = new LoadQueuePeonTester();
    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                .withDruidCluster(
                                    new DruidCluster(
                                        null,
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                                                               .create(
                                                                   Arrays.asList(
                                                                       new ServerHolder(druidServer1, fromPeon),
                                                                       new ServerHolder(druidServer2, toPeon)
                                                                   )
                                                               )
                                        )
                                    )
                                )
                                .withLoadManagementPeons(
                                    ImmutableMap.<String, LoadQueuePeon>of(
                                        "from",
                                        fromPeon,
                                        "to",
                                        toPeon
                                    )
                                )
                                .withAvailableSegments(segments.values())
                                .withDynamicConfigs(
                                    new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE)
                                                                     .build()
                                )
                                     .withBalancerStrategy(balancerStrategy)
                                     .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
    exec.shutdown();
  }


  @Test
  public void testRun2() throws IOException
  {
    // Mock some servers of different usages
    EasyMock.expect(druidServer1.getName()).andReturn("1").atLeastOnce();
    EasyMock.expect(druidServer1.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer1.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer1.getSegments()).andReturn(segments).anyTimes();
    EasyMock.expect(druidServer1.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getName()).andReturn("2").atLeastOnce();
    EasyMock.expect(druidServer2.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer2.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServer2.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer2.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
    EasyMock.expect(druidServer2.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer2);

    EasyMock.expect(druidServer3.getName()).andReturn("3").atLeastOnce();
    EasyMock.expect(druidServer3.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer3.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServer3.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer3.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
    EasyMock.expect(druidServer3.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer3);

    EasyMock.expect(druidServer4.getName()).andReturn("4").atLeastOnce();
    EasyMock.expect(druidServer4.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer4.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServer4.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer4.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
    EasyMock.expect(druidServer4.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer4);

    // Mock stuff that the coordinator needs
    coordinator.moveSegment(
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<ImmutableDruidServer>anyObject(),
        EasyMock.<DataSegment>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);

    LoadQueuePeonTester peon1 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon2 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon3 = new LoadQueuePeonTester();
    LoadQueuePeonTester peon4 = new LoadQueuePeonTester();

    ListeningExecutorService exec = MoreExecutors.listeningDecorator(
            Executors.newFixedThreadPool(1));
    BalancerStrategy balancerStrategy =
            new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                .withDruidCluster(
                                    new DruidCluster(
                                        null,
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                                                               .create(
                                                                   Arrays.asList(
                                                                       new ServerHolder(druidServer1, peon1),
                                                                       new ServerHolder(druidServer2, peon2),
                                                                       new ServerHolder(druidServer3, peon3),
                                                                       new ServerHolder(druidServer4, peon4)
                                                                   )
                                                               )
                                        )
                                    )
                                )
                                .withLoadManagementPeons(
                                    ImmutableMap.<String, LoadQueuePeon>of(
                                        "1",
                                        peon1,
                                        "2",
                                        peon2,
                                        "3",
                                        peon3,
                                        "4",
                                        peon4
                                    )
                                )
                                .withAvailableSegments(segments.values())
                                .withDynamicConfigs(
                                    new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                                        MAX_SEGMENTS_TO_MOVE
                                    ).build()
                                )
                                     .withBalancerStrategy(balancerStrategy)
                                     .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                .build();

    params = new DruidCoordinatorBalancerTester(coordinator).run(params);
    Assert.assertTrue(params.getCoordinatorStats().getTieredStat("movedCount", "normal") > 0);
    exec.shutdown();
  }

}
