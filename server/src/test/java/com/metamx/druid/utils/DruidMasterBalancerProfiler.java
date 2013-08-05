package com.metamx.druid.utils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.master.DruidCluster;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterBalancerTester;
import com.metamx.druid.master.DruidMasterRuntimeParams;
import com.metamx.druid.master.LoadPeonCallback;
import com.metamx.druid.master.LoadQueuePeon;
import com.metamx.druid.master.LoadQueuePeonTester;
import com.metamx.druid.master.ServerHolder;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidMasterBalancerProfiler
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private DruidMaster master;
  private DruidServer druidServer1;
  private DruidServer druidServer2;
  Map<String, DataSegment> segments = Maps.newHashMap();
  DateTime start1 = new DateTime("2012-01-01");
  DateTime version = new DateTime("2012-03-01");
  ServiceEmitter emitter;
  @Before
  public void setUp() throws Exception
  {
    master = EasyMock.createMock(DruidMaster.class);
    druidServer1 = EasyMock.createMock(DruidServer.class);
    druidServer2 = EasyMock.createMock(DruidServer.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
  }

  @Test
  public void bigProfiler()
  {
    Stopwatch watch = new Stopwatch();
    int numServers = 1000;
    master.moveSegment(
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );

    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(master);

    List<DruidServer> serverList = Lists.newArrayList();
    Map<String, LoadQueuePeon> peonMap = Maps.newHashMap();
    List<ServerHolder> serverHolderList = Lists.newArrayList();
    Map<String,DataSegment> segmentMap = Maps.newHashMap();
    for (int i=0;i<numServers;i++)
    {
      segmentMap.put(
          "segment" + i,
          new DataSegment(
              "datasource" + i,
              new Interval(new DateTime("2012-01-01"), (new DateTime("2012-01-01")).plusHours(1)),
              (new DateTime("2012-03-01")).toString(),
              Maps.<String, Object>newHashMap(),
              Lists.<String>newArrayList(),
              Lists.<String>newArrayList(),
              new NoneShardSpec(),
              0,
              4L
          )
      );
    }

    for (int i=0;i<50;i++)
    {
      DruidServer server =EasyMock.createMock(DruidServer.class);
      EasyMock.expect(server.getMetadata()).andReturn(null).anyTimes();
      EasyMock.expect(server.getCurrSize()).andReturn(30L).atLeastOnce();
      EasyMock.expect(server.getMaxSize()).andReturn(100L).atLeastOnce();
      EasyMock.expect(server.getName()).andReturn(Integer.toString(i)).atLeastOnce();
      if (i==0)
      {
        EasyMock.expect(server.getSegments()).andReturn(segmentMap).anyTimes();
      }
      else
      {
        EasyMock.expect(server.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
      }
      EasyMock.expect(server.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
      EasyMock.replay(server);

      LoadQueuePeon peon = new LoadQueuePeonTester();
      peonMap.put(Integer.toString(i),peon);
      serverHolderList.add(new ServerHolder(server, peon));
    }

    DruidMasterRuntimeParams params =
        DruidMasterRuntimeParams.newBuilder()
                                .withDruidCluster(
                                    new DruidCluster(
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidMasterBalancerTester.percentUsedComparator)
                                                               .create(
                                                                   serverHolderList
                                                               )
                                        )
                                    )
                                )
                                .withLoadManagementPeons(
                                    peonMap
                                )
                                .withAvailableSegments(segmentMap.values())
                                .withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE)
                                .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                .withEmitter(emitter)
                                .build();

    DruidMasterBalancerTester tester = new DruidMasterBalancerTester(master);
    watch.start();
    while (!tester.isBalanced(20,50))
    {
      params = tester.run(params);
    }
    System.out.println(watch.stop());
  }



  @Test
  public void profileRun(){
  Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
  LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
  LoadQueuePeonTester toPeon = new LoadQueuePeonTester();

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

  master.moveSegment(
      EasyMock.<String>anyObject(),
      EasyMock.<String>anyObject(),
      EasyMock.<String>anyObject(),
      EasyMock.<LoadPeonCallback>anyObject()
  );
  EasyMock.expectLastCall().anyTimes();
  EasyMock.replay(master);

  DruidMasterRuntimeParams params =
      DruidMasterRuntimeParams.newBuilder()
                              .withDruidCluster(
                                  new DruidCluster(
                                      ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                          "normal",
                                          MinMaxPriorityQueue.orderedBy(DruidMasterBalancerTester.percentUsedComparator)
                                                             .create(
                                                                 Arrays.asList(
                                                                     new ServerHolder(druidServer1, fromPeon),
                                                                     new ServerHolder(druidServer2, toPeon)
                                                                 )
                                                             )
                                      )
                                  )
                              )
                              .withLoadManagementPeons(ImmutableMap.<String, LoadQueuePeon>of("from", fromPeon, "to", toPeon))
                              .withAvailableSegments(segments.values())
                              .withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE)
                              .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                              .build();
    DruidMasterBalancerTester tester = new DruidMasterBalancerTester(master);

 }

}
