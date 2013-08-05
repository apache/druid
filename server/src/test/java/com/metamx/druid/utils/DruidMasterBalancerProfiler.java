package com.metamx.druid.utils;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.master.DruidCluster;
import com.metamx.druid.master.DruidMaster;
import com.metamx.druid.master.DruidMasterBalancerTester;
import com.metamx.druid.master.DruidMasterRuleRunner;
import com.metamx.druid.master.DruidMasterRuntimeParams;
import com.metamx.druid.master.LoadPeonCallback;
import com.metamx.druid.master.LoadQueuePeon;
import com.metamx.druid.master.LoadQueuePeonTester;
import com.metamx.druid.master.ReplicationThrottler;
import com.metamx.druid.master.SegmentReplicantLookup;
import com.metamx.druid.master.ServerHolder;
import com.metamx.druid.master.rules.PeriodLoadRule;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
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
  DatabaseRuleManager manager;
  PeriodLoadRule loadRule = new PeriodLoadRule(new Period("P5000Y"),3,"normal");
  List<Rule> rules = ImmutableList.<Rule>of(loadRule);
  @Before
  public void setUp() throws Exception
  {
    master = EasyMock.createMock(DruidMaster.class);
    druidServer1 = EasyMock.createMock(DruidServer.class);
    druidServer2 = EasyMock.createMock(DruidServer.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    manager = EasyMock.createMock(DatabaseRuleManager.class);
  }

  @Test
  public void bigProfiler()
  {
    Stopwatch watch = new Stopwatch();
    int numServers = 1000;

    EasyMock.expect(manager.getAllRules()).andReturn(ImmutableMap.<String, List<Rule>>of("test", rules)).anyTimes();
    EasyMock.expect(manager.getRules(EasyMock.<String>anyObject())).andReturn(rules).anyTimes();
    EasyMock.expect(manager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(rules).anyTimes();
    EasyMock.replay(manager);

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
      EasyMock.expect(server.getTier()).andReturn("normal").anyTimes();
      EasyMock.expect(server.getName()).andReturn(Integer.toString(i)).atLeastOnce();
      EasyMock.expect(server.getHost()).andReturn(Integer.toString(i)).anyTimes();
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
                                .withDatabaseRuleManager(manager)
                                .withReplicationManager(new ReplicationThrottler(2, 500))
                                .withSegmentReplicantLookup(
                                    SegmentReplicantLookup.make(new DruidCluster(
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidMasterBalancerTester.percentUsedComparator)
                                                               .create(
                                                                   serverHolderList
                                                               )
                                        )
                                    )
                                    )
                                )
                                .build();

    DruidMasterBalancerTester tester = new DruidMasterBalancerTester(master);
    DruidMasterRuleRunner runner = new DruidMasterRuleRunner(master,500,5);
    watch.start();
    while (!tester.isBalanced(40,50))
    {
      params = tester.run(params);
      params = runner.run(params);
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
