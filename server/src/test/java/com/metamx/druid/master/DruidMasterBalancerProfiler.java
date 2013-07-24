package com.metamx.druid.master;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.shard.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DruidMasterBalancerProfiler
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private DruidMaster master;
  private DruidServer druidServer1;
  private DruidServer druidServer2;
  Map<String, DataSegment> segments = Maps.newHashMap();
  DateTime start1 = new DateTime("2012-01-01");
  DateTime version = new DateTime("2012-03-01");

  @Before
  public void setUp() throws Exception
  {
    master = EasyMock.createMock(DruidMaster.class);
    druidServer1 = EasyMock.createMock(DruidServer.class);
    druidServer2 = EasyMock.createMock(DruidServer.class);
    for (int i=0; i<55000;i++)
    {
      DataSegment segment = new DataSegment(
          "datasource1",
          new Interval(start1, start1.plusHours(1)),
          version.toString(),
          Maps.<String, Object>newHashMap(),
          Lists.<String>newArrayList(),
          Lists.<String>newArrayList(),
          new NoneShardSpec(),
          0,
          11L
      );
      segments.put("datasource"+i+"_2012-01-01T00:00:00.000Z_2012-01-01T01:00:00.000Z_2012-03-01T00:00:00.000Z",segment);
    }
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(master);
    EasyMock.verify(druidServer1);
    EasyMock.verify(druidServer2);
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
    params = new DruidMasterBalancerTester(master).run(params);
    System.out.println(stopwatch.elapsed(TimeUnit.MILLISECONDS));
 }

}
