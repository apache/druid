package com.metamx.druid.master;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;


/**
 */
public class DruidMasterReplicatorTest
{
  private DruidMaster master;
  private DruidServer server1;
  private DruidServer server2;
  private DruidServer server3;
  private DruidDataSource dataSource1;
  private DruidDataSource dataSource2;
  private DruidDataSource dataSource3;
  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private LoadQueuePeon peon;

  @Before
  public void setUp() throws Exception
  {
    master = EasyMock.createMock(DruidMaster.class);
    server1 = EasyMock.createMock(DruidServer.class);
    server2 = EasyMock.createMock(DruidServer.class);
    server3 = EasyMock.createMock(DruidServer.class);
    dataSource1 = EasyMock.createMock(DruidDataSource.class);
    dataSource2 = EasyMock.createMock(DruidDataSource.class);
    dataSource3 = EasyMock.createMock(DruidDataSource.class);
    segment1 = EasyMock.createMock(DataSegment.class);
    segment2 = EasyMock.createMock(DataSegment.class);
    segment3 = EasyMock.createMock(DataSegment.class);
    peon = EasyMock.createMock(LoadQueuePeon.class);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(master);
    EasyMock.verify(server1);
    EasyMock.verify(server2);
    EasyMock.verify(server3);
    EasyMock.verify(dataSource1);
    EasyMock.verify(dataSource2);
    EasyMock.verify(dataSource3);
    EasyMock.verify(segment1);
    EasyMock.verify(segment2);
    EasyMock.verify(segment3);
    EasyMock.verify(peon);
  }

  @Test
  public void testRun()
  {
    // Mock some servers
    mockServer(server1, "server1", dataSource1);
    mockServer(server2, "server2", dataSource2);
    mockServer(server3, "server3", dataSource3);

    // Mock some data sources
    EasyMock.expect(dataSource1.getSegments()).andReturn(
        Sets.newHashSet(segment1, segment2, segment3)
    );
    EasyMock.replay(dataSource1);

    EasyMock.expect(dataSource2.getSegments()).andReturn(
        Sets.newHashSet(segment1, segment2)
    );
    EasyMock.replay(dataSource2);

    EasyMock.expect(dataSource3.getSegments()).andReturn(
        Sets.newHashSet(segment2)
    );
    EasyMock.replay(dataSource3);

    // Mock the segments on those data sources
    EasyMock.expect(segment1.getIdentifier()).andReturn("segment1").anyTimes();
    EasyMock.expect(segment1.getSize()).andReturn(1L).anyTimes();
    EasyMock.expect(segment1.getInterval()).andReturn(new Interval(0, Integer.MAX_VALUE)).atLeastOnce();
    EasyMock.expect(segment1.compareTo(EasyMock.<DataSegment>anyObject())).andReturn(0).anyTimes();
    EasyMock.replay(segment1);

    EasyMock.expect(segment2.getIdentifier()).andReturn("segment2").anyTimes();
    EasyMock.expect(segment2.getSize()).andReturn(1L).anyTimes();
    EasyMock.expect(segment2.getInterval()).andReturn(new Interval(0, Integer.MAX_VALUE)).atLeastOnce();
    EasyMock.expect(segment2.compareTo(EasyMock.<DataSegment>anyObject())).andReturn(0).anyTimes();
    EasyMock.replay(segment2);

    EasyMock.expect(segment3.getIdentifier()).andReturn("segment3").anyTimes();
    EasyMock.expect(segment3.getSize()).andReturn(1L).anyTimes();
    EasyMock.expect(segment3.getInterval()).andReturn(new Interval(0, Integer.MAX_VALUE)).atLeastOnce();
    EasyMock.expect(segment3.compareTo(EasyMock.<DataSegment>anyObject())).andReturn(0).anyTimes();
    EasyMock.replay(segment3);

    // Mock master stuff
    EasyMock.expect(peon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(peon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet());
    EasyMock.expect(peon.getSegmentsToDrop()).andReturn(Sets.<DataSegment>newHashSet());
    EasyMock.replay(peon);

    master.cloneSegment(
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall();
    master.dropSegment(
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall();
    EasyMock.replay(master);

    DruidMasterRuntimeParams params =
        DruidMasterRuntimeParams.newBuilder()
                                .withHistoricalServers(Arrays.asList(server1, server2, server3))
                                .withLoadManagementPeons(
                                    ImmutableMap.of(
                                        "server1",
                                        peon,
                                        "server2",
                                        peon,
                                        "server3",
                                        peon
                                    )
                                )
                                .withAvailableSegments(Arrays.asList(segment1, segment2, segment3))
                                .build();
    params = new DruidMasterReplicator(master).run(params);

    Assert.assertEquals(params.getCreatedReplicantCount(), 1);
    Assert.assertEquals(params.getDestroyedReplicantCount(), 1);
  }

  private void mockServer(DruidServer server, String serverName, DruidDataSource dataSource)
  {
    EasyMock.expect(server.getDataSources()).andReturn(Arrays.asList(dataSource)).anyTimes();
    EasyMock.expect(server.getName()).andReturn(serverName).anyTimes();
    EasyMock.expect(server.getCurrSize()).andReturn(1L).anyTimes();
    EasyMock.expect(server.getMaxSize()).andReturn(10L).anyTimes();
    EasyMock.expect(server.getSegment("segment1")).andReturn(null).anyTimes();
    EasyMock.expect(server.getSegment("segment2")).andReturn(null).anyTimes();
    EasyMock.expect(server.getSegment("segment3")).andReturn(null).anyTimes();
    EasyMock.replay(server);
  }
}
