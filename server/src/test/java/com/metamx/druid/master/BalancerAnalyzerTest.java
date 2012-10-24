package com.metamx.druid.master;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class BalancerAnalyzerTest
{
  private ServerHolder high;
  private ServerHolder low;
  private DruidServer server;
  private DruidDataSource dataSource;
  private DataSegment segment;

  @Before
  public void setUp() throws Exception
  {
    high = EasyMock.createMock(ServerHolder.class);
    low = EasyMock.createMock(ServerHolder.class);
    server = EasyMock.createMock(DruidServer.class);
    dataSource = EasyMock.createMock(DruidDataSource.class);
    segment = EasyMock.createMock(DataSegment.class);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(high);
    EasyMock.verify(low);
  }

  @Test
  public void testGetPercentDifference()
  {
    EasyMock.expect(high.getSizeUsed()).andReturn(6L);
    EasyMock.expect(high.getPercentUsed()).andReturn(60.0);
    EasyMock.expect(high.getMaxSize()).andReturn(10L);
    EasyMock.replay(high);

    EasyMock.expect(low.getSizeUsed()).andReturn(2L);
    EasyMock.expect(low.getPercentUsed()).andReturn(20.0);
    EasyMock.expect(low.getMaxSize()).andReturn(10L);
    EasyMock.replay(low);

    BalancerAnalyzer analyzer = new BalancerAnalyzer();

    analyzer.init(high, low);
    Assert.assertEquals(100.0, analyzer.getPercentDiff());
  }

  @Test
  public void testGetLookaheadPercentDifference()
  {
    EasyMock.expect(high.getSizeUsed()).andReturn(2L);
    EasyMock.expect(high.getPercentUsed()).andReturn(20.0);
    EasyMock.expect(high.getMaxSize()).andReturn(10L);
    EasyMock.replay(high);

    EasyMock.expect(low.getSizeUsed()).andReturn(1L);
    EasyMock.expect(low.getPercentUsed()).andReturn(10.0);
    EasyMock.expect(low.getMaxSize()).andReturn(10L);
    EasyMock.replay(low);

    BalancerAnalyzer analyzer = new BalancerAnalyzer();

    analyzer.init(high, low);
    Assert.assertEquals(100.0, analyzer.getLookaheadPercentDiff(2L, 6L));
  }

  @Test
  public void testFindSegmentsToMove()
  {
    EasyMock.expect(high.getSizeUsed()).andReturn(6L);
    EasyMock.expect(high.getPercentUsed()).andReturn(60.0);
    EasyMock.expect(high.getMaxSize()).andReturn(10L);
    EasyMock.replay(high);

    EasyMock.expect(low.getSizeUsed()).andReturn(2L);
    EasyMock.expect(low.getPercentUsed()).andReturn(20.0);
    EasyMock.expect(low.getMaxSize()).andReturn(10L);
    EasyMock.replay(low);

    EasyMock.expect(segment.getSize()).andReturn(1L).atLeastOnce();
    EasyMock.replay(segment);

    EasyMock.expect(dataSource.getSegments()).andReturn(Sets.newHashSet(segment));
    EasyMock.replay(dataSource);

    EasyMock.expect(server.getDataSources()).andReturn(Lists.newArrayList(dataSource));
    EasyMock.replay(server);

    BalancerAnalyzer analyzer = new BalancerAnalyzer();

    analyzer.init(high, low);

    Assert.assertEquals(analyzer.findSegmentsToMove(server).iterator().next().getSegment(), segment);

    EasyMock.verify(server);
    EasyMock.verify(dataSource);
    EasyMock.verify(segment);
  }

}
