/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.client;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Bytes;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.Set;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;

@RunWith(EasyMockRunner.class)
public class CachingClusteredClientCacheKeyManagerTest extends EasyMockSupport
{
  @Mock
  private CacheStrategy<Object, Object, Query<Object>> strategy;
  @Mock
  private Query<Object> query;
  @Mock
  private JoinableFactoryWrapper joinableFactoryWrapper;
  @Mock
  private DataSourceAnalysis dataSourceAnalysis;

  private static final byte[] QUERY_CACHE_KEY = new byte[]{1, 2, 3};
  private static final byte[] JOIN_KEY = new byte[]{4, 5};

  @Before
  public void setup()
  {
    expect(strategy.computeCacheKey(query)).andReturn(QUERY_CACHE_KEY).anyTimes();
    expect(query.getContextValue("bySegment")).andReturn(false).anyTimes();
  }

  @After
  public void teardown()
  {
    verifyAllUnexpectedCalls();
  }

  @Test
  public void testComputeEtag_nonHistorical()
  {
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(0),
        makeRealtimeServerSelector(1)
    );
    String actual = keyManager.computeResultLevelCachingEtag(selectors, QUERY_CACHE_KEY);
    Assert.assertNull(actual);
  }

  @Test
  public void testComputeEtag_DifferentHistoricals()
  {
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual1 = keyManager.computeResultLevelCachingEtag(selectors, QUERY_CACHE_KEY);
    Assert.assertNotNull(actual1);

    selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual2 = keyManager.computeResultLevelCachingEtag(selectors, QUERY_CACHE_KEY);
    Assert.assertNotNull(actual2);
    Assert.assertEquals("cache key should not change for same server selectors", actual1, actual2);

    selectors = ImmutableSet.of(
        makeHistoricalServerSelector(2),
        makeHistoricalServerSelector(1)
    );
    String actual3 = keyManager.computeResultLevelCachingEtag(selectors, QUERY_CACHE_KEY);
    Assert.assertNotNull(actual3);
    Assert.assertNotEquals(actual1, actual3);
  }

  @Test
  public void testComputeEtag_DifferentQueryCacheKey()
  {
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual1 = keyManager.computeResultLevelCachingEtag(selectors, new byte[]{1, 2});
    Assert.assertNotNull(actual1);

    String actual2 = keyManager.computeResultLevelCachingEtag(selectors, new byte[]{3, 4});
    Assert.assertNotNull(actual2);
    Assert.assertNotEquals(actual1, actual2);
  }

  @Test
  public void testComputeEtag_nonJoinDataSource()
  {
    expect(dataSourceAnalysis.isJoin()).andReturn(false);
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual1 = keyManager.computeResultLevelCachingEtag(selectors, QUERY_CACHE_KEY);
    Assert.assertNotNull(actual1);

    selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual2 = keyManager.computeResultLevelCachingEtag(selectors, null);
    Assert.assertNotNull(actual2);
    Assert.assertEquals(actual1, actual2);
  }

  @Test
  public void testComputeEtag_joinWithUnsupportedCaching()
  {
    expect(dataSourceAnalysis.isJoin()).andReturn(true);
    expect(joinableFactoryWrapper.computeJoinDataSourceCacheKey(dataSourceAnalysis)).andReturn(Optional.empty());
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual = keyManager.computeResultLevelCachingEtag(selectors, null);
    Assert.assertNull(actual);
  }

  @Test
  public void testComputeEtag_joinWithSupportedCaching()
  {
    expect(dataSourceAnalysis.isJoin()).andReturn(true).anyTimes();
    expect(joinableFactoryWrapper.computeJoinDataSourceCacheKey(dataSourceAnalysis)).andReturn(Optional.of(JOIN_KEY));
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual1 = keyManager.computeResultLevelCachingEtag(selectors, null);
    Assert.assertNotNull(actual1);

    reset(joinableFactoryWrapper);
    expect(joinableFactoryWrapper.computeJoinDataSourceCacheKey(dataSourceAnalysis)).andReturn(Optional.of(new byte[]{9}));
    replay(joinableFactoryWrapper);
    selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual2 = keyManager.computeResultLevelCachingEtag(selectors, null);
    Assert.assertNotNull(actual2);
    Assert.assertNotEquals(actual1, actual2);
  }

  @Test
  public void testComputeEtag_noEffectifBySegment()
  {
    expect(dataSourceAnalysis.isJoin()).andReturn(false);
    reset(query);
    expect(query.getContextValue("bySegment")).andReturn(true).anyTimes();
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual = keyManager.computeResultLevelCachingEtag(selectors, null);
    Assert.assertNotNull(actual);
  }

  @Test
  public void testComputeEtag_noEffectIfUseAndPopulateFalse()
  {
    expect(dataSourceAnalysis.isJoin()).andReturn(false);
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = new CachingClusteredClient.CacheKeyManager<>(
        query,
        strategy,
        false,
        false,
        dataSourceAnalysis,
        joinableFactoryWrapper
    );
    Set<SegmentServerSelector> selectors = ImmutableSet.of(
        makeHistoricalServerSelector(1),
        makeHistoricalServerSelector(1)
    );
    String actual = keyManager.computeResultLevelCachingEtag(selectors, null);
    Assert.assertNotNull(actual);
  }

  @Test
  public void testSegmentQueryCacheKey_nonJoinDataSource()
  {
    expect(dataSourceAnalysis.isJoin()).andReturn(false);
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    byte[] cacheKey = keyManager.computeSegmentLevelQueryCacheKey();
    Assert.assertArrayEquals(QUERY_CACHE_KEY, cacheKey);
  }

  @Test
  public void testSegmentQueryCacheKey_joinWithUnsupportedCaching()
  {
    expect(dataSourceAnalysis.isJoin()).andReturn(true);
    expect(joinableFactoryWrapper.computeJoinDataSourceCacheKey(dataSourceAnalysis)).andReturn(Optional.empty());
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    byte[] cacheKey = keyManager.computeSegmentLevelQueryCacheKey();
    Assert.assertNull(cacheKey);
  }

  @Test
  public void testSegmentQueryCacheKey_joinWithSupportedCaching()
  {

    expect(dataSourceAnalysis.isJoin()).andReturn(true);
    expect(joinableFactoryWrapper.computeJoinDataSourceCacheKey(dataSourceAnalysis)).andReturn(Optional.of(JOIN_KEY));
    replayAll();
    CachingClusteredClient.CacheKeyManager<Object> keyManager = makeKeyManager();
    byte[] cacheKey = keyManager.computeSegmentLevelQueryCacheKey();
    Assert.assertArrayEquals(Bytes.concat(JOIN_KEY, QUERY_CACHE_KEY), cacheKey);
  }

  @Test
  public void testSegmentQueryCacheKey_noCachingIfBySegment()
  {
    reset(query);
    expect(query.getContextValue("bySegment")).andReturn(true).anyTimes();
    replayAll();
    byte[] cacheKey = makeKeyManager().computeSegmentLevelQueryCacheKey();
    Assert.assertNull(cacheKey);
  }

  @Test
  public void testSegmentQueryCacheKey_useAndPopulateCacheFalse()
  {
    replayAll();
    Assert.assertNull(new CachingClusteredClient.CacheKeyManager<>(
        query,
        strategy,
        false,
        false,
        dataSourceAnalysis,
        joinableFactoryWrapper
    ).computeSegmentLevelQueryCacheKey());
  }

  private CachingClusteredClient.CacheKeyManager<Object> makeKeyManager()
  {
    return new CachingClusteredClient.CacheKeyManager<>(
        query,
        strategy,
        true,
        true,
        dataSourceAnalysis,
        joinableFactoryWrapper
    );
  }

  private SegmentServerSelector makeHistoricalServerSelector(int partitionNumber)
  {
    return makeServerSelector(true, partitionNumber);
  }

  private SegmentServerSelector makeRealtimeServerSelector(int partitionNumber)
  {
    return makeServerSelector(false, partitionNumber);
  }

  /**
   * using partitionNumber, its possible to create segments with different ids
   */
  private SegmentServerSelector makeServerSelector(boolean isHistorical, int partitionNumber)
  {
    ServerSelector serverSelector = mock(ServerSelector.class);
    QueryableDruidServer queryableDruidServer = mock(QueryableDruidServer.class);
    DruidServer server = mock(DruidServer.class);
    SegmentId segmentId = SegmentId.dummy("data-source", partitionNumber);
    DataSegment segment = new DataSegment(
        segmentId,
        null,
        null,
        null,
        new NumberedShardSpec(partitionNumber, 10),
        null,
        0,
        0
    );
    expect(server.isSegmentReplicationTarget()).andReturn(isHistorical).anyTimes();
    expect(serverSelector.pick(query)).andReturn(queryableDruidServer).anyTimes();
    expect(queryableDruidServer.getServer()).andReturn(server).anyTimes();
    expect(serverSelector.getSegment()).andReturn(segment).anyTimes();
    replay(serverSelector, queryableDruidServer, server);
    return new SegmentServerSelector(serverSelector, segmentId.toDescriptor());
  }
}
