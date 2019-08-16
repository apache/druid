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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.select.SelectQueryConfig;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.SingleElementPartitionChunk;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 */
public class CachingClusteredClientFunctionalityTest
{
  private static final ObjectMapper OBJECT_MAPPER = CachingClusteredClientTestUtils.createObjectMapper();
  private static final Supplier<SelectQueryConfig> SELECT_CONFIG_SUPPLIER = Suppliers.ofInstance(
      new SelectQueryConfig(true)
  );
  private static final Pair<QueryToolChestWarehouse, Closer> WAREHOUSE_AND_CLOSER = CachingClusteredClientTestUtils
      .createWarehouse(OBJECT_MAPPER, SELECT_CONFIG_SUPPLIER);
  private static final QueryToolChestWarehouse WAREHOUSE = WAREHOUSE_AND_CLOSER.lhs;
  private static final Closer RESOURCE_CLOSER = WAREHOUSE_AND_CLOSER.rhs;

  private CachingClusteredClient client;
  private VersionedIntervalTimeline<String, ServerSelector> timeline;
  private TimelineServerView serverView;
  private Cache cache;

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    RESOURCE_CLOSER.close();
  }

  @Before
  public void setUp()
  {
    timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    serverView = EasyMock.createNiceMock(TimelineServerView.class);
    cache = MapCache.create(100000);
    client = makeClient(
        new ForegroundCachePopulator(OBJECT_MAPPER, new CachePopulatorStats(), -1)
    );
  }

  @Test
  public void testUncoveredInterval()
  {
    addToTimeline(Intervals.of("2015-01-02/2015-01-03"), "1");
    addToTimeline(Intervals.of("2015-01-04/2015-01-05"), "1");
    addToTimeline(Intervals.of("2015-02-04/2015-02-05"), "1");

    final Druids.TimeseriesQueryBuilder builder = Druids.newTimeseriesQueryBuilder()
                                                        .dataSource("test")
                                                        .intervals("2015-01-02/2015-01-03")
                                                        .granularity("day")
                                                        .aggregators(Collections.singletonList(
                                                            new CountAggregatorFactory(
                                                                "rows")))
                                                        .context(ImmutableMap.of(
                                                            "uncoveredIntervalsLimit",
                                                            3
                                                        ));

    ResponseContext responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    Assert.assertNull(responseContext.get(ResponseContext.Key.UNCOVERED_INTERVALS));

    builder.intervals("2015-01-01/2015-01-03");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-01/2015-01-02");

    builder.intervals("2015-01-01/2015-01-04");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-01/2015-01-02", "2015-01-03/2015-01-04");

    builder.intervals("2015-01-02/2015-01-04");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-03/2015-01-04");

    builder.intervals("2015-01-01/2015-01-30");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-01/2015-01-02", "2015-01-03/2015-01-04", "2015-01-05/2015-01-30");

    builder.intervals("2015-01-02/2015-01-30");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-03/2015-01-04", "2015-01-05/2015-01-30");

    builder.intervals("2015-01-04/2015-01-30");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-05/2015-01-30");

    builder.intervals("2015-01-10/2015-01-30");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, false, "2015-01-10/2015-01-30");

    builder.intervals("2015-01-01/2015-02-25");
    responseContext = ResponseContext.createEmpty();
    runQuery(client, builder.build(), responseContext);
    assertUncovered(responseContext, true, "2015-01-01/2015-01-02", "2015-01-03/2015-01-04", "2015-01-05/2015-02-04");
  }

  private void assertUncovered(ResponseContext context, boolean uncoveredIntervalsOverflowed, String... intervals)
  {
    List<Interval> expectedList = Lists.newArrayListWithExpectedSize(intervals.length);
    for (String interval : intervals) {
      expectedList.add(Intervals.of(interval));
    }
    Assert.assertEquals((Object) expectedList, context.get(ResponseContext.Key.UNCOVERED_INTERVALS));
    Assert.assertEquals(uncoveredIntervalsOverflowed, context.get(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED));
  }

  private void addToTimeline(Interval interval, String version)
  {
    timeline.add(interval, version, new SingleElementPartitionChunk<>(
        new ServerSelector(
            DataSegment.builder()
                       .dataSource("test")
                       .interval(interval)
                       .version(version)
                       .shardSpec(NoneShardSpec.instance())
                       .build(),
            new TierSelectorStrategy()
            {
              @Override
              public Comparator<Integer> getComparator()
              {
                return Ordering.natural();
              }

              @Override
              public QueryableDruidServer pick(
                  Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
                  DataSegment segment
              )
              {
                return new QueryableDruidServer(
                    new DruidServer("localhost", "localhost", null, 100, ServerType.HISTORICAL, "a", 10),
                    EasyMock.createNiceMock(DirectDruidClient.class)
                );
              }

              @Override
              public List<QueryableDruidServer> pick(
                  Int2ObjectRBTreeMap<Set<QueryableDruidServer>> prioritizedServers,
                  DataSegment segment,
                  int numServersToPick
              )
              {
                return Collections.singletonList(
                    new QueryableDruidServer(
                        new DruidServer("localhost", "localhost", null, 100, ServerType.HISTORICAL, "a", 10),
                        EasyMock.createNiceMock(DirectDruidClient.class)
                    )
                );
              }
            }
        )
    ));
  }

  protected CachingClusteredClient makeClient(final CachePopulator cachePopulator)
  {
    return makeClient(cachePopulator, cache, 10);
  }

  protected CachingClusteredClient makeClient(
      final CachePopulator cachePopulator,
      final Cache cache,
      final int mergeLimit
  )
  {
    return new CachingClusteredClient(
        WAREHOUSE,
        new TimelineServerView()
        {
          @Override
          public void registerSegmentCallback(Executor exec, SegmentCallback callback)
          {
          }

          @Override
          public VersionedIntervalTimeline<String, ServerSelector> getTimeline(DataSource dataSource)
          {
            return timeline;
          }

          @Nullable
          @Override
          public List<ImmutableDruidServer> getDruidServers()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void registerTimelineCallback(final Executor exec, final TimelineCallback callback)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public <T> QueryRunner<T> getQueryRunner(DruidServer server)
          {
            return serverView.getQueryRunner(server);
          }

          @Override
          public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
          {

          }
        },
        cache,
        OBJECT_MAPPER,
        cachePopulator,
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return true;
          }

          @Override
          public boolean isUseCache()
          {
            return true;
          }

          @Override
          public boolean isQueryCacheable(Query query)
          {
            return true;
          }

          @Override
          public int getCacheBulkMergeLimit()
          {
            return mergeLimit;
          }
        },
        new DruidHttpClientConfig() {
          @Override
          public long getMaxQueuedBytes()
          {
            return 0L;
          }
        }
    );
  }

  private static <T> Sequence<T> runQuery(
      CachingClusteredClient client,
      final Query<T> query,
      final ResponseContext responseContext
  )
  {
    return client.getQueryRunnerForIntervals(query, query.getIntervals()).run(
        QueryPlus.wrap(query),
        responseContext
    );
  }
}
