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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.TestSequence;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.join.JoinableFactoryWrapperTest;
import org.apache.druid.server.QueryScheduler;
import org.apache.druid.server.coordination.ServerManagerTest;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;

import static org.mockito.ArgumentMatchers.any;

/**
 * Performance tests for {@link CachingClusteredClient}, that do not require a real cluster, can be added here.
 * There is one test for a scenario where a single interval has large number of segments.
 */
public class CachingClusteredClientPerfTest
{

  @Test(timeout = 10_000)
  public void testGetQueryRunnerForSegments_singleIntervalLargeSegments()
  {
    final int segmentCount = 30_000;
    final Interval interval = Intervals.of("2021-02-13/2021-02-14");
    final List<SegmentDescriptor> segmentDescriptors = new ArrayList<>(segmentCount);
    final List<DataSegment> dataSegments = new ArrayList<>(segmentCount);
    final VersionedIntervalTimeline<String, ServerSelector> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    final DruidServer server = new DruidServer(
        "server",
        "localhost:9000",
        null,
        Long.MAX_VALUE,
        ServerType.HISTORICAL,
        DruidServer.DEFAULT_TIER,
        DruidServer.DEFAULT_PRIORITY
    );

    for (int ii = 0; ii < segmentCount; ii++) {
      segmentDescriptors.add(new SegmentDescriptor(interval, "1", ii));
      DataSegment segment = makeDataSegment("test", interval, "1", ii);
      dataSegments.add(segment);
    }
    timeline.addAll(
        Iterators.transform(dataSegments.iterator(), segment -> {
          ServerSelector ss = new ServerSelector(
              segment,
              new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy())
          );
          ss.addServerAndUpdateSegment(new QueryableDruidServer(
              server,
              new MockQueryRunner()
          ), segment);
          return new VersionedIntervalTimeline.PartitionChunkEntry<>(
              segment.getInterval(),
              segment.getVersion(),
              segment.getShardSpec().createChunk(ss)
          );
        })
    );

    TimelineServerView serverView = Mockito.mock(TimelineServerView.class);
    QueryScheduler queryScheduler = Mockito.mock(QueryScheduler.class);
    // mock scheduler to return same sequence as argument
    Mockito.when(queryScheduler.run(any(), any())).thenAnswer(i -> i.getArgument(1));
    Mockito.when(queryScheduler.prioritizeAndLaneQuery(any(), any()))
           .thenAnswer(i -> ((QueryPlus) i.getArgument(0)).getQuery());

    Mockito.doReturn(Optional.of(timeline)).when(serverView).getTimeline(any());
    Mockito.doReturn(new MockQueryRunner()).when(serverView).getQueryRunner(any());
    CachingClusteredClient cachingClusteredClient = new CachingClusteredClient(
        new MockQueryToolChestWareHouse(),
        serverView,
        MapCache.create(1024),
        TestHelper.makeJsonMapper(),
        Mockito.mock(CachePopulator.class),
        new CacheConfig(),
        Mockito.mock(DruidHttpClientConfig.class),
        Mockito.mock(DruidProcessingConfig.class),
        ForkJoinPool.commonPool(),
        queryScheduler,
        JoinableFactoryWrapperTest.NOOP_JOINABLE_FACTORY_WRAPPER,
        new NoopServiceEmitter()
    );

    Query<SegmentDescriptor> fakeQuery = makeFakeQuery(interval);
    QueryRunner<SegmentDescriptor> queryRunner = cachingClusteredClient.getQueryRunnerForSegments(
        fakeQuery,
        segmentDescriptors
    );
    Sequence<SegmentDescriptor> sequence = queryRunner.run(QueryPlus.wrap(fakeQuery));
    Assert.assertEquals(segmentDescriptors, sequence.toList());
  }

  private Query<SegmentDescriptor> makeFakeQuery(Interval interval)
  {
    return new TestQuery(
        new TableDataSource("test"),
        new MultipleIntervalSegmentSpec(Collections.singletonList(interval)),
        false,
        ImmutableMap.of(BaseQuery.QUERY_ID, "testQuery")
    );
  }

  private DataSegment makeDataSegment(String dataSource, Interval interval, String version, int partition)
  {
    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(interval)
                      .version(version)
                      .shardSpec(new LinearShardSpec(partition))
                      .size(1)
                      .build();
  }

  private static class MockQueryToolChestWareHouse implements QueryToolChestWarehouse
  {

    @Override
    public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(QueryType query)
    {
      return new ServerManagerTest.NoopQueryToolChest<>();
    }
  }

  private static class MockQueryRunner implements QueryRunner<SegmentDescriptor>
  {

    @Override
    public Sequence<SegmentDescriptor> run(
        QueryPlus<SegmentDescriptor> queryPlus,
        ResponseContext responseContext
    )
    {
      TestQuery query = (TestQuery) queryPlus.getQuery();
      return TestSequence.create(((MultipleSpecificSegmentSpec) query.getSpec()).getDescriptors());
    }
  }

  private static class TestQuery extends BaseQuery<SegmentDescriptor>
  {
    private QuerySegmentSpec spec;

    public TestQuery(
        DataSource dataSource,
        QuerySegmentSpec querySegmentSpec,
        boolean descending,
        Map<String, Object> context
    )
    {
      super(dataSource, querySegmentSpec, descending, context);
    }

    @Override
    public boolean hasFilters()
    {
      return false;
    }

    @Override
    public DimFilter getFilter()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return "string";
    }

    @Override
    public Query<SegmentDescriptor> withOverriddenContext(Map<String, Object> contextOverride)
    {
      return this;
    }

    @Override
    public Query<SegmentDescriptor> withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      this.spec = spec;
      return this;
    }

    @Override
    public Query<SegmentDescriptor> withDataSource(DataSource dataSource)
    {
      return this;
    }

    public QuerySegmentSpec getSpec()
    {
      return spec;
    }
  }

}
