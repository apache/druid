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

package org.apache.druid.server;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.LookupJoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactoryTest;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.scheduling.ManualQueryPrioritizationStrategy;
import org.apache.druid.server.scheduling.NoQueryLaningStrategy;
import org.apache.druid.timeline.VersionedIntervalTimeline;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Utilities for creating query-stack objects for tests.
 */
public class QueryStackTests
{
  public static final QueryScheduler DEFAULT_NOOP_SCHEDULER = new QueryScheduler(
      0,
      ManualQueryPrioritizationStrategy.INSTANCE,
      NoQueryLaningStrategy.INSTANCE,
      new ServerConfig()
  );
  private static final ServiceEmitter EMITTER = new NoopServiceEmitter();
  private static final int COMPUTE_BUFFER_SIZE = 10 * 1024 * 1024;

  private QueryStackTests()
  {
    // No instantiation.
  }

  public static ClientQuerySegmentWalker createClientQuerySegmentWalker(
      final QuerySegmentWalker clusterWalker,
      final QuerySegmentWalker localWalker,
      final QueryRunnerFactoryConglomerate conglomerate,
      final JoinableFactory joinableFactory,
      final ServerConfig serverConfig
  )
  {
    return new ClientQuerySegmentWalker(
        EMITTER,
        clusterWalker,
        localWalker,
        new QueryToolChestWarehouse()
        {
          @Override
          public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
          {
            return conglomerate.findFactory(query).getToolchest();
          }
        },
        joinableFactory,
        new RetryQueryRunnerConfig(),
        TestHelper.makeJsonMapper(),
        serverConfig,
        null /* Cache */,
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return false;
          }

          @Override
          public boolean isUseCache()
          {
            return false;
          }

          @Override
          public boolean isPopulateResultLevelCache()
          {
            return false;
          }

          @Override
          public boolean isUseResultLevelCache()
          {
            return false;
          }
        }
    );
  }

  public static TestClusterQuerySegmentWalker createClusterQuerySegmentWalker(
      Map<String, VersionedIntervalTimeline<String, ReferenceCountingSegment>> timelines,
      JoinableFactory joinableFactory,
      QueryRunnerFactoryConglomerate conglomerate,
      @Nullable QueryScheduler scheduler
  )
  {
    return new TestClusterQuerySegmentWalker(timelines, joinableFactory, conglomerate, scheduler);
  }

  public static LocalQuerySegmentWalker createLocalQuerySegmentWalker(
      final QueryRunnerFactoryConglomerate conglomerate,
      final SegmentWrangler segmentWrangler,
      final JoinableFactory joinableFactory,
      final QueryScheduler scheduler
  )
  {
    return new LocalQuerySegmentWalker(
        conglomerate,
        segmentWrangler,
        joinableFactory,
        scheduler,
        EMITTER
    );
  }

  public static DruidProcessingConfig getProcessingConfig(boolean useParallelMergePoolConfigured)
  {
    return new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return null;
      }

      @Override
      public int intermediateComputeSizeBytes()
      {
        return COMPUTE_BUFFER_SIZE;
      }

      @Override
      public int getNumThreads()
      {
        // Only use 1 thread for tests.
        return 1;
      }

      @Override
      public int getNumMergeBuffers()
      {
        // Need 3 buffers for CalciteQueryTest.testDoubleNestedGroupby.
        // Two buffers for the broker and one for the queryable.
        return 3;
      }

      @Override
      public boolean useParallelMergePoolConfigured()
      {
        return useParallelMergePoolConfigured;
      }
    };
  }

  /**
   * Returns a new {@link QueryRunnerFactoryConglomerate}. Adds relevant closeables to the passed-in {@link Closer}.
   */
  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(final Closer closer)
  {
    return createQueryRunnerFactoryConglomerate(closer, true);
  }

  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(
      final Closer closer,
      final boolean useParallelMergePoolConfigured
  )
  {
    final CloseableStupidPool<ByteBuffer> stupidPool = new CloseableStupidPool<>(
        "TopNQueryRunnerFactory-bufferPool",
        () -> ByteBuffer.allocate(COMPUTE_BUFFER_SIZE)
    );

    closer.register(stupidPool);

    final Pair<GroupByQueryRunnerFactory, Closer> factoryCloserPair =
        GroupByQueryRunnerTest.makeQueryRunnerFactory(
            GroupByQueryRunnerTest.DEFAULT_MAPPER,
            new GroupByQueryConfig()
            {
              @Override
              public String getDefaultStrategy()
              {
                return GroupByStrategySelector.STRATEGY_V2;
              }
            },
            getProcessingConfig(useParallelMergePoolConfigured)
        );

    final GroupByQueryRunnerFactory groupByQueryRunnerFactory = factoryCloserPair.lhs;
    closer.register(factoryCloserPair.rhs);

    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
            .put(
                SegmentMetadataQuery.class,
                new SegmentMetadataQueryRunnerFactory(
                    new SegmentMetadataQueryQueryToolChest(
                        new SegmentMetadataQueryConfig("P1W")
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                ScanQuery.class,
                new ScanQueryRunnerFactory(
                    new ScanQueryQueryToolChest(
                        new ScanQueryConfig(),
                        new DefaultGenericQueryMetricsFactory()
                    ),
                    new ScanQueryEngine(),
                    new ScanQueryConfig()
                )
            )
            .put(
                TimeseriesQuery.class,
                new TimeseriesQueryRunnerFactory(
                    new TimeseriesQueryQueryToolChest(),
                    new TimeseriesQueryEngine(),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(
                TopNQuery.class,
                new TopNQueryRunnerFactory(
                    stupidPool,
                    new TopNQueryQueryToolChest(new TopNQueryConfig()),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
            .put(GroupByQuery.class, groupByQueryRunnerFactory)
            .build()
    );

    return conglomerate;
  }

  public static JoinableFactory makeJoinableFactoryForLookup(
      LookupExtractorFactoryContainerProvider lookupProvider
  )
  {
    return makeJoinableFactoryFromDefault(lookupProvider, null);
  }

  public static JoinableFactory makeJoinableFactoryFromDefault(
      @Nullable LookupExtractorFactoryContainerProvider lookupProvider,
      @Nullable Map<Class<? extends DataSource>, JoinableFactory> custom
  )
  {
    ImmutableMap.Builder<Class<? extends DataSource>, JoinableFactory> builder = ImmutableMap.builder();
    builder.put(InlineDataSource.class, new InlineJoinableFactory());
    if (lookupProvider != null) {
      builder.put(LookupDataSource.class, new LookupJoinableFactory(lookupProvider));
    }
    if (custom != null) {
      builder.putAll(custom);
    }
    return MapJoinableFactoryTest.fromMap(builder.build());
  }
}
