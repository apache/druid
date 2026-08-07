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

package org.apache.druid.sql.calcite.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TestBufferPool;
import org.apache.druid.query.groupby.DefaultGroupByQueryMetricsFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByResourcesReservationPool;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.operator.WindowOperatorQueryQueryRunnerFactory;
import org.apache.druid.query.operator.WindowOperatorQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchQueryConfig;
import org.apache.druid.query.search.SearchQueryQueryToolChest;
import org.apache.druid.query.search.SearchQueryRunnerFactory;
import org.apache.druid.query.search.SearchStrategySelector;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.query.union.UnionQuery;
import org.apache.druid.query.union.UnionQueryLogic;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;

public class SqlTestQueryStack
{
  private static final int COMPUTE_BUFFER_SIZE = 10 * 1024 * 1024;

  private SqlTestQueryStack()
  {
  }

  public static DruidProcessingConfig getProcessingConfig(final int mergeBuffers)
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
        return 1;
      }

      @Override
      public int getNumMergeBuffers()
      {
        return mergeBuffers < 0 ? 2 : mergeBuffers;
      }
    };
  }

  public static TestBufferPool makeTestBufferPool(final Closer closer)
  {
    final TestBufferPool testBufferPool = TestBufferPool.offHeap(COMPUTE_BUFFER_SIZE, Integer.MAX_VALUE);
    closer.register(() -> Assertions.assertEquals(0, testBufferPool.getOutstandingObjectCount()));
    return testBufferPool;
  }

  public static TestGroupByBuffers makeGroupByBuffers(
      final Closer closer,
      final DruidProcessingConfig processingConfig
  )
  {
    return closer.register(TestGroupByBuffers.createFromProcessingConfig(processingConfig));
  }

  public static QueryRunnerFactoryConglomerate createQueryRunnerFactoryConglomerate(final Closer closer)
  {
    final DruidProcessingConfig processingConfig = getProcessingConfig(-1);
    final TestBufferPool testBufferPool = makeTestBufferPool(closer);
    final TestGroupByBuffers groupByBuffers = makeGroupByBuffers(closer, processingConfig);
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    final ImmutableMap<Class<? extends Query>, QueryRunnerFactory> factories = ImmutableMap
        .<Class<? extends Query>, QueryRunnerFactory>builder()
        .put(
            SegmentMetadataQuery.class,
            new SegmentMetadataQueryRunnerFactory(
                new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig("P1W")),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(
            SearchQuery.class,
            new SearchQueryRunnerFactory(
                new SearchStrategySelector(Suppliers.ofInstance(new SearchQueryConfig())),
                new SearchQueryQueryToolChest(new SearchQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(
            ScanQuery.class,
            new ScanQueryRunnerFactory(
                new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
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
                testBufferPool,
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
        .put(GroupByQuery.class, makeGroupByQueryRunnerFactory(jsonMapper, groupByBuffers, processingConfig))
        .put(TimeBoundaryQuery.class, new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER))
        .put(
            WindowOperatorQuery.class,
            new WindowOperatorQueryQueryRunnerFactory(
                new WindowOperatorQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance())
            )
        )
        .build();
    final UnionQueryLogic unionQueryLogic = new UnionQueryLogic();
    final QueryRunnerFactoryConglomerate conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        factories,
        Maps.transformValues(factories, QueryRunnerFactory::getToolchest),
        ImmutableMap.of(UnionQuery.class, unionQueryLogic)
    );
    unionQueryLogic.initialize(conglomerate);
    return conglomerate;
  }

  private static GroupByQueryRunnerFactory makeGroupByQueryRunnerFactory(
      final ObjectMapper mapper,
      final TestGroupByBuffers bufferPools,
      final DruidProcessingConfig processingConfig
  )
  {
    if (bufferPools.getBufferSize() != processingConfig.intermediateComputeSizeBytes()) {
      throw new ISE("Provided buffer size [%,d] does not match configured size [%,d]",
                    bufferPools.getBufferSize(), processingConfig.intermediateComputeSizeBytes());
    }
    if (bufferPools.getNumMergeBuffers() != processingConfig.getNumMergeBuffers()) {
      throw new ISE("Provided merge buffer count [%,d] does not match configured count [%,d]",
                    bufferPools.getNumMergeBuffers(), processingConfig.getNumMergeBuffers());
    }
    final GroupByQueryConfig config = new GroupByQueryConfig();
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupByStatsProvider statsProvider = new GroupByStatsProvider();
    final GroupByResourcesReservationPool reservationPool =
        new GroupByResourcesReservationPool(bufferPools.getMergePool(), config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        processingConfig,
        configSupplier,
        reservationPool,
        mapper,
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        statsProvider
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        groupingEngine,
        configSupplier,
        DefaultGroupByQueryMetricsFactory.instance(),
        reservationPool,
        statsProvider
    );
    return new GroupByQueryRunnerFactory(groupingEngine, toolChest, bufferPools.getProcessingPool());
  }
}
