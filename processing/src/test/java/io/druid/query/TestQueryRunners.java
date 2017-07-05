/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.StupidPool;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchQueryRunnerFactory;
import io.druid.query.search.SearchStrategySelector;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.timeboundary.TimeBoundaryQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.segment.Segment;

import java.nio.ByteBuffer;

/**
 */
public class TestQueryRunners
{
  public static final NonBlockingPool<ByteBuffer> pool = new StupidPool<ByteBuffer>(
      "TestQueryRunners-bufferPool",
      new Supplier<ByteBuffer>()
      {
        @Override
        public ByteBuffer get()
        {
          return ByteBuffer.allocate(1024 * 1024 * 10);
        }
      }
  );
  public static final TopNQueryConfig topNConfig = new TopNQueryConfig();

  public static NonBlockingPool<ByteBuffer> getPool()
  {
    return pool;
  }

  public static <T> QueryRunner<T> makeTopNQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TopNQueryRunnerFactory(
        pool,
        new TopNQueryQueryToolChest(
            topNConfig,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }

  public static <T> QueryRunner<T> makeTimeSeriesQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()),
        new TimeseriesQueryEngine(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );

    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }

  public static <T> QueryRunner<T> makeSearchQueryRunner(
      Segment adapter
  )
  {
    final SearchQueryConfig config = new SearchQueryConfig();
    QueryRunnerFactory factory = new SearchQueryRunnerFactory(
        new SearchStrategySelector(Suppliers.ofInstance(config)),
        new SearchQueryQueryToolChest(
            config,
            QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }

  public static <T> QueryRunner<T> makeTimeBoundaryQueryRunner(
      Segment adapter
  )
  {
    QueryRunnerFactory factory = new TimeBoundaryQueryRunnerFactory(QueryRunnerTestHelper.NOOP_QUERYWATCHER);
    return new FinalizeResultsQueryRunner<T>(
        factory.createRunner(adapter),
        factory.getToolchest()
    );
  }
}
