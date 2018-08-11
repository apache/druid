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
package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.io.Closer;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.search.SearchQuery;
import io.druid.query.search.SearchQueryConfig;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;

public final class CachingClusteredClientTestUtils
{
  /**
   * Returns a new {@link QueryToolChestWarehouse} for unit tests and a resourceCloser which should be closed at the end
   * of the test.
   */
  public static Pair<QueryToolChestWarehouse, Closer> createWarehouse(
      ObjectMapper objectMapper,
      Supplier<SelectQueryConfig> selectConfigSupplier
  )
  {
    final Pair<GroupByQueryRunnerFactory, Closer> factoryCloserPair = GroupByQueryRunnerTest.makeQueryRunnerFactory(
        new GroupByQueryConfig()
    );
    final GroupByQueryRunnerFactory factory = factoryCloserPair.lhs;
    final Closer resourceCloser = factoryCloserPair.rhs;
    return Pair.of(
        new MapQueryToolChestWarehouse(
            ImmutableMap.<Class<? extends Query>, QueryToolChest>builder()
                .put(
                    TimeseriesQuery.class,
                    new TimeseriesQueryQueryToolChest(
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    )
                )
                .put(
                    TopNQuery.class,
                    new TopNQueryQueryToolChest(
                        new TopNQueryConfig(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    )
                )
                .put(
                    SearchQuery.class,
                    new SearchQueryQueryToolChest(
                        new SearchQueryConfig(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    )
                )
                .put(
                    SelectQuery.class,
                    new SelectQueryQueryToolChest(
                        objectMapper,
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
                        selectConfigSupplier
                    )
                )
                .put(
                    GroupByQuery.class,
                    factory.getToolchest()
                )
                .put(TimeBoundaryQuery.class, new TimeBoundaryQueryQueryToolChest())
                .build()
        ),
        resourceCloser
    );
  }

  public static ObjectMapper createObjectMapper()
  {
    final SmileFactory factory = new SmileFactory();
    final ObjectMapper objectMapper = new DefaultObjectMapper(factory);
    factory.setCodec(objectMapper);
    return objectMapper;
  }

  private CachingClusteredClientTestUtils() {}
}
