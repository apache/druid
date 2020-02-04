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
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchQueryConfig;
import org.apache.druid.query.search.SearchQueryQueryToolChest;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;

public final class CachingClusteredClientTestUtils
{
  /**
   * Returns a new {@link QueryToolChestWarehouse} for unit tests and a resourceCloser which should be closed at the end
   * of the test.
   */
  public static Pair<QueryToolChestWarehouse, Closer> createWarehouse(
      ObjectMapper objectMapper
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
                    new TimeseriesQueryQueryToolChest()
                )
                .put(
                    TopNQuery.class,
                    new TopNQueryQueryToolChest(new TopNQueryConfig())
                )
                .put(
                    SearchQuery.class,
                    new SearchQueryQueryToolChest(new SearchQueryConfig())
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

  private CachingClusteredClientTestUtils()
  {
  }
}
