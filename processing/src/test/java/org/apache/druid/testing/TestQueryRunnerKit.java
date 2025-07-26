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

package org.apache.druid.testing;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TestQueryRunner;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.planning.ExecutionVertex;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;

import java.util.Map;
import java.util.Optional;

/**
 * Factory for creating {@link TestQueryRunner} instances for different query types.
 * This is used in tests to create query runners with specific configurations.
 */
public class TestQueryRunnerKit
{
  public static TestQueryRunnerKit DEFAULT = new TestQueryRunnerKit();
  private final Map<Class<? extends Query>, QueryRunnerFactory> queryRunnerFactories;

  TestQueryRunnerKit()
  {
    this.queryRunnerFactories = Map.of(
        TimeseriesQuery.class,
        new TimeseriesQueryRunnerFactory(
            new TimeseriesQueryQueryToolChest(),
            new TimeseriesQueryEngine(),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        GroupByQuery.class,
        GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig()),
        TopNQuery.class,
        new TopNQueryRunnerFactory(
            TestQueryRunners.createDefaultNonBlockingPool(),
            new TopNQueryQueryToolChest(new TopNQueryConfig()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
  }

  public <T> Sequence<T> run(ReferenceCountedSegmentProvider segmentProvider, Query<T> query, String runnerName)
      throws ISE
  {
    return makeQueryRunner(segmentProvider, query, runnerName).run(QueryPlus.wrap(query));
  }

  private <T> TestQueryRunner<T> makeQueryRunner(
      ReferenceCountedSegmentProvider segmentProvider,
      Query<T> query,
      String runnerName
  ) throws ISE
  {
    if (!queryRunnerFactories.containsKey(query.getClass())) {
      throw new ISE("Unsupported query type: %s", query.getClass().getName());
    }
    final Optional<Segment> segmentReference = ExecutionVertex.of(query)
                                                              .createSegmentMapFunction(NoopPolicyEnforcer.instance())
                                                              .apply(segmentProvider);
    return QueryRunnerTestHelper.makeQueryRunner(
        queryRunnerFactories.get(query.getClass()),
        segmentReference.orElseThrow(),
        runnerName
    );
  }
}
