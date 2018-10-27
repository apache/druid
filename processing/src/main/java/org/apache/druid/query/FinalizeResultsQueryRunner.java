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

package org.apache.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.Map;

/**
 */
public class FinalizeResultsQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public FinalizeResultsQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, Map<String, Object> responseContext)
  {
    final Query<T> query = queryPlus.getQuery();
    final boolean shouldFinalize = QueryContexts.isFinalize(query, true);
    if (!shouldFinalize) {
      return baseRunner.run(queryPlus.withQuery(query), responseContext);
    }

    final boolean isBySegment = QueryContexts.isBySegment(query);

    Function<T, T> finalizerFn = toolChest.makePostComputeManipulatorFn(
        query,
        AggregatorFactory::finalizeComputation
    );

    final Query<T> queryToRun = query.withOverriddenContext(ImmutableMap.of("finalize", false));
    if (isBySegment) {
      Function<T, T> perSegmentResultFinalizerFn = finalizerFn;
      finalizerFn = (T input) -> {
        @SuppressWarnings("unchecked")
        Result<BySegmentResultValue<T>> result = (Result<BySegmentResultValue<T>>) input;
        return (T) result.map(
            (BySegmentResultValue<T> bySegmentResults) -> new BySegmentResultValueImpl(
                Lists.transform(bySegmentResults.getResults(), perSegmentResultFinalizerFn),
                bySegmentResults.getSegmentId(),
                bySegmentResults.getInterval()
            )
        );
      };
    }
    return baseRunner.run(queryPlus.withQuery(queryToRun), responseContext).map(finalizerFn::apply);
  }
}
