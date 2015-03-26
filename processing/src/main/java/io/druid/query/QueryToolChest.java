/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.timeline.LogicalSegment;

import java.util.List;

/**
 * The broker-side (also used by server in some cases) API for a specific Query type.  This API is still undergoing
 * evolution and is only semi-stable, so proprietary Query implementations should be ready for the potential
 * maintenance burden when upgrading versions.
 */
public abstract class QueryToolChest<ResultType, QueryType extends Query<ResultType>>
{
  public abstract QueryRunner<ResultType> mergeResults(QueryRunner<ResultType> runner);

  /**
   * This method doesn't belong here, but it's here for now just to make it work.
   *
   * @param seqOfSequences sequence of sequences to be merged
   *
   * @return the sequence of merged results
   */
  public abstract Sequence<ResultType> mergeSequences(Sequence<Sequence<ResultType>> seqOfSequences);

  public abstract Sequence<ResultType> mergeSequencesUnordered(Sequence<Sequence<ResultType>> seqOfSequences);


  public abstract ServiceMetricEvent.Builder makeMetricBuilder(QueryType query);

  public abstract Function<ResultType, ResultType> makePreComputeManipulatorFn(
      QueryType query,
      MetricManipulationFn fn
  );

  public Function<ResultType, ResultType> makePostComputeManipulatorFn(QueryType query, MetricManipulationFn fn)
  {
    return makePreComputeManipulatorFn(query, fn);
  }

  public abstract TypeReference<ResultType> getResultTypeReference();

  public <T> CacheStrategy<ResultType, T, QueryType> getCacheStrategy(QueryType query)
  {
    return null;
  }

  public QueryRunner<ResultType> preMergeQueryDecoration(QueryRunner<ResultType> runner)
  {
    return runner;
  }

  public QueryRunner<ResultType> postMergeQueryDecoration(QueryRunner<ResultType> runner)
  {
    return runner;
  }

  /**
   * @param query
   * @param segments list of segments sorted by segment intervals.
   * @return list of segments to be queried in order to determine query results.
   */
  public <T extends LogicalSegment> List<T> filterSegments(QueryType query, List<T> segments)
  {
    return segments;
  }
}
