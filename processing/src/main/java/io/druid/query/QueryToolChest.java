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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.timeline.LogicalSegment;

import javax.annotation.Nullable;
import java.util.List;

/**
 * The broker-side (also used by server in some cases) API for a specific Query type.  This API is still undergoing
 * evolution and is only semi-stable, so proprietary Query implementations should be ready for the potential
 * maintenance burden when upgrading versions.
 */
public abstract class QueryToolChest<ResultType, QueryType extends Query<ResultType>>
{
  /**
   * This method wraps a QueryRunner.  The input QueryRunner, by contract, will provide a series of
   * ResultType objects in time order (ascending or descending).  This method should return a new QueryRunner that
   * potentially merges the stream of ordered ResultType objects.
   *
   * @param runner A QueryRunner that provides a series of ResultType objects in time order (ascending or descending)
   *
   * @return a QueryRunner that potentially merges the stream of ordered ResultType objects
   */
  public abstract QueryRunner<ResultType> mergeResults(QueryRunner<ResultType> runner);

  /**
   * Creates a {@link QueryMetrics} object that is used to generate metrics for this specific query type.  This exists
   * to allow for query-specific dimensions and metrics.  That is, the ToolChest is expected to set some
   * meaningful dimensions for metrics given this query type.  Examples might be the topN threshold for
   * a TopN query or the number of dimensions included for a groupBy query.
   * 
   * <p>QueryToolChests for query types in core (druid-processing) and public extensions (belonging to the Druid source
   * tree) should use delegate this method to {@link GenericQueryMetricsFactory#makeMetrics(Query)} on an injected
   * instance of {@link GenericQueryMetricsFactory}, as long as they don't need to emit custom dimensions and/or
   * metrics.
   *
   * <p>If some custom dimensions and/or metrics should be emitted for a query type, a plan described in
   * "Making subinterfaces of QueryMetrics" section in {@link QueryMetrics}'s class-level Javadocs should be followed.
   *
   * <p>One way or another, this method should ensure that {@link QueryMetrics#query(Query)} is called with the given
   * query passed on the created QueryMetrics object before returning.
   *
   * @param query The query that is being processed
   *
   * @return A QueryMetrics that can be used to make metrics for the provided query
   */
  public abstract QueryMetrics<? super QueryType> makeMetrics(QueryType query);

  /**
   * Creates a Function that can take in a ResultType and return a new ResultType having applied
   * the MetricManipulatorFn to each of the metrics.
   * <p>
   * This exists because the QueryToolChest is the only thing that understands the internal serialization
   * format of ResultType, so it's primary responsibility is to "decompose" that structure and apply the
   * given function to all metrics.
   * <p>
   * This function is called very early in the processing pipeline on the Broker.
   *
   * @param query The Query that is currently being processed
   * @param fn    The function that should be applied to all metrics in the results
   *
   * @return A function that will apply the provided fn to all metrics in the input ResultType object
   */
  public abstract Function<ResultType, ResultType> makePreComputeManipulatorFn(
      QueryType query,
      MetricManipulationFn fn
  );

  /**
   * Generally speaking this is the exact same thing as makePreComputeManipulatorFn.  It is leveraged in
   * order to compute PostAggregators on results after they have been completely merged together, which
   * should actually be done in the mergeResults() call instead of here.
   * <p>
   * This should never actually be overridden and it should be removed as quickly as possible.
   *
   * @param query The Query that is currently being processed
   * @param fn    The function that should be applied to all metrics in the results
   *
   * @return A function that will apply the provided fn to all metrics in the input ResultType object
   */
  public Function<ResultType, ResultType> makePostComputeManipulatorFn(QueryType query, MetricManipulationFn fn)
  {
    return makePreComputeManipulatorFn(query, fn);
  }

  /**
   * Returns a TypeReference object that is just passed through to Jackson in order to deserialize
   * the results of this type of query.
   *
   * @return A TypeReference to indicate to Jackson what type of data will exist for this query
   */
  public abstract TypeReference<ResultType> getResultTypeReference();

  /**
   * Returns a CacheStrategy to be used to load data into the cache and remove it from the cache.
   * <p>
   * This is optional.  If it returns null, caching is effectively disabled for the query.
   *
   * @param query The query whose results might be cached
   * @param <T>   The type of object that will be stored in the cache
   *
   * @return A CacheStrategy that can be used to populate and read from the Cache
   */
  @Nullable
  public <T> CacheStrategy<ResultType, T, QueryType> getCacheStrategy(QueryType query)
  {
    return null;
  }

  /**
   * Wraps a QueryRunner.  The input QueryRunner is the QueryRunner as it exists *before* being passed to
   * mergeResults().
   * <p>
   * In fact, the return value of this method is always passed to mergeResults, so it is equivalent to
   * just implement this functionality as extra decoration on the QueryRunner during mergeResults().
   * <p>
   * In the interests of potentially simplifying these interfaces, the recommendation is to actually not
   * override this method and instead apply anything that might be needed here in the mergeResults() call.
   *
   * @param runner The runner to be wrapped
   *
   * @return The wrapped runner
   */
  public QueryRunner<ResultType> preMergeQueryDecoration(QueryRunner<ResultType> runner)
  {
    return runner;
  }

  /**
   * Wraps a QueryRunner.  The input QueryRunner is the QueryRunner as it exists coming out of mergeResults()
   * <p>
   * In fact, the input value of this method is always the return value from mergeResults, so it is equivalent
   * to just implement this functionality as extra decoration on the QueryRunner during mergeResults().
   * <p>
   * In the interests of potentially simplifying these interfaces, the recommendation is to actually not
   * override this method and instead apply anything that might be needed here in the mergeResults() call.
   *
   * @param runner The runner to be wrapped
   *
   * @return The wrapped runner
   */
  public QueryRunner<ResultType> postMergeQueryDecoration(QueryRunner<ResultType> runner)
  {
    return runner;
  }

  /**
   * This method is called to allow the query to prune segments that it does not believe need to actually
   * be queried.  It can use whatever criteria it wants in order to do the pruning, it just needs to
   * return the list of Segments it actually wants to see queried.
   *
   * @param query    The query being processed
   * @param segments The list of candidate segments to be queried
   * @param <T>      A Generic parameter because Java is cool
   *
   * @return The list of segments to actually query
   */
  public <T extends LogicalSegment> List<T> filterSegments(QueryType query, List<T> segments)
  {
    return segments;
  }
}
