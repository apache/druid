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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Function;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.LogicalSegment;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;

/**
 * The broker-side (also used by server in some cases) API for a specific Query type.
 */
@ExtensionPoint
public abstract class QueryToolChest<ResultType, QueryType extends Query<ResultType>>
{
  private final JavaType baseResultType;
  private final JavaType bySegmentResultType;

  protected QueryToolChest()
  {
    final TypeFactory typeFactory = TypeFactory.defaultInstance();
    TypeReference<ResultType> resultTypeReference = getResultTypeReference();
    // resultTypeReference is null in MaterializedViewQueryQueryToolChest.
    // See https://github.com/apache/druid/issues/6977
    if (resultTypeReference != null) {
      baseResultType = typeFactory.constructType(resultTypeReference);
      bySegmentResultType = typeFactory.constructParametrizedType(
          Result.class,
          Result.class,
          typeFactory.constructParametrizedType(
              BySegmentResultValueClass.class,
              BySegmentResultValueClass.class,
              baseResultType
          )
      );
    } else {
      baseResultType = null;
      bySegmentResultType = null;
    }
  }

  public final JavaType getBaseResultType()
  {
    return baseResultType;
  }

  public final JavaType getBySegmentResultType()
  {
    return bySegmentResultType;
  }

  /**
   * Perform any per-query decoration of an {@link ObjectMapper} that enables it to read and write objects of the
   * query's {@link ResultType}. It is used by QueryResource on the write side, and DirectDruidClient on the read side.
   * <p>
   * For most queries, this is a no-op, but it can be useful for query types that support more than one result
   * serialization format. Queries that implement this method must not modify the provided ObjectMapper, but instead
   * must return a copy.
   */
  public ObjectMapper decorateObjectMapper(final ObjectMapper objectMapper, final QueryType query)
  {
    return objectMapper;
  }

  /**
   * This method wraps a QueryRunner.  The input QueryRunner, by contract, will provide a series of
   * ResultType objects in time order (ascending or descending).  This method should return a new QueryRunner that
   * merges the stream of ordered ResultType objects.
   * <p>
   * A default implementation constructs a {@link ResultMergeQueryRunner} which creates a
   * {@link org.apache.druid.common.guava.CombiningSequence} using the supplied {@link QueryRunner} with
   * {@link QueryToolChest#createResultComparator(Query)} and {@link QueryToolChest#createMergeFn(Query)}} supplied
   * by this toolchest.
   * <p>
   * Generally speaking, the logic that exists in makePostComputeManipulatorFn should actually exist in this method.
   * Additionally, if a query supports PostAggregations, this method should take steps to ensure that it computes
   * PostAggregations a minimum number of times.  This is most commonly achieved by computing the PostAgg results
   * during merge <strong>and also</strong> rewriting the query such that it has the minimum number of PostAggs (most
   * often zero).
   *
   * @param runner A QueryRunner that provides a series of ResultType objects in time order (ascending or descending)
   * @return a QueryRunner that merges the stream of ordered ResultType objects
   */
  public QueryRunner<ResultType> mergeResults(QueryRunner<ResultType> runner)
  {
    return new ResultMergeQueryRunner<>(runner, this::createResultComparator, this::createMergeFn);
  }

  /**
   * Like {@link #mergeResults(QueryRunner)}, but with an additional flag that indicates the type of runner that is passeed to the call.
   *
   * willMergeRunner specifies that the input runner to the mergeResults would be the one created by the corresponding
   * {@link QueryRunnerFactory#mergeRunners}.
   * While it depends on the input runner, it is usually true since most of the time the same server is generating a runner
   * that it wants to merge. The notable deviation from this norm is when the broker is accumulating the results from the
   * data servers and needs to merge them together. In this case willMergeRunner is false.
   *
   * Currently, the sole consumer of this parameter is {@link org.apache.druid.query.groupby.GroupByQueryQueryToolChest}, where
   * it is used to determine if the mergeResults is called with {@link org.apache.druid.query.groupby.epinephelinae.GroupByMergingQueryRunner}
   * to estimate the number of merge buffers required for the query to succeed. It is set false on the brokers, because they
   * (mostly) fetch the results from the historicals, while the data servers set it to false (because they call this method
   * with {@link QueryRunnerFactory#mergeRunners}.
   *
   * By default, the willMergeRunners is ignored, and the {@link #mergeResults(QueryRunner)} is called. For the toolchests
   * that override this method must ensure that {@link #mergeResults(QueryRunner)} delegates to it (else it will use the
   * default implementation for {@link #mergeResults(QueryRunner)}) which would be undesirable.
   */
  public QueryRunner<ResultType> mergeResults(QueryRunner<ResultType> runner, boolean willMergeRunner)
  {
    return mergeResults(runner);
  }

  /**
   * Creates a merge function that is used to merge intermediate aggregates from historicals in broker. This merge
   * function is used in the default {@link ResultMergeQueryRunner} provided by
   * {@link QueryToolChest#mergeResults(QueryRunner)} and also used in
   * {@link org.apache.druid.java.util.common.guava.ParallelMergeCombiningSequence} by 'CachingClusteredClient' if it
   * does not return null.
   * <p>
   * Returning null from this function means that a query does not support result merging, at
   * least via the mechanisms that utilize this function.
   */
  @Nullable
  public BinaryOperator<ResultType> createMergeFn(Query<ResultType> query)
  {
    return null;
  }

  /**
   * Creates an ordering comparator that is used to order results. This comparator is used in the default
   * {@link ResultMergeQueryRunner} provided by {@link QueryToolChest#mergeResults(QueryRunner)}
   */
  public Comparator<ResultType> createResultComparator(Query<ResultType> query)
  {
    throw DruidException.defensive("%s doesn't provide a result comparator", query.getClass().getName());
  }

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
   * @return A QueryMetrics that can be used to make metrics for the provided query
   */
  public abstract QueryMetrics<? super QueryType> makeMetrics(QueryType query);

  /**
   * Creates a Function that can take in a ResultType and return a new ResultType having applied
   * the MetricManipulatorFn to each of the metrics.
   * <p>
   * This function's primary purpose is to help work around some challenges that exist around deserializing
   * results across the wire.  Specifically, different aggregators will generate different object types in a
   * result set, if we wanted jackson to be able to deserialize these directly, we'd need to generate a response
   * class for each query that jackson could use to deserialize things.  That is not what we do.  Instead, we have
   * jackson deserialize Object instances and then use a MetricManipulatorFn to convert from those object instances
   * to the actual object that the aggregator expects.  As such, this would be more effectively named
   * "makeObjectDeserializingFn".
   * <p>
   * It is safe and acceptable for implementations of this method to first validate that the MetricManipulationFn
   * is {@link org.apache.druid.query.aggregation.MetricManipulatorFns#DESERIALIZING_INSTANCE} and throw an exception
   * if it is not.  If such an exception is ever thrown, it is indicative of a bug in the caller which should be fixed
   * by not calling this method with anything other than the deserializing manipulator function.
   * <p>
   * There are some implementations where this was also tasked with computing PostAggregators, but this is actually
   * not a good place to compute those as this function can be called in a number of cases when PostAggs are not
   * really meaningful to compute.  Instead, PostAggs should be computed in the mergeResults call and the
   * mergeResults implementation should take care to ensure that PostAggs are only computed the minimum number of
   * times necessary.
   * <p>
   * This function is called very early in the processing pipeline on the Broker.
   *
   * @param query The Query that is currently being processed
   * @param fn    The function that should be applied to all metrics in the results
   * @return A function that will apply the provided fn to all metrics in the input ResultType object
   */
  public abstract Function<ResultType, ResultType> makePreComputeManipulatorFn(
      QueryType query,
      MetricManipulationFn fn
  );

  /**
   * This manipulator functions primary purpose is to conduct finalization of aggregator values.  It would be better
   * named "makeFinalizingManipulatorFn", even that should really be done as part of {@link #mergeResults} instead
   * of with this separate method.
   * <p>
   * It is safe and acceptable for implementations of this method to first validate that the MetricManipulationFn
   * is either {@link org.apache.druid.query.aggregation.MetricManipulatorFns#FINALIZING_INSTANCE} or
   * {@link org.apache.druid.query.aggregation.MetricManipulatorFns#IDENTITY_INSTANCE} and throw an exception
   * if it is not.  If such an exception is ever thrown, it is indicative of a bug in the caller which should be fixed
   * by not calling this method with unsupported manipulator functions.
   *
   * @param query The Query that is currently being processed
   * @param fn    The function that should be applied to all metrics in the results
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
   * @return The list of segments to actually query
   */
  public <T extends LogicalSegment> List<T> filterSegments(QueryType query, List<T> segments)
  {
    return segments;
  }

  /**
   * Returns whether this toolchest is able to handle the provided subquery.
   * <p>
   * When this method returns true, the core query stack will pass subquery datasources over to the toolchest and will
   * assume they are properly handled.
   * <p>
   * When this method returns false, the core query stack will throw an error if subqueries are present. In the future,
   * instead of throwing an error, the core query stack will handle the subqueries on its own.
   */
  public boolean canPerformSubquery(final Query<?> subquery)
  {
    return false;
  }

  /**
   * Returns a {@link RowSignature} for the arrays returned by {@link #resultsAsArrays}. The returned signature will
   * be the same length as each array returned by {@link #resultsAsArrays}.
   *
   * @param query same query passed to {@link #resultsAsArrays}
   * @return row signature
   * @throws UnsupportedOperationException if this query type does not support returning results as arrays
   */
  public RowSignature resultArraySignature(QueryType query)
  {
    throw new UOE("Query type '%s' does not support returning results as arrays", query.getType());
  }

  /**
   * Converts a sequence of this query's ResultType into arrays. The array signature is given by
   * {@link #resultArraySignature}. This functionality is useful because it allows higher-level processors to operate on
   * the results of any query in a consistent way. This is useful for the SQL layer and for any algorithm that might
   * operate on the results of an inner query.
   * <p>
   * Not all query types support this method. They will throw {@link UnsupportedOperationException}, and they cannot
   * be used by the SQL layer or by generic higher-level algorithms.
   * <p>
   * Some query types return less information after translating their results into arrays, especially in situations
   * where there is no clear way to translate fully rich results into flat arrays. For example, the scan query does not
   * include the segmentId in its array-based results, because it could potentially conflict with a 'segmentId' field
   * in the actual datasource being scanned.
   * <p>
   * It is possible that there will be multiple arrays returned for a single result object. For example, in the topN
   * query, each {@link org.apache.druid.query.topn.TopNResultValue} will generate a separate array for each of its
   * {@code values}.
   * <p>
   * By convention, the array form should include the __time column, if present, as a long (milliseconds since epoch).
   *
   * @param resultSequence results of the form returned by {@link #mergeResults}
   * @return results in array form
   * @throws UnsupportedOperationException if this query type does not support returning results as arrays
   */
  public Sequence<Object[]> resultsAsArrays(QueryType query, Sequence<ResultType> resultSequence)
  {
    throw new UOE("Query type '%s' does not support returning results as arrays", query.getType());
  }

  /**
   * Converts a sequence of this query's ResultType into a sequence of {@link FrameSignaturePair}. The array signature
   * is the one give by {@link #resultArraySignature(Query)}. If the toolchest doesn't support this method, then it can
   * return an empty optional. It is the duty of the callees to throw an appropriate exception in that case or use an
   * alternative fallback approach
   * <p>
   * Check documentation of {@link #resultsAsArrays(Query, Sequence)} as the behaviour of the rows represented by the
   * frame sequence is identical.
   * <p>
   * Each Frame has a separate {@link RowSignature} because for some query types like the Scan query, every
   * column in the final result might not be present in the individual ResultType (and subsequently Frame). Therefore,
   * this is done to preserve the space by not populating the column in that particular Frame and omitting it from its
   * signature
   *
   * @param query                    Query being executed by the toolchest. Used to determine the rowSignature of the Frames
   * @param resultSequence           results of the form returned by {@link #mergeResults(QueryRunner)}
   * @param memoryAllocatorFactory
   * @param useNestedForUnknownTypes true if the unknown types in the results can be serded using complex types
   */
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      QueryType query,
      Sequence<ResultType> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes
  )
  {
    return Optional.empty();
  }
}
