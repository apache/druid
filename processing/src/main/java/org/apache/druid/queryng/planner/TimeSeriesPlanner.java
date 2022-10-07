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

package org.apache.druid.queryng.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.LimitOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.general.ScatterGatherOperator.OrderedScatterGatherOperator;
import org.apache.druid.queryng.operators.timeseries.GrandTotalOperator;
import org.apache.druid.queryng.operators.timeseries.IntermediateAggOperator;
import org.apache.druid.queryng.operators.timeseries.TimeseriesEngineOperator;
import org.apache.druid.queryng.operators.timeseries.TimeseriesEngineOperator.CursorDefinition;
import org.apache.druid.queryng.operators.timeseries.ToArrayOperator;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.Filters;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Time series-specific parts of the hybrid query planner.
 */
public class TimeSeriesPlanner
{
  /**
   * Plan a time-series grand merge: combine per-segment values to create
   * totals (if requested), limit (if requested), and create grand totals
   * (if requested).
   *
   * @see TimeseriesQueryQueryToolChest#mergeResults
   */
  public static Sequence<Result<TimeseriesResultValue>> mergeResults(
      QueryPlus<Result<TimeseriesResultValue>> queryPlus,
      QueryRunner<Result<TimeseriesResultValue>> inputQueryRunner,
      Function<Query<Result<TimeseriesResultValue>>, Comparator<Result<TimeseriesResultValue>>> comparatorGenerator,
      Function<Query<Result<TimeseriesResultValue>>, BinaryOperator<Result<TimeseriesResultValue>>> mergeFnGenerator
  )
  {
    final FragmentContext fragmentContext = queryPlus.fragment();
    final TimeseriesQuery query = (TimeseriesQuery) queryPlus.getQuery();
    final Operator<Result<TimeseriesResultValue>> inputOp = Operators.toOperator(inputQueryRunner, queryPlus);

    Operator<Result<TimeseriesResultValue>> op;
    if (QueryContexts.isBySegment(queryPlus.getQuery())) {
      op = inputOp;
    } else {
      final Supplier<Result<TimeseriesResultValue>> zeroResultSupplier;
      // When granularity = ALL, there is no grouping key for this query.
      // To be more sql-compliant, we should return something (e.g., 0 for count queries) even when
      // the sequence is empty.
      if (query.getGranularity().equals(Granularities.ALL) &&
              // Returns empty sequence if this query allows skipping empty buckets
              !query.isSkipEmptyBuckets() &&
              // Returns empty sequence if bySegment is set because bySegment results are mostly used for
              // caching in historicals or debugging where the exact results are preferred.
              !QueryContexts.isBySegment(query)) {
        // A bit of a hack to avoid passing the query into the operator.
        zeroResultSupplier = () -> TimeseriesQueryQueryToolChest.getNullTimeseriesResultValue(query);
      } else {
        zeroResultSupplier = null;
      }
      // Don't do post aggs until
      // TimeseriesQueryQueryToolChest.makePostComputeManipulatorFn() is called
      TimeseriesQuery aggQuery = query.withPostAggregatorSpecs(ImmutableList.of());
      op = new IntermediateAggOperator(
          fragmentContext,
          inputOp,
          comparatorGenerator.apply(aggQuery),
          mergeFnGenerator.apply(aggQuery),
          zeroResultSupplier
      );
    }

    // Apply limit to the aggregated values.
    final int limit = query.getLimit();
    if (limit < Integer.MAX_VALUE) {
      op = new LimitOperator<Result<TimeseriesResultValue>>(fragmentContext, op, limit);
    }

    if (query.isGrandTotal()) {
      op = new GrandTotalOperator(
          fragmentContext,
          op,
          query.getAggregatorSpecs()
      );
    }

    // Return the result as a sequence.
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> scatterGather(
      final QueryPlus<T> queryPlus,
      final QueryProcessingPool queryProcessingPool,
      final Iterable<QueryRunner<T>> queryables,
      final QueryWatcher queryWatcher
  )
  {
    final Operator<T> op = new OrderedScatterGatherOperator<T>(
        queryPlus,
        queryProcessingPool,
        queryables,
        queryPlus.getQuery().getResultOrdering(),
        queryWatcher
    );
    return Operators.toSequence(op);
  }

  public static Sequence<Object[]> toArray(
      final QueryPlus<Result<TimeseriesResultValue>> queryPlus,
      final Sequence<Result<TimeseriesResultValue>> resultSequence,
      final RowSignature signature
  )
  {
    final Operator<Object[]> op = new ToArrayOperator(
        queryPlus.fragment(),
        Operators.unwrapOperator(resultSequence),
        signature.getColumnNames()
    );
    return Operators.toSequence(op);
  }

  public static Sequence<Result<TimeseriesResultValue>> queryEngine(
      final QueryPlus<Result<TimeseriesResultValue>> queryPlus,
      final StorageAdapter adapter,
      final NonBlockingPool<ByteBuffer> bufferPool
  )
  {
    final Query<Result<TimeseriesResultValue>> input = queryPlus.getQuery();
    if (!(input instanceof TimeseriesQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeseriesQuery.class);
    }
    final TimeseriesQuery query = (TimeseriesQuery) input;

    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
    final boolean descending = query.isDescending();
    final CursorDefinition cursorDefn = new CursorDefinition(
        adapter,
        Iterables.getOnlyElement(query.getIntervals()),
        filter,
        query.getVirtualColumns(),
        descending, // Non-vectorized only
        query.getGranularity(),
        queryPlus.getQueryMetrics(),
        QueryContexts.getVectorSize(query) // Vectorized only
    );

    final ColumnInspector inspector = query.getVirtualColumns().wrapInspector(adapter);
    final boolean doVectorize = QueryContexts.getVectorize(query).shouldVectorize(
        adapter.canVectorize(filter, query.getVirtualColumns(), descending)
        && VirtualColumns.shouldVectorize(query, query.getVirtualColumns(), adapter)
        && query.getAggregatorSpecs().stream().allMatch(aggregatorFactory -> aggregatorFactory.canVectorize(inspector))
    );

    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        queryPlus.fragment(),
        cursorDefn,
        query.getAggregatorSpecs(),
        doVectorize ? bufferPool : null,
        query.isSkipEmptyBuckets()
    );
    final int limit = query.getLimit();
    if (limit < Integer.MAX_VALUE) {
      op = new LimitOperator<Result<TimeseriesResultValue>>(
          queryPlus.fragment(),
          op,
          limit
      );
    }
    return Operators.toSequence(op);
  }
}
