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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.ConcatOperator;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.scan.GroupedScanResultLimitOperator;
import org.apache.druid.queryng.operators.scan.ScanBatchToRowOperator;
import org.apache.druid.queryng.operators.scan.ScanCompactListToArrayOperator;
import org.apache.druid.queryng.operators.scan.ScanEngineOperator;
import org.apache.druid.queryng.operators.scan.ScanEngineOperator.CursorDefinition;
import org.apache.druid.queryng.operators.scan.ScanEngineOperator.Order;
import org.apache.druid.queryng.operators.scan.ScanListToArrayOperator;
import org.apache.druid.queryng.operators.scan.ScanResultOffsetOperator;
import org.apache.druid.queryng.operators.scan.UngroupedScanResultLimitOperator;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Scan-specific parts of the hybrid query planner.
 */
public class ScanPlanner
{
  /**
   * Sets up an operator over a ScanResultValue operator.  Its behaviour
   * varies depending on whether the query is returning time-ordered values
   * and whether the CTX_KEY_OUTERMOST flag is false.
   * <p>
   * Behaviours:
   * <ol>
   * <li>No time ordering: expects the child to produce ScanResultValues which
   *     each contain up to query.batchSize events. The operator will be "done"
   *     when the limit of events is reached.  The final ScanResultValue might contain
   *     fewer than batchSize events so that the limit number of events is returned.</li>
   * <li>Time Ordering, CTX_KEY_OUTERMOST false: Same behaviour as no time ordering.</li>
   * <li>Time Ordering, CTX_KEY_OUTERMOST=true or null: The child operator in this
   *     case should produce ScanResultValues that contain only one event each for
   *     the CachingClusteredClient n-way merge.  This operator will perform
   *     batching according to query batch size until the limit is reached.</li>
   * </ol>
   *
   * @see {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator}
   * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest#mergeResults}
   */
  public static Sequence<ScanResultValue> runLimitAndOffset(
      final QueryPlus<ScanResultValue> queryPlus,
      final QueryRunner<ScanResultValue> input,
      final ResponseContext responseContext,
      final ScanQueryConfig scanQueryConfig)
  {
    // Remove "offset" and add it to the "limit" (we won't push the offset
    // down, just apply it here, at the merge at the top of the stack).
    final ScanQuery originalQuery = (ScanQuery) queryPlus.getQuery();
    ScanQuery.verifyOrderByForNativeExecution(originalQuery);

    final boolean hasLimit = originalQuery.isLimited();
    final long limit = hasLimit ? originalQuery.getScanRowsLimit() : Long.MAX_VALUE;
    final long offset = originalQuery.getScanRowsOffset();
    final long newLimit;
    if (!hasLimit) {
      // Unlimited stays unlimited.
      newLimit = limit;
    } else if (limit > Long.MAX_VALUE - offset) {
      throw new ISE(
          "Cannot apply limit [%d] with offset [%d] due to overflow",
          limit,
          offset
      );
    } else {
      newLimit = limit + offset;
    }

    // Ensure "legacy" is a non-null value, such that all other nodes this
    // query is forwarded to will treat it the same way, even if they have
    // different default legacy values.
    final ScanQuery queryToRun = originalQuery.withNonNullLegacy(scanQueryConfig)
                                              .withOffset(0)
                                              .withLimit(newLimit);

    final boolean hasOffset = offset > 0;
    final boolean isGrouped = isGrouped(queryToRun);

    // Short-circuit if no limit or offset.
    if (!hasLimit && !hasOffset) {
      return input.run(queryPlus.withQuery(queryToRun), responseContext);
    }

    Query<ScanResultValue> historicalQuery = queryToRun;
    if (hasLimit) {
      ScanQuery.ResultFormat resultFormat = queryToRun.getResultFormat();
      if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
        throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
      }
      historicalQuery =
          queryToRun.withOverriddenContext(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false));
    }
    // No metrics past this point: metrics are not thread-safe.
    QueryPlus<ScanResultValue> historicalQueryPlus = queryPlus.withQuery(historicalQuery).withoutMetrics();
    FragmentContext fragmentContext = queryPlus.fragment();
    Operator<ScanResultValue> oper = Operators.toOperator(
        input,
        historicalQueryPlus);
    if (hasOffset) {
      oper = new ScanResultOffsetOperator(
          fragmentContext,
          oper,
          offset
          );
    }
    if (hasLimit) {
      if (isGrouped) {
        oper = new GroupedScanResultLimitOperator(
            fragmentContext,
            oper,
            limit
            );
      } else {
        oper = new UngroupedScanResultLimitOperator(
            fragmentContext,
            oper,
            limit,
            queryToRun.getBatchSize()
            );
      }
    }
    return Operators.toSequence(oper);
  }

  private static boolean isGrouped(ScanQuery query)
  {
    // TODO: Review
    return query.getTimeOrder() == ScanQuery.Order.NONE ||
        !query.getContextBoolean(ScanQuery.CTX_KEY_OUTERMOST, true);
  }

  /**
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#mergeRunners}
   */
  private static Sequence<ScanResultValue> runConcatMerge(
      final QueryPlus<ScanResultValue> queryPlus,
      final Iterable<QueryRunner<ScanResultValue>> queryRunners
  )
  {
    List<Operator<ScanResultValue>> inputs = new ArrayList<>();
    for (QueryRunner<ScanResultValue> qr : queryRunners) {
      inputs.add(Operators.toOperator(qr, queryPlus));
    }
    Operator<ScanResultValue> op = ConcatOperator.concatOrNot(
        queryPlus.fragment(),
        inputs);
    // TODO(paul): The original code applies a limit. Yet, when
    // run, the stack shows two limits one top of one another,
    // so the limit here seems unnecessary.
    // That is, we are doing a concat operation. It does not matter
    // if the limit is applied in the concat, or the next operator
    // along: in either case, we'll stop reading from upstream when the
    // limit is hit.
    //
    // ScanQuery query = (ScanQuery) queryPlus.getQuery();
    // if (query.isLimited()) {
    //   op = new ScanResultLimitOperator(
    //       query.getScanRowsLimit(),
    //       isGrouped(query),
    //       query.getBatchSize(),
    //       op
    //       );
    // }
    return Operators.toSequence(op);
  }

  /**
   * Given a set of scattered runners, gather the results via a merge.
   *
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#mergeRunners}
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#nWayMergeAndLimit}
   */
  public static Sequence<ScanResultValue> runMerge(
      final QueryPlus<ScanResultValue> queryPlus,
      final Iterable<QueryRunner<ScanResultValue>> queryRunners,
      final ResponseContext responseContext)
  {
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    ScanQuery.verifyOrderByForNativeExecution(query);
    // Note: this variable is effective only when queryContext has a timeout.
    // See the comment of ResponseContext.Key.TIMEOUT_AT.
    final long timeoutAt = System.currentTimeMillis() + QueryContexts.getTimeout(queryPlus.getQuery());
    responseContext.putTimeoutTime(timeoutAt);

    // TODO: Review
    if (query.getTimeOrder() == ScanQuery.Order.NONE) {
      // Use normal strategy
      return runConcatMerge(
          queryPlus,
          queryRunners);
    }
    return null;
  }

  /**
   * Convert the operator-based scan to that expected by the sequence-based
   * query runner.
   *
   * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory.ScanQueryRunner}
   * @see {@link org.apache.druid.query.scan.ScanQueryEngine}
   */
  public static Sequence<ScanResultValue> runScan(
      final QueryPlus<ScanResultValue> queryPlus,
      final Segment segment,
      final ResponseContext responseContext)
  {
    FragmentContext fragmentContext = queryPlus.fragment();
    if (isTombstone(segment)) {
      return Operators.toSequence(new NullOperator<>(fragmentContext));
    }
    if (!(queryPlus.getQuery() instanceof ScanQuery)) {
      throw new ISE("Got a [%s] which isn't a %s", queryPlus.getQuery().getClass(), ScanQuery.class);
    }
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    ScanQuery.verifyOrderByForNativeExecution((ScanQuery) query);
    final Long timeoutAt = responseContext.getTimeoutTime();
    if (timeoutAt == null || timeoutAt == 0L) {
      responseContext.putTimeoutTime(JodaUtils.MAX_INSTANT);
    }
    // TODO (paul): Set the timeout at the overall fragment context level.
    return Operators.toSequence(
        buildScanOperator(
            fragmentContext,
            query,
            segment,
            queryPlus.getQueryMetrics()
            )
        );
  }

  public static ScanEngineOperator buildScanOperator(
      final FragmentContext context,
      final ScanQuery query,
      final Segment segment,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got [%s]", intervals);
    // "legacy" should be non-null due to toolChest.mergeResults
    final boolean isLegacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
    final List<String> columns = defineColumns(query, isLegacy);

    final ScanEngineOperator.Order order;
    if (query.getTimeOrder() == ScanQuery.Order.NONE) {
      order = Order.NONE;
    } else if (isDescendingOrder(query)) {
      order = Order.DESCENDING;
    } else {
      order = Order.ASCENDING;
    }

    CursorDefinition cursorDefn = new CursorDefinition(
        segment,
        interval(query),
        filter,
        query.getVirtualColumns(),
        order == Order.DESCENDING,
        Granularities.ALL,
        queryMetrics
    );
    return new ScanEngineOperator(
          context,
          cursorDefn,
          query.getBatchSize(),
          isLegacy,
          columns,
          order,
          query.getScanRowsLimit(),
          query.getResultFormat(),
          QueryContexts.hasTimeout(query) ? context.responseContext().getTimeoutTime() : Long.MAX_VALUE
    );
  }

  /**
   * Define the query columns when the list is given by the query.
   */
  private static List<String> defineColumns(ScanQuery query, boolean isLegacy)
  {
    if (isWildcard(query)) {
      return null;
    }
    // Unless we're in legacy mode, planCols equals query.getColumns() exactly. This is nice since it makes
    // the compactedList form easier to use.
    List<String> queryCols = query.getColumns();
    if (isLegacy && !queryCols.contains(ScanEngineOperator.LEGACY_TIMESTAMP_KEY)) {
      final List<String> planCols = new ArrayList<>();
      planCols.add(ScanEngineOperator.LEGACY_TIMESTAMP_KEY);
      planCols.addAll(queryCols);
      return planCols;
    } else {
      return queryCols;
    }
  }

  public static boolean isWildcard(ScanQuery query)
  {
    // Missing or empty list means wildcard
    List<String> queryCols = query.getColumns();
    return (queryCols == null || queryCols.isEmpty());
  }

  // TODO: Review against latest
  public static boolean isDescendingOrder(final ScanQuery query)
  {
    return query.getTimeOrder().equals(ScanQuery.Order.DESCENDING) ||
        (query.getTimeOrder().equals(ScanQuery.Order.NONE) && query.isDescending());
  }

  private static boolean isTombstone(final Segment segment)
  {
    QueryableIndex queryableIndex = segment.asQueryableIndex();
    return queryableIndex != null && queryableIndex.isFromTombstone();
  }

  private static Interval interval(final ScanQuery query)
  {
    return query.getQuerySegmentSpec().getIntervals().get(0);
  }

  public static Sequence<Object[]> resultsAsArrays(
      final QueryPlus<ScanResultValue> queryPlus,
      final List<String> fields,
      final Sequence<ScanResultValue> resultSequence)
  {
    FragmentContext context = queryPlus.fragment();
    Operator<ScanResultValue> inputOp = Operators.toOperator(
        context,
        resultSequence);
    Operator<Object[]> outputOp;
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    switch (query.getResultFormat()) {
      case RESULT_FORMAT_LIST:
        outputOp = new ScanListToArrayOperator(
            context,
            new ScanBatchToRowOperator<Map<String, Object>>(
                context,
                inputOp),
            fields);
        break;
      case RESULT_FORMAT_COMPACTED_LIST:
        outputOp = new ScanCompactListToArrayOperator(
            context,
            new ScanBatchToRowOperator<List<Object>>(
                context,
                inputOp),
            fields);
        break;
      default:
        throw new UOE("Unsupported resultFormat for array-based results: %s", query.getResultFormat());
    }
    return Operators.toSequence(outputOp);
  }
}
