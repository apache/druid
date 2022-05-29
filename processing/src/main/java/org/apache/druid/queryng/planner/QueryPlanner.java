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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.UnionQueryRunner;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.SpecificSegmentQueryRunner;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.queryng.Timer;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.ConcatOperator;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.TransformOperator;
import org.apache.druid.queryng.operators.general.CpuMetricOperator;
import org.apache.druid.queryng.operators.general.MergeOperator;
import org.apache.druid.queryng.operators.general.MetricsOperator;
import org.apache.druid.queryng.operators.general.QueryRunnerFactoryOperator;
import org.apache.druid.queryng.operators.general.SegmentLockOperator;
import org.apache.druid.queryng.operators.general.ThreadLabelOperator;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.utils.JvmUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;

/**
 * Hybrid query planner/runner to act as a "shim" between the existing
 * QueryRunner and similar abstractions and the operator-based execution
 * layer. Over time, this should be replaced with a proper low-level
 * planner that just does planning by eliminating the use of sequences.
 * Then, once the operator pipeline is planned, just run the result.
 * <p>
 * This class collects, in one place, the steps needed to plan (and, for
 * now, run) a scan query. The functions are referenced from various
 * query runners, factories, toolchests, etc. to preserve the existing
 * structure. This odd shape is an intermediate step to the goal explained
 * above.
 * <p>
 * The key thing to note is that the original runner-based model gives a
 * query as input, and returns a sequence. In this shim, the sequence is
 * a wrapper around an operator. Operators will "unwrap" the sequence of
 * we have the pattern:<br/>
 * operator &rarr; sequence &rarr; operator</br
 * to produce:<br/>
 * operator &rarr; operator</br>
 * Once all runners simply produce a wrapped operator, using this class,
 * then the runners themselves, and the sequences, can melt away leaving
 * just a pipeline of operators, thereby separating the low-level plan
 * phase from the actual execution phase.
 * <p>
 * Careful inspection will show that much of the code here is redundant:
 * there is repeated wrapping and unwrapping of operators. The same
 * values are passed in repeatedly. When converted to an operator-only
 * planner, the wrapping and unwrapping will disappear, and the
 * constant-per-query values can be set in a constructor and reused.
 * <p>
 * At present, the planner consists of a bunch of static functions called
 * from query runners. As a result, all required info is passed in to
 * each function. Once we can retire the query runners, this turns into
 * a stateful class that holds the common information about a query.
 * The functions become methods that do only operator planning (without
 * the sequence wrappers.) Flow of control is between methods, not
 * from query runners into this class as in the current structure.
 */
public class QueryPlanner
{
  protected static <T> Operator<T> concat(FragmentContext context, List<Operator<T>> children)
  {
    return ConcatOperator.concatOrNot(context, children);
  }

  /**
   * @see {org.apache.druid.query.CPUTimeMetricQueryRunner}
   */
  public static <T> Sequence<T> runCpuTimeMetric(
      final QueryRunner<T> delegate,
      final QueryToolChest<T, ? extends Query<T>> queryToolChest,
      final AtomicLong cpuTimeAccumulator,
      final ServiceEmitter emitter,
      final boolean report,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext)
  {
    final QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);

    // Short circuit if not reporting CPU time.
    if (!report || !JvmUtils.isThreadCpuTimeEnabled()) {
      return delegate.run(queryWithMetrics, responseContext);
    }
    Operator<T> inputOp = Operators.toOperator(
        delegate,
        queryWithMetrics
    );
    CpuMetricOperator<T> op = new CpuMetricOperator<T>(
        queryPlus.fragment(),
        cpuTimeAccumulator,
        queryWithMetrics.getQueryMetrics(),
        emitter,
        inputOp
    );
    return Operators.toSequence(op);
  }

  /**
   * Plan step that applies {@link QueryToolChest#makePostComputeManipulatorFn(Query, MetricManipulationFn)} to the
   * result stream. It is expected to be the last operator in the pipeline, after results are fully merged.
   * <p>
   * Note that despite the type parameter "T", this runner may not actually return sequences with type T. This most
   * commonly happens when an upstream {@link BySegmentQueryRunner} changes the result stream to type
   * {@code Result<BySegmentResultValue<T>>}, in which case this class will retain the structure, but call the finalizer
   * function on each result in the by-segment list (which may change their type from T to something else).
   *
   * @see {@link org.apache.druid.query.FinalizeResultsQueryRunner}
   */
  public static <T> Sequence<T> runFinalizeResults(
      final QueryRunner<T> baseRunner,
      final QueryToolChest<T, Query<T>> toolChest,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext
  )
  {
    final Query<T> query = queryPlus.getQuery();
    final boolean shouldFinalize = QueryContexts.isFinalize(query, true);

    final Query<T> queryToRun;
    final MetricManipulationFn metricManipulationFn;

    if (shouldFinalize) {
      queryToRun = query.withOverriddenContext(ImmutableMap.of(QueryContexts.FINALIZE_KEY, false));
      metricManipulationFn = MetricManipulatorFns.finalizing();
    } else {
      queryToRun = query;
      metricManipulationFn = MetricManipulatorFns.identity();
    }

    final Function<T, T> baseFinalizer = toolChest.makePostComputeManipulatorFn(
        query,
        metricManipulationFn
        );
    final Function<T, ?> finalizerFn;
    if (QueryContexts.isBySegment(query)) {
      finalizerFn = new Function<T, Result<BySegmentResultValue<T>>>()
      {
        @Override
        public Result<BySegmentResultValue<T>> apply(T input)
        {
          //noinspection unchecked (input is not actually a T; see class-level javadoc)
          @SuppressWarnings("unchecked")
          Result<BySegmentResultValueClass<T>> result = (Result<BySegmentResultValueClass<T>>) input;

          if (input == null) {
            throw new ISE("Cannot have a null result!");
          }

          BySegmentResultValue<T> resultsClass = result.getValue();

          return new Result<>(
              result.getTimestamp(),
              new BySegmentResultValueClass<>(
                  Lists.transform(resultsClass.getResults(), baseFinalizer),
                  resultsClass.getSegmentId(),
                  resultsClass.getInterval()
              )
          );
        }
      };
    } else if (baseFinalizer == Functions.identity()) {
      // Optimize away the finalizer operator if nothing to do.
      return baseRunner.run(queryPlus.withQuery(queryToRun), responseContext);
    } else {
      finalizerFn = baseFinalizer;
    }

    QueryPlus<T> queryPlusToRun = queryPlus.withQuery(queryToRun);
    Operator<T> inputOp = Operators.toOperator(
        baseRunner,
        queryPlusToRun);
    @SuppressWarnings("unchecked")
    TransformOperator<T, T> op = new TransformOperator<>(
        queryPlus.fragment(),
        inputOp,
        (Function<T, T>) finalizerFn,
        "finalizer"
    );
    return Operators.toSequence(op);
  }

  /**
   * Plan for a specific segment. The {@code SpecificSegmentQueryRunner}
   * does two things: renames the thread and reports missing segments by
   * catching the {@code SegmentMissingException}. In the operator model,
   * the missing segment is caught in the
   * {@link SegmentLockOperator} and no exception is thrown.
   * <p>
   * The only place the exception is thrown is in the
   * {@code TimeseriesQueryEngine} and {@code TopNQueryEngine}. Those
   * should be caught by the corresponding operators.
   *
   * @see {@link org.apache.druid.query.spec.SpecificSegmentQueryRunner}
   */
  public static <T> Sequence<T> runSpecificSegment(
      final QueryRunner<T> base,
      final SpecificSegmentSpec specificSpec,
      final QueryPlus<T> input,
      final ResponseContext responseContext
  )
  {
    final QueryPlus<T> queryPlus = input.withQuery(
        Queries.withSpecificSegments(
            input.getQuery(),
            Collections.singletonList(specificSpec.getDescriptor())
        )
    );
    final Query<T> query = queryPlus.getQuery();
    final String newName = query.getType() + "_" + query.getDataSource() + "_" + query.getIntervals();
    Operator<T> op = Operators.toOperator(base, queryPlus);
    final boolean setName = input.getQuery().getContextBoolean(
        SpecificSegmentQueryRunner.CTX_SET_THREAD_NAME,
        true);
    if (setName) {
      op = new ThreadLabelOperator<T>(
          queryPlus.fragment(),
          newName,
          op);
    }
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> runMetrics(
      final ServiceEmitter emitter,
      final QueryToolChest<T, ? extends Query<T>> queryToolChest,
      final QueryRunner<T> queryRunner,
      final long creationTimeNs,
      final ObjLongConsumer<QueryMetrics<?>> reportMetric,
      final Consumer<QueryMetrics<?>> applyCustomDimensions,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext
  )
  {
    final QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);
    final QueryMetrics<?> queryMetrics = queryWithMetrics.getQueryMetrics();

    applyCustomDimensions.accept(queryMetrics);

    // Short circuit if no metrics.
    if (queryMetrics == null) {
      return queryRunner.run(queryWithMetrics, responseContext);
    }
    Operator<T> inputOp = Operators.toOperator(
        queryRunner,
        queryWithMetrics);
    MetricsOperator<T> op = new MetricsOperator<>(
        queryPlus.fragment(),
        emitter,
        queryMetrics,
        reportMetric,
        creationTimeNs == 0 ? null : Timer.createAt(creationTimeNs),
        inputOp);
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> runSegmentLock(
      final SegmentReference segment,
      final SegmentDescriptor descriptor,
      final QueryPlus<T> queryPlus,
      final QueryRunnerFactory<T, Query<T>> factory,
      final ResponseContext responseContext
  )
  {
    // The factory operator defers creating the runner until
    // after the lock operator has obtained a segment lock.
    Operator<T> inputOp = new QueryRunnerFactoryOperator<T>(
        () -> factory.createRunner(segment),
        queryPlus);
    SegmentLockOperator<T> op = new SegmentLockOperator<>(
        queryPlus.fragment(),
        segment,
        descriptor,
        inputOp);
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> runUnionQuery(
      final QueryPlus<T> queryPlus,
      final QueryRunner<T> baseRunner,
      final ResponseContext responseContext
  )
  {
    FragmentContext fragmentContext = queryPlus.fragment();
    Query<T> query = queryPlus.getQuery();
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (!analysis.isConcreteTableBased() || !analysis.getBaseUnionDataSource().isPresent()) {
      // Not a union of tables. Do nothing special.
      return baseRunner.run(queryPlus, responseContext);
    }

    // Union of tables.
    final UnionDataSource unionDataSource = analysis.getBaseUnionDataSource().get();
    if (unionDataSource.getDataSources().isEmpty()) {
      // Shouldn't happen, because UnionDataSource doesn't allow empty unions.
      return Operators.toSequence(
          new NullOperator<T>(fragmentContext)
      );
    }

    List<Operator<T>> inputs = new ArrayList<>();
    for (int i = 0; i < unionDataSource.getDataSources().size(); i++) {
      TableDataSource dataSource = unionDataSource.getDataSources().get(i);
      Query<T> childQuery = Queries
                .withBaseDataSource(query, dataSource)
                 // assign the subqueryId. this will be used to validate that every query servers
                 // have responded per subquery in RetryQueryRunner
                 .withSubQueryId(
                     UnionQueryRunner.generateSubqueryId(
                       query.getSubQueryId(),
                       dataSource.getName(),
                       i
                     )
                  );
      Operator<T> inputOp = Operators.toOperator(
          fragmentContext,
          baseRunner.run(
              queryPlus.withQuery(childQuery),
              responseContext
          )
      );
      inputs.add(inputOp);
    }
    Ordering<T> ordering = query.getResultOrdering();
    Operator<T> mergeOp;
    if (ordering.equals(Ordering.natural())) {
      mergeOp = new ConcatOperator<T>(fragmentContext, inputs);
    } else {
      mergeOp = new MergeOperator<T>(fragmentContext, ordering, inputs);
    }
    return Operators.toSequence(mergeOp);
  }
}
