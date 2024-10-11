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

package org.apache.druid.query.union;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryExecSomething;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.SupportRowSignature;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Finalization;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RealUnionQueryQueryToolChest extends QueryToolChest<RealUnionResult, UnionQuery>
    implements QueryExecSomething<RealUnionResult>
{

  public QueryRunner<RealUnionResult> executeQuery2(QueryToolChestWarehouse warehouse,
      Query<RealUnionResult> query, QuerySegmentWalker clientQuerySegmentWalker)
  {
    RealUnionQueryRunner2 runner = new RealUnionQueryRunner2(warehouse, (UnionQuery) query, clientQuerySegmentWalker);
    setWarehouse(warehouse);
    return runner;
  }

  public Optional<QueryRunner<RealUnionResult>> executeQuery1(QueryToolChestWarehouse warehouse,
      Query<RealUnionResult> query, QuerySegmentWalker clientQuerySegmentWalker)
  {
    UnionQuery unionQuery = (UnionQuery) query;
    List<QueryRunner> queryRunners = new ArrayList<>();
    for (Query<?> q : unionQuery.queries) {
      QueryRunner subRunner = clientQuerySegmentWalker.executeQuery(q);
      queryRunners.add(subRunner);
    }
    QueryRunner<RealUnionResult> unionRunner = new LocalRealUnionQueryRunner(
        queryRunners
    );
    return Optional.of(unionRunner);
  }

  private static class RealUnionQueryRunner2 implements QueryRunner<RealUnionResult>
  {

    private QueryToolChestWarehouse warehouse;
    private QuerySegmentWalker walker;
    private UnionQuery query;
    private List<QueryRunner> runners;

    public RealUnionQueryRunner2(QueryToolChestWarehouse warehouse, UnionQuery query,
        QuerySegmentWalker walker)
    {
      this.warehouse = warehouse;
      this.query = query;
      this.walker = walker;

      this.runners = makeSubQueryRunners(query);
    }

    private List<QueryRunner> makeSubQueryRunners(UnionQuery unionQuery)
    {
      List<QueryRunner> runners = new ArrayList<>();
      for (Query<?> query : unionQuery.queries) {
        runners.add(query.getRunner(walker));
      }
      return runners;

    }

    @Override
    public Sequence<RealUnionResult> run(QueryPlus<RealUnionResult> queryPlus, ResponseContext responseContext)
    {
      UnionQuery unionQuery = queryPlus.unwrapQuery(UnionQuery.class);

      List<RealUnionResult> seqs = new ArrayList<RealUnionResult>();
      for (int i = 0; i < runners.size(); i++) {
        Query<?> q = unionQuery.queries.get(i);
        QueryRunner r = runners.get(i);
        seqs.add(makeUnionResult(r, queryPlus.withQuery(q), responseContext));
      }
      return Sequences.simple(seqs);
    }

    private <T> RealUnionResult makeUnionResult(QueryRunner runner, QueryPlus<T> withQuery,
        ResponseContext responseContext)
    {
      Sequence<T> seq = runner.run(withQuery, responseContext);
      return new RealUnionResult(seq);
    }
  }

  private static class LocalRealUnionQueryRunner implements QueryRunner<RealUnionResult>
  {

    public LocalRealUnionQueryRunner(List<QueryRunner> queryRunners)
    {

    }

    @Override
    public Sequence<RealUnionResult> run(QueryPlus<RealUnionResult> queryPlus, ResponseContext responseContext)
    {
      return buildSequence();
    }

    Sequence<RealUnionResult> buildSequence()
    {
      return null;
    }

  }

  public void RealUnionQueryQueryToolChest()
  {
    int asd = 1;
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<RealUnionResult> mergeResults(QueryRunner<RealUnionResult> runner)
  {
    if (true) {
      throw new UnsupportedOperationException("Not supported");
    }

    return new RealUnionResultSerializingQueryRunner(
        (queryPlus, responseContext) -> {
          return runner.run(queryPlus, responseContext);
        }
    );
  }

  @Override
  public QueryMetrics<? super UnionQuery> makeMetrics(UnionQuery query)
  {
    return new DefaultQueryMetrics<>();
  }

  @Override
  public Function<RealUnionResult, RealUnionResult> makePreComputeManipulatorFn(
      UnionQuery query,
      MetricManipulationFn fn)
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<RealUnionResult> getResultTypeReference()
  {
    return new TypeReference<RealUnionResult>()
    {
    };
  }

  @Override
  public RowSignature resultArraySignature(UnionQuery query)
  {
    Query<?> q0 = query.queries.get(0);
    if (q0 instanceof SupportRowSignature) {
      return ((SupportRowSignature) q0).getResultRowSignature(Finalization.UNKNOWN);
    }
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Sequence<Object[]> resultsAsArrays(
      UnionQuery query,
      Sequence<RealUnionResult> resultSequence)
  {
    List<RealUnionResult> results = resultSequence.toList();

    List<Sequence<Object[]>> resultSeqs = new ArrayList<Sequence<Object[]>>();

    for (int i = 0; i < results.size(); i++) {
      Query<?> q = query.queries.get(i);
      RealUnionResult realUnionResult = results.get(i);
      resultSeqs.add(resultsAsArrays(q, realUnionResult));
    }

    return Sequences.concat(resultSeqs);
  }

  private <T, QueryType extends Query<T>> Sequence<Object[]> resultsAsArrays(QueryType q,
      RealUnionResult realUnionResult)
  {
    QueryToolChest<T, QueryType> toolChest = warehouse.getToolChest(q);
    return toolChest.resultsAsArrays(q, realUnionResult.getResults());
  }

  @SuppressWarnings("unused")
  private Sequence<Object[]> resultsAsArrays1(Query<?> q, RealUnionResult realUnionResult)
  {
    warehouse.getToolChest(q);
    return null;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      UnionQuery query,
      Sequence<RealUnionResult> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  /**
   * This class exists to serialize the RealUnionResult that are used in this
   * query and make it the return Sequence actually be a Sequence of rows or
   * frames, as the query requires. This is relatively broken in a number of
   * regards, the most obvious of which is that it is going to run counter to
   * the stated class on the Generic of the QueryToolChest. That is, the code
   * makes it look like you are getting a Sequence of RealUnionResult, but, by
   * using this, the query will actually ultimately produce a Sequence of
   * Object[] or Frames. This works because of type Erasure in Java (it's all
   * Object at the end of the day).
   * <p>
   * While it might seem like this will break all sorts of things, the Generic
   * type is actually there more as a type "hint" to make the writing of the
   * ToolChest and Factory and stuff a bit more simple. Any caller of this
   * cannot truly depend on the type anyway other than to just throw it across
   * the wire, so this should just magically work even though it looks like it
   * shouldn't even compile.
   * <p>
   * Not our proudest moment, but we use the tools available to us.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static class RealUnionResultSerializingQueryRunner implements QueryRunner
  {

    private final QueryRunner<RealUnionResult> baseQueryRunner;

    private RealUnionResultSerializingQueryRunner(
        QueryRunner<RealUnionResult> baseQueryRunner)
    {
      this.baseQueryRunner = baseQueryRunner;
    }

    @Override
    public Sequence run(
        QueryPlus queryPlus,
        ResponseContext responseContext)
    {
      throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Translates Sequence of RACs to a Sequence of Object[]
     */
    private static Sequence asRows(final Sequence<RealUnionResult> baseSequence, final WindowOperatorQuery query)
    {
      throw new UnsupportedOperationException("Not supported");
    }

    /**
     * Translates a sequence of RACs to a Sequence of Frames
     */
    private static Sequence asFrames(final Sequence<RealUnionResult> baseSequence)
    {
      throw new UnsupportedOperationException("Not supported");
    }
  }
}
