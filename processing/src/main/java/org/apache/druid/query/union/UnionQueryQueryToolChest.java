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
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryExecutor;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.groupby.SupportRowSignature;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.RowSignature.Finalization;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class UnionQueryQueryToolChest extends QueryToolChest<RealUnionResult, UnionQuery>
    implements QueryExecutor<RealUnionResult>
{

  @Override
  public QueryRunner<RealUnionResult> makeQueryRunner(Query<RealUnionResult> query,
      QueryToolChestWarehouse warehouse, QuerySegmentWalker clientQuerySegmentWalker)
  {
    return new UnionQueryRunner((UnionQuery) query, clientQuerySegmentWalker);
  }

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<RealUnionResult> mergeResults(QueryRunner<RealUnionResult> runner)
  {
    throw new UnsupportedOperationException("Not supported");
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
    for (Query<?> q : query.queries) {
      if (q instanceof SupportRowSignature) {
        return ((SupportRowSignature) q).getResultRowSignature(Finalization.UNKNOWN);
      }
    }
    throw DruidException.defensive("None of the subqueries support row signature");
  }

  abstract static class UnionSequenceMaker<T>
  {
    public Optional<Sequence<T>> transform(
        UnionQuery query,
        Sequence<RealUnionResult> resultSequence)
    {
      List<RealUnionResult> results = resultSequence.toList();
      List<Sequence<T>> resultSeqs = new ArrayList<>();

      for (int i = 0; i < results.size(); i++) {
        Query<?> q = query.queries.get(i);
        RealUnionResult realUnionResult = results.get(i);
        Optional<Sequence<T>> queryResults = transformResults(q, realUnionResult.getResults());
        if (!queryResults.isPresent()) {
          return Optional.empty();
        }
        resultSeqs.add(queryResults.get());
      }
      return Optional.of(Sequences.concat(resultSeqs));
    }

    public abstract Optional<Sequence<T>> transformResults(Query<?> q, Sequence<Object> results);
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Sequence<Object[]> resultsAsArrays(
      UnionQuery query,
      Sequence<RealUnionResult> resultSequence)
  {
    return new UnionSequenceMaker<Object[]>()
    {
      @Override
      public Optional<Sequence<Object[]>> transformResults(Query<?> query, Sequence<Object> results)
      {
        QueryToolChest toolChest = warehouse.getToolChest(query);
        return Optional.of(toolChest.resultsAsArrays(query, results));
      }
    }.transform(query, resultSequence).get();
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      UnionQuery query,
      Sequence<RealUnionResult> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes)
  {
    return new UnionSequenceMaker<FrameSignaturePair>()
    {
      @Override
      public Optional<Sequence<FrameSignaturePair>> transformResults(Query<?> query, Sequence<Object> results)
      {
        QueryToolChest toolChest = warehouse.getToolChest(query);
        return toolChest.resultsAsFrames(query, results, memoryAllocatorFactory, useNestedForUnknownTypes);
      }
    }.transform(query, resultSequence);
  }
}
