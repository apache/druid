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
    implements QueryExecSomething<RealUnionResult>
{

  public QueryRunner<RealUnionResult> makeQueryRunner(QueryToolChestWarehouse warehouse,
      Query<RealUnionResult> query, QuerySegmentWalker clientQuerySegmentWalker)
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
}
