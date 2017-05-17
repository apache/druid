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
package io.druid.query.scan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.aggregation.MetricManipulationFn;

import java.util.Map;

public class ScanQueryQueryToolChest extends QueryToolChest<ScanResultValue, ScanQuery>
{
  private static final TypeReference<ScanResultValue> TYPE_REFERENCE = new TypeReference<ScanResultValue>()
  {
  };

  private final GenericQueryMetricsFactory queryMetricsFactory;

  @Inject
  public ScanQueryQueryToolChest(GenericQueryMetricsFactory queryMetricsFactory)
  {
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<ScanResultValue> mergeResults(final QueryRunner<ScanResultValue> runner)
  {
    return new QueryRunner<ScanResultValue>()
    {
      @Override
      public Sequence<ScanResultValue> run(
          final QueryPlus<ScanResultValue> queryPlus, final Map<String, Object> responseContext
      )
      {
        ScanQuery scanQuery = (ScanQuery) queryPlus.getQuery();
        if (scanQuery.getLimit() == Long.MAX_VALUE) {
          return runner.run(queryPlus, responseContext);
        }
        return new BaseSequence<>(
            new BaseSequence.IteratorMaker<ScanResultValue, ScanQueryLimitRowIterator>()
            {
              @Override
              public ScanQueryLimitRowIterator make()
              {
                return new ScanQueryLimitRowIterator(runner, queryPlus, responseContext);
              }

              @Override
              public void cleanup(ScanQueryLimitRowIterator iterFromMake)
              {
                CloseQuietly.close(iterFromMake);
              }
            }
        );
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(ScanQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public Function<ScanResultValue, ScanResultValue> makePreComputeManipulatorFn(
      ScanQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<ScanResultValue> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<ScanResultValue> preMergeQueryDecoration(final QueryRunner<ScanResultValue> runner)
  {
    return new QueryRunner<ScanResultValue>()
    {
      @Override
      public Sequence<ScanResultValue> run(
          QueryPlus<ScanResultValue> queryPlus, Map<String, Object> responseContext
      )
      {
        ScanQuery scanQuery = (ScanQuery) queryPlus.getQuery();
        if (scanQuery.getDimensionsFilter() != null) {
          scanQuery = scanQuery.withDimFilter(scanQuery.getDimensionsFilter().optimize());
          queryPlus = queryPlus.withQuery(scanQuery);
        }
        return runner.run(queryPlus, responseContext);
      }
    };
  }
}
