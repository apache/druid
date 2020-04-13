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

package org.apache.druid.query.datasourcemetadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BySegmentSkippingQueryRunner;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.timeline.LogicalSegment;

import java.util.List;
import java.util.stream.Collectors;

/**
 */
public class DataSourceQueryQueryToolChest
    extends QueryToolChest<Result<DataSourceMetadataResultValue>, DataSourceMetadataQuery>
{
  private static final TypeReference<Result<DataSourceMetadataResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<DataSourceMetadataResultValue>>() {};

  private final GenericQueryMetricsFactory queryMetricsFactory;

  @Inject
  public DataSourceQueryQueryToolChest(GenericQueryMetricsFactory queryMetricsFactory)
  {
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(DataSourceMetadataQuery query, List<T> segments)
  {
    if (segments.size() <= 1) {
      return segments;
    }

    final T max = segments.get(segments.size() - 1);

    return segments.stream()
                   .filter(input -> max != null && input.getInterval().overlaps(max.getTrueInterval()))
                   .collect(Collectors.toList());
  }

  @Override
  public QueryRunner<Result<DataSourceMetadataResultValue>> mergeResults(
      final QueryRunner<Result<DataSourceMetadataResultValue>> runner
  )
  {
    return new BySegmentSkippingQueryRunner<Result<DataSourceMetadataResultValue>>(runner)
    {
      @Override
      protected Sequence<Result<DataSourceMetadataResultValue>> doRun(
          QueryRunner<Result<DataSourceMetadataResultValue>> baseRunner,
          QueryPlus<Result<DataSourceMetadataResultValue>> input,
          ResponseContext context
      )
      {
        DataSourceMetadataQuery query = (DataSourceMetadataQuery) input.getQuery();
        return Sequences.simple(
            query.mergeResults(baseRunner.run(input, context).toList())
        );
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(DataSourceMetadataQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public Function<Result<DataSourceMetadataResultValue>, Result<DataSourceMetadataResultValue>> makePreComputeManipulatorFn(
      DataSourceMetadataQuery query,
      MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<DataSourceMetadataResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy getCacheStrategy(DataSourceMetadataQuery query)
  {
    return null;
  }
}
