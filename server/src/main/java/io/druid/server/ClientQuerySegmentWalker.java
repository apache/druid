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

package io.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.guava.LazySequence;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.CachingClusteredClient;
import io.druid.query.FluentQueryRunnerBuilder;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RetryQueryRunner;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.query.SegmentDescriptor;
import io.druid.query.UnionAllQuery;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final ServiceEmitter emitter;
  private final CachingClusteredClient baseClient;
  private final QueryToolChestWarehouse warehouse;
  private final RetryQueryRunnerConfig retryConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient baseClient,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper
  )
  {
    this.emitter = emitter;
    this.baseClient = baseClient;
    this.warehouse = warehouse;
    this.retryConfig = retryConfig;
    this.objectMapper = objectMapper;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner(query, true);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query, true);
  }

  private <T> QueryRunner<T> makeRunner(Query<T> query, boolean finalize)
  {
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    FluentQueryRunnerBuilder<T> builder = new FluentQueryRunnerBuilder<>(toolChest);

    QueryRunner<T> runner;
    if (query instanceof UnionAllQuery) {
      UnionAllQuery<T> union = (UnionAllQuery) query;
      runner = getUnionQueryRunner(union.getQueries(), union.isSortOnUnion());
    } else {
      runner = builder.create(new RetryQueryRunner<>(baseClient, toolChest, retryConfig, objectMapper))
                      .applyPreMergeDecoration()
                      .mergeResults()
                      .applyPostMergeDecoration();
    }
    return finalize ? finalize(query, runner, toolChest) : runner;
  }

  private <T> QueryRunner<T> finalize(Query<T> query, QueryRunner<T> runner, QueryToolChest<T, Query<T>> toolChest)
  {
    FluentQueryRunnerBuilder.FluentQueryRunner fluentRunner;
    if ((runner instanceof FluentQueryRunnerBuilder.FluentQueryRunner)) {
      fluentRunner = (FluentQueryRunnerBuilder.FluentQueryRunner) runner;
    } else {
      fluentRunner = new FluentQueryRunnerBuilder(toolChest).create(runner);
    }

    final PostProcessingOperator<T> postProcessing = objectMapper.convertValue(
        query.<String>getContextValue("postProcessing"),
        new TypeReference<PostProcessingOperator<T>>()
        {
        }
    );
    return fluentRunner.emitCPUTimeMetric(emitter).postProcess(postProcessing);
  }

  private <T> QueryRunner<T> getUnionQueryRunner(final List<Query<T>> targets, final boolean sortOnUnion)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence run(Query query, final Map responseContext)
      {
        final Sequence<Sequence> sequences = Sequences.simple(
            Lists.transform(
                targets,
                new Function<Query, Sequence>()
                {
                  @Override
                  public Sequence apply(final Query query)
                  {
                    return new LazySequence(
                        new Supplier<Sequence>()
                        {
                          @Override
                          public Sequence get()
                          {
                            return makeRunner(query, false).run(query, responseContext);
                          }
                        }
                    );
                  }
                }
            )
        );

        if (sortOnUnion) {
          return new MergeSequence(query.getResultOrdering(), sequences);
        }
        return Sequences.concat(sequences);
      }
    };
  }
}
