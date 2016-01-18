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
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.CachingClusteredClient;
import io.druid.query.CPUTimeMetricQueryRunner;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RetryQueryRunner;
import io.druid.query.RetryQueryRunnerConfig;
import io.druid.query.SegmentDescriptor;
import io.druid.query.UnionQueryRunner;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
    return makeRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query);
  }

  private <T> QueryRunner<T> makeRunner(final Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    final QueryRunner<T> baseRunner = CPUTimeMetricQueryRunner.safeBuild(
        new FinalizeResultsQueryRunner<T>(
            toolChest.postMergeQueryDecoration(
                toolChest.mergeResults(
                    new UnionQueryRunner<T>(
                        toolChest.preMergeQueryDecoration(
                            new RetryQueryRunner<T>(
                                baseClient,
                                toolChest,
                                retryConfig,
                                objectMapper
                            )
                        )
                    )
                )
            ),
            toolChest
        ),
        new Function<Query<T>, ServiceMetricEvent.Builder>()
        {
          @Nullable
          @Override
          public ServiceMetricEvent.Builder apply(Query<T> tQuery)
          {
            return toolChest.makeMetricBuilder(tQuery);
          }
        },
        emitter,
        new AtomicLong(0L),
        true
    );

    final Map<String, Object> context = query.getContext();
    PostProcessingOperator<T> postProcessing = null;
    if (context != null) {
      postProcessing = objectMapper.convertValue(
          context.get("postProcessing"),
          new TypeReference<PostProcessingOperator<T>>()
          {
          }
      );
    }

    return postProcessing != null ?
           postProcessing.postProcess(baseRunner) : baseRunner;
  }
}
