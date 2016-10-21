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

package io.druid.query;

import com.google.common.base.Function;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.guava.Sequence;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class FluentQueryRunnerBuilder<T>
{
  final QueryToolChest<T, Query<T>> toolChest;

  public FluentQueryRunner create(QueryRunner<T> baseRunner) {
    return new FluentQueryRunner(baseRunner);
  }

  public FluentQueryRunnerBuilder(QueryToolChest<T, Query<T>> toolChest)
  {
    this.toolChest = toolChest;
  }

  public class FluentQueryRunner implements QueryRunner<T>
  {
    private QueryRunner<T> baseRunner;

    public FluentQueryRunner(QueryRunner<T> runner)
    {
      this.baseRunner = runner;
    }

    @Override
    public Sequence<T> run(
        Query<T> query, Map<String, Object> responseContext
    )
    {
      return baseRunner.run(query, responseContext);
    }

    public FluentQueryRunner from(QueryRunner<T> runner) {
      return new FluentQueryRunner(runner);
    }

    public FluentQueryRunner applyPostMergeDecoration()
    {
      return from(
          new FinalizeResultsQueryRunner<T>(
              toolChest.postMergeQueryDecoration(
                  baseRunner
              ),
              toolChest
          )
      );
    }

    public FluentQueryRunner applyPreMergeDecoration()
    {
      return from(
          new UnionQueryRunner<T>(
              toolChest.preMergeQueryDecoration(
                  baseRunner
              )
          )
      );
    }

    public FluentQueryRunner emitCPUTimeMetric(ServiceEmitter emitter)
    {
      return from(
          CPUTimeMetricQueryRunner.safeBuild(
              baseRunner,
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
          )
      );
    }

    public FluentQueryRunner postProcess(PostProcessingOperator<T> postProcessing)
    {
      return from(
          postProcessing != null ?
             postProcessing.postProcess(baseRunner) : baseRunner
      );
    }

    public FluentQueryRunner mergeResults()
    {
      return from(
          toolChest.mergeResults(baseRunner)
      );
    }
  }
}
