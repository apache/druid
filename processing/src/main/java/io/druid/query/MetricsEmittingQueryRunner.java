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

import com.google.common.base.Supplier;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.java.util.common.guava.LazySequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceWrapper;
import io.druid.java.util.common.guava.Sequences;

import java.util.Map;

/**
 */
public class MetricsEmittingQueryRunner<T> implements QueryRunner<T>
{
  private final ServiceEmitter emitter;
  private final QueryToolChest<?, ? super Query<T>> queryToolChest;
  private final QueryRunner<T> queryRunner;
  private final long creationTimeNs;
  private final QueryMetric metric;
  private final Map<String, String> userDimensions;

  private MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      QueryToolChest<?, ? super Query<T>> queryToolChest,
      QueryRunner<T> queryRunner,
      long creationTimeNs,
      QueryMetric metric,
      Map<String, String> userDimensions
  )
  {
    this.emitter = emitter;
    this.queryToolChest = queryToolChest;
    this.queryRunner = queryRunner;
    this.creationTimeNs = creationTimeNs;
    this.metric = metric;
    this.userDimensions = userDimensions;
  }

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      QueryToolChest<?, ? super Query<T>> queryToolChest,
      QueryRunner<T> queryRunner,
      QueryMetric metric,
      Map<String, String> userDimensions
  )
  {
    this(emitter, queryToolChest, queryRunner, -1, metric, userDimensions);
  }

  public MetricsEmittingQueryRunner<T> withWaitMeasuredFromNow()
  {
    return new MetricsEmittingQueryRunner<>(
        emitter,
        queryToolChest,
        queryRunner,
        System.nanoTime(),
        metric,
        userDimensions
    );
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final QueryMetrics<? super Query<T>> queryMetrics = queryToolChest.makeMetrics(query);

    queryMetrics.userDimensions(userDimensions);

    return Sequences.wrap(
        // Use LazySequence because want to account execution time of queryRunner.run() (it prepares the underlying
        // Sequence) as part of the reported query time, i. e. we want to execute queryRunner.run() after
        // `startTime = System.currentTimeMillis();` (see below).
        new LazySequence<>(new Supplier<Sequence<T>>()
        {
          @Override
          public Sequence<T> get()
          {
            return queryRunner.run(query, responseContext);
          }
        }),
        new SequenceWrapper()
        {
          private long startTimeNs;

          @Override
          public void before()
          {
            startTimeNs = System.nanoTime();
          }

          @Override
          public void after(boolean isDone, Throwable thrown)
          {
            if (thrown != null) {
              queryMetrics.status("failed");
            } else if (!isDone) {
              queryMetrics.status("short");
            }
            long timeTakenNs = System.nanoTime() - startTimeNs;
            metric.apply(queryMetrics, timeTakenNs);

            if (creationTimeNs > 0) {
              queryMetrics.waitTime(startTimeNs - creationTimeNs);
            }
            queryMetrics.emit(emitter);
          }
        }
    );
  }
}
