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
import com.google.common.base.Supplier;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
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
  private final Function<Query<T>, ServiceMetricEvent.Builder> builderFn;
  private final QueryRunner<T> queryRunner;
  private final long creationTime;
  private final String metricName;
  private final Map<String, String> userDimensions;

  private MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner,
      long creationTime,
      String metricName,
      Map<String, String> userDimensions
  )
  {
    this.emitter = emitter;
    this.builderFn = builderFn;
    this.queryRunner = queryRunner;
    this.creationTime = creationTime;
    this.metricName = metricName;
    this.userDimensions = userDimensions;
  }

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner,
      String metricName,
      Map<String, String> userDimensions
  )
  {
    this(emitter, builderFn, queryRunner, -1, metricName, userDimensions);
  }

  public MetricsEmittingQueryRunner<T> withWaitMeasuredFromNow()
  {
    return new MetricsEmittingQueryRunner<T>(
        emitter,
        builderFn,
        queryRunner,
        System.currentTimeMillis(),
        metricName,
        userDimensions
    );
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final ServiceMetricEvent.Builder builder = builderFn.apply(query);

    for (Map.Entry<String, String> userDimension : userDimensions.entrySet()) {
      builder.setDimension(userDimension.getKey(), userDimension.getValue());
    }

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
          private long startTime;

          @Override
          public void before()
          {
            startTime = System.currentTimeMillis();
          }

          @Override
          public void after(boolean isDone, Throwable thrown)
          {
            if (thrown != null) {
              builder.setDimension(DruidMetrics.STATUS, "failed");
            } else if (!isDone) {
              builder.setDimension(DruidMetrics.STATUS, "short");
            }
            long timeTaken = System.currentTimeMillis() - startTime;
            emitter.emit(builder.build(metricName, timeTaken));

            if (creationTime > 0) {
              emitter.emit(builder.build("query/wait/time", startTime - creationTime));
            }
          }
        }
    );
  }
}
