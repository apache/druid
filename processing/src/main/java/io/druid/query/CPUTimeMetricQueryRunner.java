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
import com.google.common.base.Preconditions;

import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.common.utils.VMUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CPUTimeMetricQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> delegate;
  private final Function<Query<T>, ServiceMetricEvent.Builder> builderFn;
  private final ServiceEmitter emitter;
  private final AtomicLong cpuTimeAccumulator;
  private final boolean report;

  private CPUTimeMetricQueryRunner(
      QueryRunner<T> delegate,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      ServiceEmitter emitter,
      AtomicLong cpuTimeAccumulator,
      boolean report
  )
  {
    if (!VMUtils.isThreadCpuTimeEnabled()) {
      throw new ISE("Cpu time must enabled");
    }
    this.delegate = delegate;
    this.builderFn = builderFn;
    this.emitter = emitter;
    this.cpuTimeAccumulator = cpuTimeAccumulator == null ? new AtomicLong(0L) : cpuTimeAccumulator;
    this.report = report;
  }


  @Override
  public Sequence<T> run(
      final Query<T> query, final Map<String, Object> responseContext
  )
  {
    final Sequence<T> baseSequence = delegate.run(query, responseContext);
    return Sequences.withBaggage(
        new Sequence<T>()
        {
          @Override
          public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
          {
            final long start = VMUtils.getCurrentThreadCpuTime();
            try {
              return baseSequence.accumulate(initValue, accumulator);
            }
            finally {
              cpuTimeAccumulator.addAndGet(VMUtils.getCurrentThreadCpuTime() - start);
            }
          }

          @Override
          public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
          {
            final long start = VMUtils.getCurrentThreadCpuTime();
            final Yielder<OutType> delegateYielder;
            try {
              delegateYielder = baseSequence.toYielder(initValue, accumulator);
            }
            finally {
              cpuTimeAccumulator.addAndGet(VMUtils.getCurrentThreadCpuTime() - start);
            }
            return new Yielder<OutType>()
            {
              @Override
              public OutType get()
              {
                final long start = VMUtils.getCurrentThreadCpuTime();
                try {
                  return delegateYielder.get();
                }
                finally {
                  cpuTimeAccumulator.addAndGet(VMUtils.getCurrentThreadCpuTime() - start);
                }
              }

              @Override
              public Yielder<OutType> next(OutType initValue)
              {
                final long start = VMUtils.getCurrentThreadCpuTime();
                try {
                  return delegateYielder.next(initValue);
                }
                finally {
                  cpuTimeAccumulator.addAndGet(VMUtils.getCurrentThreadCpuTime() - start);
                }
              }

              @Override
              public boolean isDone()
              {
                return delegateYielder.isDone();
              }

              @Override
              public void close() throws IOException
              {
                delegateYielder.close();
              }
            };
          }
        },
        new Closeable()
        {
          @Override
          public void close()
          {
            if (report) {
              final long cpuTime = cpuTimeAccumulator.get();
              if (cpuTime > 0) {
                final ServiceMetricEvent.Builder builder = Preconditions.checkNotNull(builderFn.apply(query));
                emitter.emit(builder.build("query/cpu/time", cpuTimeAccumulator.get() / 1000));
              }
            }
          }
        }
    );
  }

  public static <T> QueryRunner<T> safeBuild(
      QueryRunner<T> delegate,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      ServiceEmitter emitter,
      AtomicLong accumulator,
      boolean report
  )
  {
    if (!VMUtils.isThreadCpuTimeEnabled()) {
      return delegate;
    } else {
      return new CPUTimeMetricQueryRunner<>(delegate, builderFn, emitter, accumulator, report);
    }
  }
}
