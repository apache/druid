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
import io.druid.common.utils.VMUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceWrapper;
import io.druid.java.util.common.guava.Sequences;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CPUTimeMetricQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> delegate;
  private final QueryToolChest<T, ? extends Query<T>> queryToolChest;
  private final ServiceEmitter emitter;
  private final AtomicLong cpuTimeAccumulator;
  private final boolean report;

  private CPUTimeMetricQueryRunner(
      QueryRunner<T> delegate,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      ServiceEmitter emitter,
      AtomicLong cpuTimeAccumulator,
      boolean report
  )
  {
    if (!VMUtils.isThreadCpuTimeEnabled()) {
      throw new ISE("Cpu time must enabled");
    }
    this.delegate = delegate;
    this.queryToolChest = queryToolChest;
    this.emitter = emitter;
    this.cpuTimeAccumulator = cpuTimeAccumulator == null ? new AtomicLong(0L) : cpuTimeAccumulator;
    this.report = report;
  }


  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final Map<String, Object> responseContext)
  {
    final QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);
    final Sequence<T> baseSequence = delegate.run(queryWithMetrics, responseContext);
    return Sequences.wrap(
        baseSequence,
        new SequenceWrapper()
        {
          @Override
          public <RetType> RetType wrap(Supplier<RetType> sequenceProcessing)
          {
            final long start = VMUtils.getCurrentThreadCpuTime();
            try {
              return sequenceProcessing.get();
            }
            finally {
              cpuTimeAccumulator.addAndGet(VMUtils.getCurrentThreadCpuTime() - start);
            }
          }

          @Override
          public void after(boolean isDone, Throwable thrown) throws Exception
          {
            if (report) {
              final long cpuTimeNs = cpuTimeAccumulator.get();
              if (cpuTimeNs > 0) {
                queryWithMetrics.getQueryMetrics().reportCpuTime(cpuTimeNs).emit(emitter);
              }
            }
          }
        }
    );
  }

  public static <T> QueryRunner<T> safeBuild(
      QueryRunner<T> delegate,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      ServiceEmitter emitter,
      AtomicLong accumulator,
      boolean report
  )
  {
    if (!VMUtils.isThreadCpuTimeEnabled()) {
      return delegate;
    } else {
      return new CPUTimeMetricQueryRunner<>(delegate, queryToolChest, emitter, accumulator, report);
    }
  }
}
