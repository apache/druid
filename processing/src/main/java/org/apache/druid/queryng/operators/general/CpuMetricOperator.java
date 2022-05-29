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

package org.apache.druid.queryng.operators.general;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.MappingOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.utils.JvmUtils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures CPU time of each child operation. Excludes CPU consumed
 * by the downstream consumer of this operator.
 *
 * @see {@link org.apache.druid.query.CPUTimeMetricQueryRunner}
 */
public class CpuMetricOperator<T> extends MappingOperator<T, T>
{
  private final AtomicLong cpuTimeAccumulator;
  private final QueryMetrics<?> queryMetrics;
  private final ServiceEmitter emitter;

  public CpuMetricOperator(
      final FragmentContext context,
      final AtomicLong cpuTimeAccumulator,
      final QueryMetrics<?> queryMetrics,
      final ServiceEmitter emitter,
      final Operator<T> child)
  {
    super(context, child);
    this.cpuTimeAccumulator = cpuTimeAccumulator == null ? new AtomicLong(0L) : cpuTimeAccumulator;
    this.queryMetrics = queryMetrics;
    this.emitter = emitter;
  }

  @Override
  public T next() throws ResultIterator.EofException
  {
    final long startRun = JvmUtils.getCurrentThreadCpuTime();
    try {
      return inputIter.next();
    }
    finally {
      cpuTimeAccumulator.addAndGet(JvmUtils.getCurrentThreadCpuTime() - startRun);
    }
  }

  @Override
  public void close(boolean cascade)
  {
    if (state != State.RUN) {
      state = State.CLOSED;
      return;
    }
    super.close(cascade);
    final long cpuTimeNs = cpuTimeAccumulator.get();
    if (cpuTimeNs > 0) {
      context.responseContext().addCpuNanos(cpuTimeNs);
      queryMetrics.reportCpuTime(cpuTimeNs).emit(emitter);
    }
    OperatorProfile profile = new OperatorProfile("cpu-time");
    profile.add(OperatorProfile.CPU_TIME_NS, cpuTimeNs);
    context.updateProfile(this, profile);
  }
}
