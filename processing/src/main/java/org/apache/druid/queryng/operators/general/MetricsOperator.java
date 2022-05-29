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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.queryng.Timer;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.WrappingOperator;

import java.util.function.ObjLongConsumer;

/**
 * Operator to emit runtime metrics. This is a temporary solution: these
 * metrics are better emitted at the top of the stack by the fragment
 * runner to avoid the per-row overhead.
 *
 * @see {@link org.apache.druid.query.MetricsEmittingQueryRunner}
 */
public class MetricsOperator<T> extends WrappingOperator<T>
{
  private static final Logger log = new Logger(MetricsOperator.class);

  private final Timer waitTimer;
  private final ServiceEmitter emitter;
  private final QueryMetrics<?> queryMetrics;
  private final ObjLongConsumer<QueryMetrics<?>> reportMetric;
  private final Timer runTimer = Timer.create();

  public MetricsOperator(
      final FragmentContext context,
      final ServiceEmitter emitter,
      final QueryMetrics<?> queryMetrics,
      final ObjLongConsumer<QueryMetrics<?>> reportMetric,
      final Timer waitTimer,
      final Operator<T> child
  )
  {
    super(context, child);
    this.emitter = emitter;
    this.queryMetrics = queryMetrics;
    this.reportMetric = reportMetric;
    this.waitTimer = waitTimer;
  }

  @Override
  public void onOpen()
  {
    runTimer.start();
  }

  @Override
  public void onClose()
  {
    if (context.state() == FragmentContext.State.FAILED) {
      queryMetrics.status("failed");
    }

    reportMetric.accept(queryMetrics, runTimer.get());
    // Wait time is reported only in the outer-most metric operator.
    if (waitTimer != null) {
      queryMetrics.reportWaitTime(waitTimer.get() - runTimer.get());
    }
    try {
      queryMetrics.emit(emitter);
    }
    catch (Exception e) {
      // Query should not fail because of emitter failure. Swallow the exception.
      log.error("Failure while trying to emit [%s] with stacktrace [%s]", emitter.toString(), e);
    }
  }
}
