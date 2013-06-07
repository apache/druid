/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.query;

import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MetricsEmittingExecutorService extends DelegatingExecutorService
{
  private final ExecutorService base;
  private final ServiceEmitter emitter;
  private final ServiceMetricEvent.Builder metricBuilder;

  public MetricsEmittingExecutorService(
      ExecutorService base,
      ServiceEmitter emitter,
      ServiceMetricEvent.Builder metricBuilder
  )
  {
    super(base);

    this.base = base;
    this.emitter = emitter;
    this.metricBuilder = metricBuilder;
  }

  @Override
  public void execute(Runnable runnable)
  {
    emitMetrics();
    base.execute(runnable);
  }

  private void emitMetrics()
  {
    if (base instanceof ThreadPoolExecutor) {
      emitter.emit(metricBuilder.build("exec/backlog", ((ThreadPoolExecutor) base).getQueue().size()));
    }
  }
}
