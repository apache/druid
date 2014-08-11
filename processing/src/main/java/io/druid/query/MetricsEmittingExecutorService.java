/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.query;

import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.concurrent.Callable;

public class MetricsEmittingExecutorService extends ForwardingListeningExecutorService
{
  private final ListeningExecutorService delegate;
  private final ServiceEmitter emitter;
  private final ServiceMetricEvent.Builder metricBuilder;

  public MetricsEmittingExecutorService(
      ListeningExecutorService delegate,
      ServiceEmitter emitter,
      ServiceMetricEvent.Builder metricBuilder
  )
  {
    super();

    this.delegate = delegate;
    this.emitter = emitter;
    this.metricBuilder = metricBuilder;
  }

  @Override
  protected ListeningExecutorService delegate()
  {
    return delegate;
  }

  @Override
  public <T> ListenableFuture<T> submit(Callable<T> tCallable)
  {
    emitMetrics();
    return delegate.submit(tCallable);
  }

  @Override
  public void execute(Runnable runnable)
  {
    emitMetrics();
    delegate.execute(runnable);
  }

  private void emitMetrics()
  {
    if (delegate instanceof PrioritizedExecutorService) {
      emitter.emit(metricBuilder.build("exec/backlog", ((PrioritizedExecutorService) delegate).getQueueSize()));
    }
  }
}
