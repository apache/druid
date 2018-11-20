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

package org.apache.druid.sql.avatica;

import org.apache.calcite.avatica.metrics.Counter;
import org.apache.calcite.avatica.metrics.Gauge;
import org.apache.calcite.avatica.metrics.Histogram;
import org.apache.calcite.avatica.metrics.Meter;
import org.apache.calcite.avatica.metrics.MetricsSystem;
import org.apache.calcite.avatica.metrics.Timer;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.metrics.AbstractMonitor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class AvaticaMonitor extends AbstractMonitor implements MetricsSystem
{
  private static final Logger log = new Logger(AvaticaMonitor.class);

  private final ConcurrentMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Gauge<?>> gauges = new ConcurrentHashMap<>();

  @Override
  public boolean doMonitor(final ServiceEmitter emitter)
  {
    for (final Map.Entry<String, AtomicLong> entry : counters.entrySet()) {
      final String name = entry.getKey();
      final long value = entry.getValue().getAndSet(0);
      emitter.emit(ServiceMetricEvent.builder().build(fullMetricName(name), value));
    }

    for (Map.Entry<String, Gauge<?>> entry : gauges.entrySet()) {
      final String name = entry.getKey();
      final Object value = entry.getValue().getValue();
      if (value instanceof Number) {
        emitter.emit(ServiceMetricEvent.builder().build(fullMetricName(name), (Number) value));
      } else {
        log.debug("Not emitting gauge[%s] since value[%s] type was[%s].", name, value, value.getClass().getName());
      }
    }

    return true;
  }

  @Override
  public Timer getTimer(final String name)
  {
    final AtomicLong counter = makeCounter(name);
    return new Timer()
    {
      @Override
      public Context start()
      {
        final long start = System.currentTimeMillis();
        final AtomicBoolean closed = new AtomicBoolean();
        return new Context()
        {
          @Override
          public void close()
          {
            if (closed.compareAndSet(false, true)) {
              counter.addAndGet(System.currentTimeMillis() - start);
            }
          }
        };
      }
    };
  }

  @Override
  public Histogram getHistogram(final String name)
  {
    // Return a dummy Histogram. We don't support Histogram metrics.
    return new Histogram()
    {
      @Override
      public void update(int i)
      {
        // Do nothing.
      }

      @Override
      public void update(long l)
      {
        // Do nothing.
      }
    };
  }

  @Override
  public Meter getMeter(final String name)
  {
    final AtomicLong counter = makeCounter(name);
    return new Meter()
    {
      @Override
      public void mark()
      {
        counter.incrementAndGet();
      }

      @Override
      public void mark(long events)
      {
        counter.addAndGet(events);
      }
    };
  }

  @Override
  public Counter getCounter(final String name)
  {
    final AtomicLong counter = makeCounter(name);
    return new Counter()
    {
      @Override
      public void increment()
      {
        counter.incrementAndGet();
      }

      @Override
      public void increment(long n)
      {
        counter.addAndGet(n);
      }

      @Override
      public void decrement()
      {
        counter.decrementAndGet();
      }

      @Override
      public void decrement(long n)
      {
        counter.addAndGet(-n);
      }
    };
  }

  @Override
  public <T> void register(final String name, final Gauge<T> gauge)
  {
    if (gauges.putIfAbsent(name, gauge) != null) {
      log.warn("Ignoring gauge[%s], one with the same name was already registered.", name);
    }
  }

  private AtomicLong makeCounter(final String name)
  {
    counters.putIfAbsent(name, new AtomicLong());
    return counters.get(name);
  }

  private String fullMetricName(final String name)
  {
    return StringUtils.replace(name, "org.apache.calcite.avatica", "avatica").replace('.', '/');
  }
}
