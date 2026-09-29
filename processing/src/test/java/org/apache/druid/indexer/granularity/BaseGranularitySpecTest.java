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

package org.apache.druid.indexer.granularity;

import com.google.common.base.Optional;
import org.apache.druid.java.util.common.Intervals;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class BaseGranularitySpecTest
{
  @Test
  public void testConcurrentLookupDuringMaterialization() throws Exception
  {
    final Interval first = Intervals.of("2026-01-01/2026-01-02");
    final Interval second = Intervals.of("2026-01-02/2026-01-03");
    final CountDownLatch partiallyMaterialized = new CountDownLatch(1);
    final CountDownLatch finishMaterialization = new CountDownLatch(1);
    final BaseGranularitySpec.LookupIntervalBuckets buckets = new BaseGranularitySpec.LookupIntervalBuckets(
        () -> new Iterator<>()
        {
          private final Iterator<Interval> delegate = List.of(first, second).iterator();

          @Override
          public boolean hasNext()
          {
            return delegate.hasNext();
          }

          @Override
          public Interval next()
          {
            final Interval interval = delegate.next();
            if (interval.equals(second)) {
              // The first interval has been inserted, but the second has not been returned to the builder.
              partiallyMaterialized.countDown();
              try {
                Assertions.assertTrue(finishMaterialization.await(10, TimeUnit.SECONDS));
              }
              catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
              }
            }
            return interval;
          }
        }
    );
    final FutureTask<TreeSet<Interval>> materialization = new FutureTask<>(buckets::materializedIntervals);
    final FutureTask<Optional<Interval>> lookup = new FutureTask<>(() -> buckets.bucketInterval(second.getStart()));
    final Thread builder = new Thread(materialization, "interval-builder");
    final Thread reader = new Thread(lookup, "interval-reader");
    builder.setDaemon(true);
    reader.setDaemon(true);

    try {
      builder.start();
      Assertions.assertTrue(partiallyMaterialized.await(10, TimeUnit.SECONDS));
      reader.start();

      // Wait for the reader to either contend on initialization or return the baseline's partial lookup.
      // This avoids relying on a sleep to assume that the second lookup has run.
      final long startNanos = System.nanoTime();
      while (!lookup.isDone()
             && reader.getState() != Thread.State.BLOCKED
             && System.nanoTime() - startNanos < TimeUnit.SECONDS.toNanos(10)) {
        Thread.sleep(1);
      }
      Assertions.assertTrue(lookup.isDone() || reader.getState() == Thread.State.BLOCKED, "Reader did not reach lookup");
      finishMaterialization.countDown();

      Assertions.assertEquals(Optional.of(second), lookup.get(10, TimeUnit.SECONDS));
      Assertions.assertEquals(List.of(first, second), List.copyOf(materialization.get(10, TimeUnit.SECONDS)));
      Assertions.assertSame(materialization.get(), buckets.materializedIntervals());
    }
    finally {
      finishMaterialization.countDown();
      builder.join(10_000);
      reader.join(10_000);
    }
  }

  @Test
  public void testLazyMaterialization()
  {
    final AtomicInteger iterations = new AtomicInteger();
    final Interval interval = Intervals.of("2026-01-01/2026-01-02");
    final BaseGranularitySpec.LookupIntervalBuckets buckets = new BaseGranularitySpec.LookupIntervalBuckets(() -> {
      iterations.incrementAndGet();
      return List.of(interval).iterator();
    });

    Assertions.assertEquals(0, iterations.get());
    Assertions.assertEquals(Optional.of(interval), buckets.bucketInterval(interval.getStart()));
    Assertions.assertEquals(List.of(interval), List.copyOf(buckets.materializedIntervals()));
    Assertions.assertEquals(interval, buckets.iterator().next());
    Assertions.assertEquals(1, iterations.get());
  }

  @Test
  public void testEmptyMaterializationIsCached()
  {
    final AtomicInteger iterations = new AtomicInteger();
    final BaseGranularitySpec.LookupIntervalBuckets buckets = new BaseGranularitySpec.LookupIntervalBuckets(() -> {
      iterations.incrementAndGet();
      return Collections.emptyIterator();
    });

    Assertions.assertEquals(0, iterations.get());
    final TreeSet<Interval> intervals = buckets.materializedIntervals();
    Assertions.assertTrue(intervals.isEmpty());
    Assertions.assertSame(intervals, buckets.materializedIntervals());
    Assertions.assertEquals(1, iterations.get());
  }

  @Test
  public void testNullIntervals()
  {
    final BaseGranularitySpec.LookupIntervalBuckets buckets = new BaseGranularitySpec.LookupIntervalBuckets(null);
    Assertions.assertTrue(buckets.materializedIntervals().isEmpty());
    Assertions.assertFalse(buckets.iterator().hasNext());
  }
}
