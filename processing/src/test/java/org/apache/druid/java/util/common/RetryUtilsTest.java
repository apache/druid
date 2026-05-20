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

package org.apache.druid.java.util.common;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.druid.java.util.RetryableException;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RetryUtilsTest
{
  private static final Predicate<Throwable> IS_TRANSIENT =
      e -> e instanceof IOException && e.getMessage().equals("what");

  @Test
  public void testImmediateSuccess() throws Exception
  {
    final AtomicInteger count = new AtomicInteger();
    final String result = RetryUtils.retry(
        () -> {
          count.incrementAndGet();
          return "hey";
        },
        IS_TRANSIENT,
        2
    );
    Assertions.assertEquals("hey", result, "result");
    Assertions.assertEquals(1, count.get(), "count");
  }

  @Test
  public void testEventualFailure() throws Exception
  {
    final AtomicInteger count = new AtomicInteger();
    boolean threwExpectedException = false;
    try {
      RetryUtils.retry(
          () -> {
            count.incrementAndGet();
            throw new IOException("what");
          },
          IS_TRANSIENT,
          2
      );
    }
    catch (IOException e) {
      threwExpectedException = e.getMessage().equals("what");
    }
    Assertions.assertTrue(threwExpectedException, "threw expected exception");
    Assertions.assertEquals(2, count.get(), "count");
  }

  @Test
  public void testEventualSuccess() throws Exception
  {
    final AtomicInteger count = new AtomicInteger();
    final String result = RetryUtils.retry(
        () -> {
          if (count.incrementAndGet() >= 2) {
            return "hey";
          } else {
            throw new IOException("what");
          }
        },
        IS_TRANSIENT,
        3
    );
    Assertions.assertEquals("hey", result, "result");
    Assertions.assertEquals(2, count.get(), "count");
  }

  @Test
  public void testExceptionPredicateNotMatching()
  {
    final AtomicInteger count = new AtomicInteger();
    Assertions.assertThrows(IOException.class, () -> {
      RetryUtils.retry(
          () -> {
            if (count.incrementAndGet() >= 2) {
              return "hey";
            } else {
              throw new IOException("uhh");
            }
          },
          IS_TRANSIENT,
          3
      );
    });
    Assertions.assertEquals(1, count.get(), "count");
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInterruptWhileSleepingBetweenTries()
  {
    ExecutorService exec = Execs.singleThreaded("test-interrupt");
    try {
      MutableInt count = new MutableInt(0);
      Future<Object> future = exec.submit(() -> RetryUtils.retry(
          () -> {
            if (count.incrementAndGet() > 1) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Test exception");
          },
          Predicates.alwaysTrue(),
          2,
          Integer.MAX_VALUE
      ));

      Assertions.assertThrows(ExecutionException.class, future::get);
    }
    finally {
      exec.shutdownNow();
    }
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInterruptRetryLoop()
  {
    ExecutorService exec = Execs.singleThreaded("test-interrupt");
    try {
      MutableInt count = new MutableInt(0);
      Future<Object> future = exec.submit(() -> RetryUtils.retry(
          () -> {
            if (count.incrementAndGet() > 1) {
              Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Test exception");
          },
          Predicates.alwaysTrue(),
          2,
          Integer.MAX_VALUE,
          null,
          null,
          true
      ));

      Assertions.assertThrows(ExecutionException.class, future::get);
    }
    finally {
      exec.shutdownNow();
    }
  }

  @Test
  public void testExceptionPredicateForRetryableException() throws Exception
  {
    final AtomicInteger count = new AtomicInteger();
    String result = RetryUtils.retry(
        () -> {
          if (count.incrementAndGet() >= 2) {
            return "hey";
          } else {
            throw new RetryableException(new RuntimeException("uhh"));
          }
        },
        e -> e instanceof RetryableException,
        3
    );
    Assertions.assertEquals(result, "hey");
    Assertions.assertEquals(2, count.get(), "count");
  }

  @Test
  public void testNextRetrySleepMillis()
  {
    long totalSleepTimeMillis = 0;

    for (int i = 1; i < 7; ++i) {
      final long nextSleepMillis = RetryUtils.nextRetrySleepMillis(i);
      Assertions.assertTrue(nextSleepMillis >= 0);
      Assertions.assertTrue(nextSleepMillis <= (2_000 * Math.pow(2, i - 1)));

      totalSleepTimeMillis += nextSleepMillis;
    }

    for (int i = 7; i < 11; ++i) {
      final long nextSleepMillis = RetryUtils.nextRetrySleepMillis(i);
      Assertions.assertTrue(nextSleepMillis >= 0);
      Assertions.assertTrue(nextSleepMillis <= 120_000);

      totalSleepTimeMillis += nextSleepMillis;
    }

    Assertions.assertTrue(totalSleepTimeMillis > 3 * 60_000);
    Assertions.assertTrue(totalSleepTimeMillis < 8 * 60_000);
  }
}
