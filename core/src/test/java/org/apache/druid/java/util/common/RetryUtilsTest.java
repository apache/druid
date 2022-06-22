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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
    Assert.assertEquals("result", "hey", result);
    Assert.assertEquals("count", 1, count.get());
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
    Assert.assertTrue("threw expected exception", threwExpectedException);
    Assert.assertEquals("count", 2, count.get());
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
    Assert.assertEquals("result", "hey", result);
    Assert.assertEquals("count", 2, count.get());
  }

  @Test
  public void testExceptionPredicateNotMatching()
  {
    final AtomicInteger count = new AtomicInteger();
    Assert.assertThrows("uhh", IOException.class, () -> {
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
    Assert.assertEquals("count", 1, count.get());
  }

  @Test(timeout = 5000L)
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

      Assert.assertThrows("sleep interrupted", ExecutionException.class, future::get);
    }
    finally {
      exec.shutdownNow();
    }
  }

  @Test(timeout = 5000L)
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

      Assert.assertThrows("Current thread is interrupted after [2] tries", ExecutionException.class, future::get);
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
    Assert.assertEquals(result, "hey");
    Assert.assertEquals("count", 2, count.get());
  }
}
