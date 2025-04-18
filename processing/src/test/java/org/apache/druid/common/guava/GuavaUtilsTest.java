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

package org.apache.druid.common.guava;

import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public class GuavaUtilsTest
{
  enum MyEnum
  {
    ONE,
    TWO,
    BUCKLE_MY_SHOE
  }

  @Test
  public void testParseLong()
  {
    Assert.assertNull(Longs.tryParse("+100"));
    Assert.assertNull(GuavaUtils.tryParseLong(""));
    Assert.assertNull(GuavaUtils.tryParseLong(null));
    Assert.assertNull(GuavaUtils.tryParseLong("+"));
    Assert.assertNull(GuavaUtils.tryParseLong("++100"));
    Assert.assertEquals((Object) Long.parseLong("+100"), GuavaUtils.tryParseLong("+100"));
    Assert.assertEquals((Object) Long.parseLong("-100"), GuavaUtils.tryParseLong("-100"));
    Assert.assertNotEquals(new Long(100), GuavaUtils.tryParseLong("+101"));
  }

  @Test
  public void testGetEnumIfPresent()
  {
    Assert.assertEquals(MyEnum.ONE, GuavaUtils.getEnumIfPresent(MyEnum.class, "ONE"));
    Assert.assertEquals(MyEnum.TWO, GuavaUtils.getEnumIfPresent(MyEnum.class, "TWO"));
    Assert.assertEquals(MyEnum.BUCKLE_MY_SHOE, GuavaUtils.getEnumIfPresent(MyEnum.class, "BUCKLE_MY_SHOE"));
    Assert.assertEquals(null, GuavaUtils.getEnumIfPresent(MyEnum.class, "buckle_my_shoe"));
  }

  @Test
  public void testCancelAll()
  {
    int tasks = 3;
    ExecutorService service = Execs.multiThreaded(tasks, "GuavaUtilsTest-%d");
    ListeningExecutorService exc = MoreExecutors.listeningDecorator(service);
    try {
      //a flag what time to throw exception.
      AtomicBoolean someoneFailed = new AtomicBoolean(false);
      List<CountDownLatch> latches = new ArrayList<>(tasks);
      Function<Integer, List<ListenableFuture<Object>>> function = (taskCount) -> {
        List<ListenableFuture<Object>> futures = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
          final CountDownLatch latch = new CountDownLatch(1);
          latches.add(latch);
          ListenableFuture<Object> future = exc.submit(new Callable<>()
          {
            @Override
            public Object call() throws RuntimeException, InterruptedException
            {
              latch.await(60, TimeUnit.SECONDS);
              if (someoneFailed.compareAndSet(false, true)) {
                throw new RuntimeException("This exception simulates an error");
              }
              return null;
            }
          });
          futures.add(future);
        }
        return futures;
      };

      List<ListenableFuture<Object>> futures = function.apply(tasks);
      Assert.assertEquals(tasks, futures.stream().filter(f -> !f.isDone()).count());
      // "release" the last tasks, which will cause it to fail as someoneFailed will still be false
      latches.get(tasks - 1).countDown();

      ListenableFuture<List<Object>> future = Futures.allAsList(futures);

      ExecutionException thrown = Assert.assertThrows(
          ExecutionException.class,
          future::get
      );
      Assert.assertEquals("This exception simulates an error", thrown.getCause().getMessage());
      GuavaUtils.cancelAll(true, future, futures);
      Assert.assertEquals(0, futures.stream().filter(f -> !f.isDone()).count());
      for (CountDownLatch latch : latches) {
        latch.countDown();
      }
    }
    finally {
      exc.shutdownNow();
      service.shutdownNow();
    }
  }
}
