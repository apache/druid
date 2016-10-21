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

package io.druid.java.util.common.guava;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class ExecutorExecutingSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    TestExecutor exec = new TestExecutor();
    final List<Integer> vals = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13);
    ExecutorExecutingSequence<Integer> seq = new ExecutorExecutingSequence<>(Sequences.simple(vals), exec);

    SequenceTestHelper.testAccumulation("", seq, vals);
    Assert.assertEquals(1, exec.getTimesCalled());

    exec.reset();

    SequenceTestHelper.testYield("", 3, seq, vals);
    Assert.assertEquals(5, exec.getTimesCalled());
  }

  @Test
  public void testSanity2() throws Exception
  {
    TestExecutor exec = new TestExecutor();
    final List<Integer> vals = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
    ExecutorExecutingSequence<Integer> seq = new ExecutorExecutingSequence<>(Sequences.simple(vals), exec);

    SequenceTestHelper.testAccumulation("", seq, vals);
    Assert.assertEquals(1, exec.getTimesCalled());

    exec.reset();

    SequenceTestHelper.testYield("", 3, seq, vals);
    Assert.assertEquals(6, exec.getTimesCalled());
  }

  public static class TestExecutor implements ExecutorService
  {
    int timesCalled = 0;

    public int getTimesCalled()
    {
      return timesCalled;
    }

    public void reset()
    {
      timesCalled = 0;
    }

    @Override
    public void shutdown()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Runnable> shutdownNow()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShutdown()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTerminated()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(Callable<T> task)
    {
      ++timesCalled;
      try {
        return Futures.immediateCheckedFuture(task.call());
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> submit(Runnable task)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable command)
    {
      throw new UnsupportedOperationException();
    }
  }
}
