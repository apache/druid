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

package io.druid.java.util.common.concurrent;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A simple class that implements the ExecutorService interface, but runs the code on a call to submit
 */
public class SameThreadExecutorService extends AbstractExecutorService
{
  // Use io.druid.java.util.common.concurrent.Execs#sameThreadExecutor()
  SameThreadExecutorService()
  {

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
    return false;
  }

  @Override
  public boolean isTerminated()
  {
    return false;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    final long nanos = TimeUnit.NANOSECONDS.convert(timeout, unit);
    final long millis = TimeUnit.MILLISECONDS.convert(timeout, unit);
    final int sleepNanos = (int) (nanos - millis * 1_000_000L);
    Thread.sleep(millis, sleepNanos);
    return false;
  }

  @Override
  public void execute(Runnable command)
  {
    command.run();
  }
}
