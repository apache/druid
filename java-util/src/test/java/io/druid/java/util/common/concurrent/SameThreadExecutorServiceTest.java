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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SameThreadExecutorServiceTest
{
  private final SameThreadExecutorService service = new SameThreadExecutorService();

  @Test
  public void timeoutAndShutdownTest() throws Exception
  {
    Assert.assertFalse(service.awaitTermination(10, TimeUnit.MILLISECONDS));
  }

  @Test
  public void runsTasks() throws Exception
  {
    final CountDownLatch finished = new CountDownLatch(1);
    service.submit(finished::countDown);
    finished.await();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void cannotShutdown() throws Exception
  {
    service.shutdown();
  }

  @Test(expected = ExecutionException.class)
  public void exceptionsCaught() throws Exception
  {
    service.submit(() -> {
      throw new RuntimeException();
    }).get();
  }
}
