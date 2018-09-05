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

package org.apache.druid.guice;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

public class ForkJoinPoolProviderTest
{
  private static final String GOOD_NAME_FORMAT = "test-fjp-%d";
  private static final ForkJoinPoolProvider FORK_JOIN_POOL_PROVIDER = new ForkJoinPoolProvider(GOOD_NAME_FORMAT);

  @Test
  public void testThreadThrowsException() throws InterruptedException
  {
    final ForkJoinPool fjp = FORK_JOIN_POOL_PROVIDER.get();
    final RuntimeException re = new RuntimeException("test exception");
    try {
      fjp.submit(() -> {
        throw re;
      }).get();
    }
    catch (ExecutionException e) {
      if (!re.equals(e.getCause().getCause())) {
        throw new RuntimeException("Unexpected exception", e);
      }
      return;
    }
    Assert.fail("Should have thrown exception");
  }

  @Test
  public void testThreadSwallowsException()
  {
    final ForkJoinPool fjp = FORK_JOIN_POOL_PROVIDER.get();
    final RuntimeException re = new RuntimeException("test exception");
    fjp.execute(() -> {
      throw re;
    });
  }
}
