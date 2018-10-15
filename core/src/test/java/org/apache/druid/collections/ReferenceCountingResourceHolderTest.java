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

package org.apache.druid.collections;

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReferenceCountingResourceHolderTest
{
  @Test
  public void testIdiomaticUsage()
  {
    // Smoke testing
    for (int i = 0; i < 100; i++) {
      runIdiomaticUsage();
    }
  }

  private void runIdiomaticUsage()
  {
    final AtomicBoolean released = new AtomicBoolean(false);
    final ReferenceCountingResourceHolder<Closeable> resourceHolder = makeReleasingHandler(released);
    List<Thread> threads = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      Thread thread = new Thread(() -> {
        try (Releaser r = resourceHolder.increment()) {
          try {
            Thread.sleep(1);
          }
          catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });
      thread.start();
      threads.add(thread);
    }
    for (Thread thread : threads) {
      try {
        thread.join();
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    Assert.assertFalse(released.get());
    resourceHolder.close();
    Assert.assertTrue(released.get());
  }

  private ReferenceCountingResourceHolder<Closeable> makeReleasingHandler(final AtomicBoolean released)
  {
    return ReferenceCountingResourceHolder
          .fromCloseable((Closeable) new Closeable()
          {
            @Override
            public void close()
            {
              released.set(true);
            }
          });
  }

  @Test(timeout = 60_000L)
  public void testResourceHandlerClearedByJVM() throws InterruptedException
  {
    long initialLeakedResources = ReferenceCountingResourceHolder.leakedResources();
    final AtomicBoolean released = new AtomicBoolean(false);
    makeReleasingHandler(released); // Don't store the handler in a variable and don't close it, the object leaked
    verifyCleanerRun(released, initialLeakedResources);
  }

  @Test(timeout = 60_000L)
  public void testResourceHandlerWithReleaserClearedByJVM() throws InterruptedException
  {
    long initialLeakedResources = ReferenceCountingResourceHolder.leakedResources();
    final AtomicBoolean released = new AtomicBoolean(false);
    // createDanglingReleaser() need to be a separate method because otherwise JVM preserves a ref to Holder on stack
    // and Cleaner is not called
    createDanglingReleaser(released);
    verifyCleanerRun(released, initialLeakedResources);
  }

  private void createDanglingReleaser(AtomicBoolean released)
  {
    try (ReferenceCountingResourceHolder<Closeable> handler = makeReleasingHandler(released)) {
      handler.increment(); // Releaser not close, the object leaked
    }
  }

  private void verifyCleanerRun(AtomicBoolean released, long initialLeakedResources) throws InterruptedException
  {
    // Wait until Closer runs
    for (int i = 0; i < 6000 && ReferenceCountingResourceHolder.leakedResources() == initialLeakedResources; i++) {
      System.gc();
      @SuppressWarnings("unused")
      byte[] garbage = new byte[10_000_000];
      Thread.sleep(10);
    }
    Assert.assertEquals(initialLeakedResources + 1, ReferenceCountingResourceHolder.leakedResources());
    // Cleaner also runs the closer
    Assert.assertTrue(released.get());
  }
}
