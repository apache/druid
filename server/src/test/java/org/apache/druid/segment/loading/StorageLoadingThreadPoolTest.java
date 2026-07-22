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

package org.apache.druid.segment.loading;

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class StorageLoadingThreadPoolTest
{
  private static SegmentLoaderConfig oneVirtualThreadConfig()
  {
    return new SegmentLoaderConfig()
    {
      @Override
      public int getVirtualStorageLoadThreads()
      {
        return 1;
      }
    }.setVirtualStorage(true);
  }

  private static SegmentLoaderConfig fixedThreadConfig()
  {
    return new SegmentLoaderConfig()
    {
      @Override
      public int getVirtualStorageLoadThreads()
      {
        return 2;
      }

      @Override
      public boolean isVirtualStorageUseVirtualThreads()
      {
        return false;
      }
    }.setVirtualStorage(true);
  }

  @Test
  void testCreateFromConfigIsUnavailableWhenNotVirtualStorage()
  {
    Assertions.assertFalse(StorageLoadingThreadPool.createFromConfig(new SegmentLoaderConfig()).isAvailable());
  }

  @Test
  void testCreateFromConfigIsAvailableWhenVirtualStorage()
  {
    // The shared ephemeral pool is built this way: createFromConfig(config.withVirtualStorage(true)).
    final StorageLoadingThreadPool pool =
        StorageLoadingThreadPool.createFromConfig(new SegmentLoaderConfig().withVirtualStorage(true));
    try {
      Assertions.assertTrue(pool.isAvailable());
      Assertions.assertNotNull(pool.getExecutorService());
    }
    finally {
      pool.stop();
    }
  }

  @Test
  void testCreateFromConfigRejectsNonPositiveThreadCountInVirtualStorage()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig()
    {
      @Override
      public int getVirtualStorageLoadThreads()
      {
        return 0;
      }
    }.setVirtualStorage(true);
    Assertions.assertThrows(DruidException.class, () -> StorageLoadingThreadPool.createFromConfig(config));
  }

  @Test
  @Timeout(30)
  void testAcquireLoadPermitReleasesOnCloseForReuse()
  {
    final StorageLoadingThreadPool pool = StorageLoadingThreadPool.createFromConfig(oneVirtualThreadConfig());
    try {
      final StorageLoadingThreadPool.LoadPermit first = pool.acquireLoadPermit();
      first.close();
      first.close(); // idempotent: a double close must not over-release the single permit
      // The permit is available again, so this returns immediately (it would hang under @Timeout if close() had not
      // released it, or the extra release above had inflated the count).
      pool.acquireLoadPermit().close();
    }
    finally {
      pool.stop();
    }
  }

  @Test
  @Timeout(30)
  void testAcquireLoadPermitIsNoOpWithoutSemaphore()
  {
    // Fixed-thread mode has no semaphore (the thread count is the bound), so acquiring repeatedly without releasing
    // must never block.
    final StorageLoadingThreadPool pool = StorageLoadingThreadPool.createFromConfig(fixedThreadConfig());
    try {
      pool.acquireLoadPermit();
      pool.acquireLoadPermit();
      pool.acquireLoadPermit().close();
    }
    finally {
      pool.stop();
    }
  }

  @Test
  @Timeout(30)
  void testAcquireLoadPermitIsInterruptibleAndDoesNotConsumeAPermit()
  {
    // A load interrupted while acquiring must abort (throw) and take no permit - this is the cancel-before-I/O
    // guarantee that lets a cancelled query stop permit-blocked downloads before they touch deep storage.
    final StorageLoadingThreadPool pool = StorageLoadingThreadPool.createFromConfig(oneVirtualThreadConfig());
    try {
      Thread.currentThread().interrupt();
      Assertions.assertThrows(RuntimeException.class, pool::acquireLoadPermit);
      Assertions.assertTrue(Thread.interrupted(), "interrupt flag should be restored (and is cleared here)");
      // The aborted acquire consumed no permit, so this succeeds (would hang under @Timeout had it leaked one).
      pool.acquireLoadPermit().close();
    }
    finally {
      pool.stop();
    }
  }
}
