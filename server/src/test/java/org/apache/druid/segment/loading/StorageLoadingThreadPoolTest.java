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

class StorageLoadingThreadPoolTest
{
  @Test
  void testCreateForEphemeralIsAvailableEvenWhenConfigIsNotVirtualStorage()
  {
    // The node config is not in virtual-storage mode, but the ephemeral (per-task) pool must still build a usable
    // on-demand loading executor so it can serve virtual-storage task caches.
    final SegmentLoaderConfig config = new SegmentLoaderConfig();
    Assertions.assertFalse(config.isVirtualStorage());

    final StorageLoadingThreadPool pool = StorageLoadingThreadPool.createForEphemeral(config);
    try {
      Assertions.assertTrue(pool.isAvailable());
      Assertions.assertNotNull(pool.getExecutorService());
    }
    finally {
      pool.stop();
    }
  }

  @Test
  void testCreateFromConfigIsUnavailableWhenNotVirtualStorage()
  {
    // Unchanged behavior: the default (unqualified) pool has no executor outside virtual-storage mode.
    Assertions.assertFalse(StorageLoadingThreadPool.createFromConfig(new SegmentLoaderConfig()).isAvailable());
  }

  @Test
  void testCreateFromConfigIsAvailableWhenVirtualStorage()
  {
    final StorageLoadingThreadPool pool =
        StorageLoadingThreadPool.createFromConfig(new SegmentLoaderConfig().setVirtualStorage(true));
    try {
      Assertions.assertTrue(pool.isAvailable());
    }
    finally {
      pool.stop();
    }
  }

  @Test
  void testCreateForEphemeralRejectsNonPositiveThreadCount()
  {
    final SegmentLoaderConfig config = new SegmentLoaderConfig()
    {
      @Override
      public int getVirtualStorageLoadThreads()
      {
        return 0;
      }
    };
    Assertions.assertThrows(DruidException.class, () -> StorageLoadingThreadPool.createForEphemeral(config));
  }
}
