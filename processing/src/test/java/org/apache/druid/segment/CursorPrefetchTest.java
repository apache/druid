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

package org.apache.druid.segment;

import org.apache.druid.common.asyncresource.AsyncResourceCanceledException;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class CursorPrefetchTest
{
  @Test
  void testNoneIsReadyWithNoStats()
  {
    try (CursorPrefetch prefetch = CursorPrefetch.none()) {
      Assertions.assertTrue(prefetch.isReady());
      Assertions.assertEquals(CursorPrefetch.Stats.NONE, prefetch.get());
    }
  }

  @Test
  void testCloseBeforeReadyRunsCancelerAndReportsCancellation()
  {
    final AtomicBoolean canceled = new AtomicBoolean(false);
    final AtomicBoolean fired = new AtomicBoolean(false);
    final CursorPrefetch prefetch = new CursorPrefetch(() -> canceled.set(true));
    prefetch.addReadyCallback(() -> fired.set(true));
    Assertions.assertFalse(prefetch.isReady());

    prefetch.close();
    Assertions.assertTrue(canceled.get(), "closing before ready must run the canceler");
    Assertions.assertTrue(fired.get(), "closing before ready must wake waiters");
    Assertions.assertTrue(prefetch.isReady(), "a canceled prefetch is over");
    Assertions.assertThrows(AsyncResourceCanceledException.class, prefetch::get);

    // the canceler is not consulted again by a late completion, which is dropped
    Assertions.assertFalse(prefetch.set(CursorPrefetch.Stats.NONE, null));
  }

  @Test
  void testCloseAfterReadyReleasesPins()
  {
    final AtomicInteger pinReleases = new AtomicInteger();
    final AtomicBoolean canceled = new AtomicBoolean(false);
    final CursorPrefetch prefetch = new CursorPrefetch(() -> canceled.set(true));
    final CursorPrefetch.Stats stats = new CursorPrefetch.Stats(List.of("__base"), 1, 10L);
    Assertions.assertTrue(prefetch.set(stats, pinReleases::incrementAndGet));
    Assertions.assertTrue(prefetch.isReady());
    Assertions.assertEquals(stats, prefetch.get());
    Assertions.assertEquals(0, pinReleases.get(), "pins stay held while the handle is open");

    prefetch.close();
    Assertions.assertEquals(1, pinReleases.get(), "closing a ready handle releases its pins");
    Assertions.assertFalse(canceled.get(), "nothing to cancel once ready");
  }

  @Test
  void testFailureSurfacesFromGet()
  {
    final CursorPrefetch prefetch = new CursorPrefetch(null);
    prefetch.setException(new IllegalStateException("deep storage went away"));
    Assertions.assertTrue(prefetch.isReady());
    final IllegalStateException e = Assertions.assertThrows(IllegalStateException.class, prefetch::get);
    Assertions.assertEquals("deep storage went away", e.getMessage());
    prefetch.close();
  }

  @Test
  void testCursorFactoryDefaultIsReadyNoOp()
  {
    final CursorFactory resident = new CursorFactory()
    {
      @Override
      public CursorHolder makeCursorHolder(CursorBuildSpec spec)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public RowSignature getRowSignature()
      {
        return RowSignature.empty();
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return null;
      }
    };
    try (CursorPrefetch prefetch = resident.prefetch(CursorBuildSpec.FULL_SCAN)) {
      Assertions.assertTrue(prefetch.isReady());
      Assertions.assertEquals(CursorPrefetch.Stats.NONE, prefetch.get());
    }
  }
}
