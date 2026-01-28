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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.ExecutionException;

public class FutureBoxTest
{
  @Test
  public void test_immediateFutures() throws Exception
  {
    try (final FutureBox box = new FutureBox()) {
      Assertions.assertEquals("a", box.register(Futures.immediateFuture("a")).get());
      Assertions.assertThrows(
          ExecutionException.class,
          () -> box.register(Futures.immediateFailedFuture(new RuntimeException())).get()
      );
      Assertions.assertTrue(box.register(Futures.immediateCancelledFuture()).isCancelled());
      Assertions.assertEquals(0, box.pendingCount());
    }
  }

  @Test
  public void test_register_thenStop()
  {
    final FutureBox box = new FutureBox();
    final SettableFuture<String> settableFuture = SettableFuture.create();

    final ListenableFuture<String> retVal = box.register(settableFuture);
    Assertions.assertSame(retVal, settableFuture);
    Assertions.assertEquals(1, box.pendingCount());

    box.close();
    Assertions.assertEquals(0, box.pendingCount());

    Assertions.assertTrue(settableFuture.isCancelled());
  }

  @Test
  public void test_stop_thenRegister()
  {
    final FutureBox box = new FutureBox();
    final SettableFuture<String> settableFuture = SettableFuture.create();

    box.close();
    final ListenableFuture<String> retVal = box.register(settableFuture);

    Assertions.assertSame(retVal, settableFuture);
    Assertions.assertEquals(0, box.pendingCount());
    Assertions.assertTrue(settableFuture.isCancelled());
  }
}
