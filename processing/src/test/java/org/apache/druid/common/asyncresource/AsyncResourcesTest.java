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

package org.apache.druid.common.asyncresource;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class AsyncResourcesTest
{
  @Test
  public void testFromFutureSuccess()
  {
    final SettableFuture<String> future = SettableFuture.create();
    final AsyncResource<String> resource = AsyncResources.fromFutureUnmanaged(future);

    Assertions.assertFalse(resource.isReady());
    future.set("value");
    Assertions.assertTrue(resource.isReady());
    Assertions.assertEquals("value", resource.get());

    // closing after a non-resource value completes is a no-op (nothing to release), and does not cancel the
    // already-completed future
    resource.close();
    Assertions.assertFalse(future.isCancelled());
  }

  @Test
  public void testFromFutureFailureSurfacesAtGet()
  {
    final SettableFuture<String> future = SettableFuture.create();
    final AsyncResource<String> resource = AsyncResources.fromFutureUnmanaged(future);

    final RuntimeException boom = new IllegalStateException("boom");
    future.setException(boom);
    Assertions.assertTrue(resource.isReady());
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, resource::get);
    Assertions.assertSame(boom, thrown);
  }

  @Test
  public void testFromFutureCloseBeforeCompleteCancelsFuture()
  {
    final SettableFuture<String> future = SettableFuture.create();
    final AsyncResource<String> resource = AsyncResources.fromFutureUnmanaged(future);

    Assertions.assertFalse(resource.isReady());
    resource.close();
    Assertions.assertTrue(future.isCancelled(), "closing before completion must cancel the backing future");
  }

  @Test
  public void testFromFutureReadyCallbackFiresOnCompletion()
  {
    final SettableFuture<String> future = SettableFuture.create();
    final AsyncResource<String> resource = AsyncResources.fromFutureUnmanaged(future);

    final boolean[] fired = {false};
    resource.addReadyCallback(() -> fired[0] = true);
    Assertions.assertFalse(fired[0]);

    future.set("value");
    Assertions.assertTrue(fired[0], "ready callback should fire when the future completes");
  }

  @Test
  public void testCollectOfFromFutureCancelsAllOnClose()
  {
    final SettableFuture<String> a = SettableFuture.create();
    final SettableFuture<String> b = SettableFuture.create();
    final AsyncResource<List<String>> collected =
        AsyncResources.collect(List.of(AsyncResources.fromFutureUnmanaged(a), AsyncResources.fromFutureUnmanaged(b)));

    Assertions.assertFalse(collected.isReady());
    // closing the collected resource before it is ready cancels every backing future
    collected.close();
    Assertions.assertTrue(a.isCancelled());
    Assertions.assertTrue(b.isCancelled());
  }

  @Test
  public void testCollectOfFromFutureReadyWhenAllComplete()
  {
    final SettableFuture<String> a = SettableFuture.create();
    final SettableFuture<String> b = SettableFuture.create();
    final AsyncResource<List<String>> collected =
        AsyncResources.collect(List.of(AsyncResources.fromFutureUnmanaged(a), AsyncResources.fromFutureUnmanaged(b)));

    a.set("a");
    Assertions.assertFalse(collected.isReady(), "not ready until every source completes");
    b.set("b");
    Assertions.assertTrue(collected.isReady());
    Assertions.assertEquals(List.of("a", "b"), collected.get());
    collected.close();
  }

  @Test
  public void testFromFutureGetBeforeReadyThrows()
  {
    final AsyncResource<String> resource = AsyncResources.fromFutureUnmanaged(SettableFuture.create());
    Assertions.assertThrows(DruidException.class, resource::get);
  }

}
