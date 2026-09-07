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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class SettableAsyncResourceTest
{
  @Test
  public void testSetThenGetAndReady()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    Assertions.assertFalse(resource.isReady());

    Assertions.assertTrue(resource.set("value", null));
    Assertions.assertTrue(resource.isReady());
    Assertions.assertEquals("value", resource.get());
  }

  @Test
  public void testGetBeforeReadyThrows()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    Assertions.assertThrows(DruidException.class, resource::get);
  }

  @Test
  public void testSetNullObjectThrows()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    Assertions.assertThrows(DruidException.class, () -> resource.set(null, null));
  }

  @Test
  public void testCloseClosesTheResourceExactlyOnce()
  {
    final AtomicInteger closeCount = new AtomicInteger();
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.set("value", closeCount::incrementAndGet);

    resource.close();
    Assertions.assertEquals(1, closeCount.get());
  }

  @Test
  public void testSetWithResourceHolder()
  {
    final AtomicInteger closeCount = new AtomicInteger();
    final TrackedCloseable closeable = new TrackedCloseable("value", closeCount);
    final SettableAsyncResource<TrackedCloseable> resource = new SettableAsyncResource<>();
    Assertions.assertTrue(resource.set(ResourceHolder.fromCloseable(closeable)));

    Assertions.assertSame(closeable, resource.get());
    resource.close();
    Assertions.assertEquals(1, closeCount.get());
  }

  @Test
  public void testDoubleSetThrows()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.set("value", null);
    Assertions.assertThrows(DruidException.class, () -> resource.set("again", null));
  }

  @Test
  public void testSetAfterExceptionThrows()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.setException(new RuntimeException("boom"));
    Assertions.assertThrows(DruidException.class, () -> resource.set("value", null));
  }

  @Test
  public void testSetExceptionUncheckedRethrownAsIs()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    final RuntimeException failure = new RuntimeException("boom");
    resource.setException(failure);

    Assertions.assertTrue(resource.isReady());
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, resource::get);
    Assertions.assertSame(failure, thrown);
  }

  @Test
  public void testSetExceptionCheckedWrappedInDruidException()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.setException(new IOException("boom"));

    Assertions.assertTrue(resource.isReady());
    Assertions.assertThrows(DruidException.class, resource::get);
  }

  @Test
  public void testReadyCallbackFiresImmediatelyWhenAlreadyReady()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.set("value", null);

    final AtomicInteger fired = new AtomicInteger();
    resource.addReadyCallback(fired::incrementAndGet);
    Assertions.assertEquals(1, fired.get());
  }

  @Test
  public void testReadyCallbackFiresOnSet()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();

    final AtomicInteger fired = new AtomicInteger();
    resource.addReadyCallback(fired::incrementAndGet);
    Assertions.assertEquals(0, fired.get());

    resource.set("value", null);
    Assertions.assertEquals(1, fired.get());
  }

  @Test
  public void testReadyCallbackFiresOnSetException()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();

    final AtomicInteger fired = new AtomicInteger();
    resource.addReadyCallback(fired::incrementAndGet);
    resource.setException(new RuntimeException("boom"));
    Assertions.assertEquals(1, fired.get());
  }

  @Test
  public void testAddReadyCallbackAfterCloseThrows()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.close();
    Assertions.assertThrows(DruidException.class, () -> resource.addReadyCallback(() -> {}));
  }

  @Test
  public void testCloseInNewStateRunsCanceler()
  {
    final AtomicInteger canceled = new AtomicInteger();
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.setCanceler(canceled::incrementAndGet);

    resource.close();
    Assertions.assertEquals(1, canceled.get());
  }

  @Test
  public void testCancelerNotRunOnceReady()
  {
    final AtomicInteger canceled = new AtomicInteger();
    final AtomicInteger closeCount = new AtomicInteger();
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.setCanceler(canceled::incrementAndGet);

    // Becoming ready clears the canceler; close() must then close the resource, not cancel.
    resource.set("value", closeCount::incrementAndGet);
    resource.close();

    Assertions.assertEquals(0, canceled.get(), "canceler must not run once the resource is ready");
    Assertions.assertEquals(1, closeCount.get());
  }

  @Test
  public void testSetCancelerTwiceThrows()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.setCanceler(() -> {});
    Assertions.assertThrows(DruidException.class, () -> resource.setCanceler(() -> {}));
  }

  @Test
  public void testSetAfterCloseReturnsFalseAndDoesNotCloseTheResource()
  {
    final AtomicInteger closeCount = new AtomicInteger();
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.close();

    // Producer racing with close: set() returns false, and the resource is NOT closed by the wrapper, so the
    // producer remains responsible for closing it.
    Assertions.assertFalse(resource.set("value", closeCount::incrementAndGet));
    Assertions.assertEquals(0, closeCount.get());
  }

  @Test
  public void testDoubleCloseThrows()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.set("value", null);
    resource.close();
    Assertions.assertThrows(DruidException.class, resource::close);
  }

  @Test
  public void testAwaitReturnsValueWhenReady() throws InterruptedException
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.set("value", null);
    Assertions.assertEquals("value", resource.await());
  }

  @Test
  public void testAwaitWithTimeoutReturnsValueWhenReady() throws Exception
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    resource.set("value", null);
    Assertions.assertEquals("value", resource.await(1000));
  }

  @Test
  public void testAwaitWithTimeoutThrowsWhenNotReady()
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    Assertions.assertThrows(TimeoutException.class, () -> resource.await(10));
  }

  @Test
  public void testAwaitWakesUpWhenSetFromAnotherThread() throws Exception
  {
    final SettableAsyncResource<String> resource = new SettableAsyncResource<>();
    final Thread setter = new Thread(() -> resource.set("value", null));
    setter.start();
    Assertions.assertEquals("value", resource.await());
    setter.join();
  }

  private static class TrackedCloseable implements java.io.Closeable
  {
    private final String value;
    private final AtomicInteger closeCount;

    TrackedCloseable(final String value, final AtomicInteger closeCount)
    {
      this.value = value;
      this.closeCount = closeCount;
    }

    @Override
    public String toString()
    {
      return value;
    }

    @Override
    public void close()
    {
      closeCount.incrementAndGet();
    }
  }
}
