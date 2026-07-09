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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicInteger;

public class TransformAsyncResourceTest
{
  @Test
  public void testTransformAppliesFunction()
  {
    final SettableAsyncResource<Integer> source = new SettableAsyncResource<>();
    final AsyncResource<String> transformed = AsyncResources.transform(source, i -> "v" + i);

    Assertions.assertFalse(transformed.isReady());
    source.set(42, null);

    Assertions.assertTrue(transformed.isReady());
    Assertions.assertEquals("v42", transformed.get());
    transformed.close();
  }

  @Test
  public void testTransformOfAlreadyReadySourceFiresImmediately()
  {
    final SettableAsyncResource<Integer> source = new SettableAsyncResource<>();
    source.set(7, null);

    final AsyncResource<String> transformed = AsyncResources.transform(source, i -> "v" + i);
    Assertions.assertTrue(transformed.isReady());
    Assertions.assertEquals("v7", transformed.get());
    transformed.close();
  }

  @Test
  public void testFunctionCalledOnceAndLazily()
  {
    final AtomicInteger calls = new AtomicInteger();
    final SettableAsyncResource<Integer> source = new SettableAsyncResource<>();
    final AsyncResource<String> transformed = AsyncResources.transform(
        source,
        i -> {
          calls.incrementAndGet();
          return "v" + i;
        }
    );

    Assertions.assertEquals(0, calls.get(), "function must not run before the source is ready");
    source.set(1, null);
    Assertions.assertEquals(1, calls.get());

    // Repeated get() must not re-run the function.
    transformed.get();
    transformed.get();
    Assertions.assertEquals(1, calls.get());
    transformed.close();
  }

  @Test
  public void testSourceFailurePropagatesAndFunctionNotCalled()
  {
    final AtomicInteger calls = new AtomicInteger();
    final SettableAsyncResource<Integer> source = new SettableAsyncResource<>();
    final AsyncResource<String> transformed = AsyncResources.transform(
        source,
        i -> {
          calls.incrementAndGet();
          return "v" + i;
        }
    );

    final RuntimeException failure = new RuntimeException("boom");
    source.setException(failure);

    Assertions.assertTrue(transformed.isReady());
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, transformed::get);
    Assertions.assertSame(failure, thrown);
    Assertions.assertEquals(0, calls.get(), "function must not run when the source failed");
    transformed.close();
  }

  @Test
  public void testFunctionThrowsPropagates()
  {
    final RuntimeException failure = new RuntimeException("function boom");
    final SettableAsyncResource<Integer> source = new SettableAsyncResource<>();
    final AsyncResource<String> transformed = AsyncResources.transform(
        source,
        i -> {
          throw failure;
        }
    );

    source.set(1, null);

    Assertions.assertTrue(transformed.isReady());
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, transformed::get);
    Assertions.assertSame(failure, thrown);
    transformed.close();
  }

  @Test
  public void testCloseClosesSourceButNotTransformedValue()
  {
    final AtomicInteger sourceClose = new AtomicInteger();
    final AtomicInteger targetClose = new AtomicInteger();
    final SettableAsyncResource<Integer> source = new SettableAsyncResource<>();

    // The function's result is Closeable, but transform must NOT manage its lifecycle (it only closes the source).
    final Closeable target = targetClose::incrementAndGet;
    final AsyncResource<Closeable> transformed = AsyncResources.transform(source, i -> target);

    source.set(1, sourceClose::incrementAndGet);
    Assertions.assertTrue(transformed.isReady());
    Assertions.assertSame(target, transformed.get());

    transformed.close();
    Assertions.assertEquals(1, sourceClose.get(), "the source resource must be closed");
    Assertions.assertEquals(0, targetClose.get(), "the transformed value must not be closed by the transform");
  }

  @Test
  public void testCloseBeforeReadyCancelsSource()
  {
    final AtomicInteger sourceCancel = new AtomicInteger();
    final SettableAsyncResource<Integer> source = new SettableAsyncResource<>();
    source.setCanceler(sourceCancel::incrementAndGet);

    final AsyncResource<String> transformed = AsyncResources.transform(source, i -> "v" + i);
    Assertions.assertFalse(transformed.isReady());

    transformed.close();
    Assertions.assertEquals(1, sourceCancel.get());
  }
}
