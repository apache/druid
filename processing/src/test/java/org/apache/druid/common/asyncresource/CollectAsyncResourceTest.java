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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CollectAsyncResourceTest
{
  @Test
  public void testEmptyListCompletesImmediatelyWithEmptyResult()
  {
    final List<AsyncResource<String>> sources = List.of();
    final AsyncResource<List<String>> collected = AsyncResources.collect(sources);

    Assertions.assertTrue(collected.isReady(), "collect of an empty list must be immediately ready");
    Assertions.assertEquals(List.of(), collected.get());
    collected.close();
  }

  @Test
  public void testReadyOnlyAfterAllSourcesReadyAndPreservesOrder()
  {
    final SettableAsyncResource<String> a = new SettableAsyncResource<>();
    final SettableAsyncResource<String> b = new SettableAsyncResource<>();
    final SettableAsyncResource<String> c = new SettableAsyncResource<>();
    final List<AsyncResource<String>> sources = List.of(a, b, c);
    final AsyncResource<List<String>> collected = AsyncResources.collect(sources);

    Assertions.assertFalse(collected.isReady());

    // Complete out of order; the result must still be in source order.
    c.set("c", null);
    Assertions.assertFalse(collected.isReady());
    a.set("a", null);
    Assertions.assertFalse(collected.isReady());
    b.set("b", null);

    Assertions.assertTrue(collected.isReady());
    Assertions.assertEquals(List.of("a", "b", "c"), collected.get());
    collected.close();
  }

  @Test
  public void testAlreadyReadySourcesCompleteOnConstruction()
  {
    final SettableAsyncResource<String> a = new SettableAsyncResource<>();
    final SettableAsyncResource<String> b = new SettableAsyncResource<>();
    a.set("a", null);
    b.set("b", null);

    final List<AsyncResource<String>> sources = List.of(a, b);
    final AsyncResource<List<String>> collected = AsyncResources.collect(sources);

    Assertions.assertTrue(collected.isReady());
    Assertions.assertEquals(List.of("a", "b"), collected.get());
    collected.close();
  }

  @Test
  public void testSourceFailurePropagates()
  {
    final SettableAsyncResource<String> a = new SettableAsyncResource<>();
    final SettableAsyncResource<String> b = new SettableAsyncResource<>();
    final List<AsyncResource<String>> sources = List.of(a, b);
    final AsyncResource<List<String>> collected = AsyncResources.collect(sources);

    final RuntimeException failure = new RuntimeException("boom");
    a.set("a", null);
    b.setException(failure);

    Assertions.assertTrue(collected.isReady());
    final RuntimeException thrown = Assertions.assertThrows(RuntimeException.class, collected::get);
    Assertions.assertSame(failure, thrown);
    collected.close();
  }

  @Test
  public void testCloseClosesAllReadySources()
  {
    final AtomicInteger aClose = new AtomicInteger();
    final AtomicInteger bClose = new AtomicInteger();
    final SettableAsyncResource<String> a = new SettableAsyncResource<>();
    final SettableAsyncResource<String> b = new SettableAsyncResource<>();
    a.set("a", aClose::incrementAndGet);
    b.set("b", bClose::incrementAndGet);

    final List<AsyncResource<String>> sources = List.of(a, b);
    final AsyncResource<List<String>> collected = AsyncResources.collect(sources);
    Assertions.assertTrue(collected.isReady());

    collected.close();
    Assertions.assertEquals(1, aClose.get());
    Assertions.assertEquals(1, bClose.get());
  }

  @Test
  public void testCloseCancelsPendingSources()
  {
    final AtomicInteger aCancel = new AtomicInteger();
    final SettableAsyncResource<String> a = new SettableAsyncResource<>();
    a.setCanceler(aCancel::incrementAndGet);

    final List<AsyncResource<String>> sources = List.of(a);
    final AsyncResource<List<String>> collected = AsyncResources.collect(sources);
    Assertions.assertFalse(collected.isReady());

    collected.close();
    Assertions.assertEquals(1, aCancel.get(), "closing the collect must cancel pending sources");
  }
}
