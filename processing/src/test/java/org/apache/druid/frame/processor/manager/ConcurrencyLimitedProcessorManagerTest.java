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

package org.apache.druid.frame.processor.manager;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.Unit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ConcurrencyLimitedProcessorManagerTest
{
  @Test
  public void test_empty() throws Exception
  {
    try (final ConcurrencyLimitedProcessorManager<Object, Long> manager =
             new ConcurrencyLimitedProcessorManager<>(ProcessorManagers.none(), 1)) {
      final ListenableFuture<Optional<ProcessorAndCallback<Object>>> future = manager.next();
      Assertions.assertTrue(future.isDone());
      Assertions.assertFalse(future.get().isPresent());
      Assertions.assertEquals(0, (long) manager.result());
    }
  }

  @Test
  public void test_one_limitOne() throws Exception
  {
    final NilFrameProcessor<Unit> processor = new NilFrameProcessor<>();

    try (final ConcurrencyLimitedProcessorManager<Unit, Long> manager =
             new ConcurrencyLimitedProcessorManager<>(ProcessorManagers.of(ImmutableList.of(processor)), 1)) {
      // First element.
      ListenableFuture<Optional<ProcessorAndCallback<Unit>>> future = manager.next();
      Assertions.assertTrue(future.isDone());
      Assertions.assertTrue(future.get().isPresent());
      Assertions.assertSame(processor, future.get().get().processor());

      // Simulate processor finishing.
      future.get().get().onComplete(Unit.instance());

      // End of sequence.
      future = manager.next();
      Assertions.assertTrue(future.isDone());
      Assertions.assertFalse(future.get().isPresent());
    }
  }

  @Test
  public void test_two_limitOne() throws Exception
  {
    final NilFrameProcessor<Unit> processor0 = new NilFrameProcessor<>();
    final NilFrameProcessor<Unit> processor1 = new NilFrameProcessor<>();
    final ImmutableList<NilFrameProcessor<Unit>> processors = ImmutableList.of(processor0, processor1);

    try (final ConcurrencyLimitedProcessorManager<Unit, Long> manager =
             new ConcurrencyLimitedProcessorManager<>(ProcessorManagers.of(processors), 1)) {
      // First element.
      ListenableFuture<Optional<ProcessorAndCallback<Unit>>> future0 = manager.next();
      Assertions.assertTrue(future0.isDone());
      Assertions.assertTrue(future0.get().isPresent());
      Assertions.assertSame(processors.get(0), future0.get().get().processor());

      // Second element. Not yet ready to run due to the limit.
      ListenableFuture<Optional<ProcessorAndCallback<Unit>>> future1 = manager.next();
      Assertions.assertFalse(future1.isDone());

      // Simulate processor0 finishing.
      future0.get().get().onComplete(Unit.instance());

      // processor1 is now ready to run.
      Assertions.assertTrue(future1.isDone());
      Assertions.assertTrue(future1.get().isPresent());
      Assertions.assertSame(processors.get(1), future1.get().get().processor());

      // Simulate processor1 finishing.
      future1.get().get().onComplete(Unit.instance());

      // End of sequence.
      future1 = manager.next();
      Assertions.assertTrue(future1.isDone());
      Assertions.assertFalse(future1.get().isPresent());
    }
  }
}
