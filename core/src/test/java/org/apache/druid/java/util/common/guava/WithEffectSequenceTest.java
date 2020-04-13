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

package org.apache.druid.java.util.common.guava;

import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class WithEffectSequenceTest
{
  @Test
  public void testConsistentEffectApplicationOrder()
  {
    final AtomicInteger effect1 = new AtomicInteger();
    final AtomicInteger effect2 = new AtomicInteger();
    final AtomicInteger counter = new AtomicInteger();

    Sequence<Integer> sequence = Sequences
        .simple(Arrays.asList(1, 2, 3))
        .withEffect(
            () -> effect1.set(counter.incrementAndGet()),
            Execs.directExecutor()
        )
        .withEffect(
            () -> effect2.set(counter.incrementAndGet()),
            Execs.directExecutor()
        );
    // Run sequence via accumulate
    sequence.toList();
    Assert.assertEquals(1, effect1.get());
    Assert.assertEquals(2, effect2.get());

    // Ensure sequence runs via Yielder, because LimitedSequence extends YieldingSequenceBase which
    // implements accumulate() via yielder().
    // "Limiting" a sequence of 3 elements with 4 to let effects be executed. If e. g. limit with 1 or 2, effects are
    // not executed.
    Sequence<Integer> yieldingSequence = sequence.limit(4);
    yieldingSequence.toList();
    Assert.assertEquals(3, effect1.get());
    Assert.assertEquals(4, effect2.get());
  }

  @Test
  public void testEffectExecutedIfWrappedSequenceThrowsExceptionFromClose()
  {
    Sequence<Integer> baseSeq = Sequences.simple(Arrays.asList(1, 2, 3));
    Sequence<Integer> throwingSeq = baseSeq.withBaggage(() -> {
      throw new RuntimeException();
    });
    final AtomicBoolean effectExecuted = new AtomicBoolean();
    Sequence<Integer> seqWithEffect =
        throwingSeq.withEffect(() -> effectExecuted.set(true), Execs.directExecutor());
    try {
      seqWithEffect.toList();
      Assert.fail("expected RuntimeException");
    }
    catch (RuntimeException e) {
      // expected
      Assert.assertTrue(effectExecuted.get());
    }
  }
}
