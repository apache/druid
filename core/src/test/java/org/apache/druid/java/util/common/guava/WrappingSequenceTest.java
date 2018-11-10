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

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class WrappingSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    final AtomicInteger closedCounter = new AtomicInteger(0);
    Closeable closeable = () -> closedCounter.incrementAndGet();

    final List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);

    SequenceTestHelper.testAll(Sequences.simple(nums).withBaggage(closeable), nums);

    Assert.assertEquals(3, closedCounter.get());

    closedCounter.set(0);
    SequenceTestHelper.testClosed(closedCounter, new UnsupportedSequence().withBaggage(closeable));
  }

  @Test
  public void testConsistentCloseOrder()
  {
    final AtomicInteger closed1 = new AtomicInteger();
    final AtomicInteger closed2 = new AtomicInteger();
    final AtomicInteger counter = new AtomicInteger();

    Sequence<Integer> sequence = Sequences
        .simple(Arrays.asList(1, 2, 3))
        .withBaggage(() -> closed1.set(counter.incrementAndGet()))
        .withBaggage(() -> closed2.set(counter.incrementAndGet()));
    // Run sequence via accumulate
    sequence.toList();
    Assert.assertEquals(1, closed1.get());
    Assert.assertEquals(2, closed2.get());

    // Ensure sequence runs via Yielder, because LimitedSequence extends YieldingSequenceBase
    Sequence<Integer> yieldingSequence = sequence.limit(1);
    yieldingSequence.toList();
    Assert.assertEquals(3, closed1.get());
    Assert.assertEquals(4, closed2.get());
  }
}
