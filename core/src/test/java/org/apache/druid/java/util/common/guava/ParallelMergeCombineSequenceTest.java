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

import com.google.common.collect.Ordering;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.nary.BinaryFn;
import org.apache.druid.query.ThreadResource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

public class ParallelMergeCombineSequenceTest
{
  private Random random = new Random(System.currentTimeMillis());
  private ExecutorService service;
  private BlockingPool<ThreadResource> resourcePool;

  @Before
  public void setup()
  {
    service = Execs.multiThreaded(2, "parallel-merge-combine-sequence-test");
    resourcePool = new DefaultBlockingPool<>(ThreadResource::new, 2);
  }

  @After
  public void teardown()
  {
    service.shutdown();
  }

  @Test(timeout = 5000L)
  public void testSimple() throws InterruptedException
  {
    List<IntPair> pairs1 = Arrays.asList(
        new IntPair(0, 6),
        new IntPair(1, 1),
        new IntPair(2, 1),
        new IntPair(5, 11),
        new IntPair(6, 1)
    );

    List<IntPair> pairs2 = Arrays.asList(
        new IntPair(0, 1),
        new IntPair(1, 13),
        new IntPair(4, 1),
        new IntPair(6, 2),
        new IntPair(10, 2)
    );

    List<IntPair> pairs3 = Arrays.asList(
        new IntPair(4, 5),
        new IntPair(10, 3)
    );

    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.simple(pairs1));
    input.add(Sequences.simple(pairs2));
    input.add(Sequences.simple(pairs3));

    assertResult(input);
  }

  @Test(timeout = 5000L)
  public void teatRandom() throws InterruptedException
  {
    final List<Sequence<IntPair>> sequences = new ArrayList<>();
    for (int i = 0; i < 7; i++) {
      final List<IntPair> pairs = new ArrayList<>();
      final int size = 10000 + random.nextInt(1000);
      for (int j = 0; j < size; j++) {
        pairs.add(new IntPair(random.nextInt(10000), random.nextInt(500)));
      }
      pairs.sort(Comparator.comparing(pair -> pair.lhs));
      sequences.add(Sequences.simple(pairs));
    }

    assertResult(sequences);
  }

  private void assertResult(List<Sequence<IntPair>> sequences) throws InterruptedException
  {
    final Ordering<IntPair> ordering = Ordering.natural().onResultOf(p -> p.lhs);
    final BinaryFn<IntPair, IntPair, IntPair> mergeFn = (lhs, rhs) -> {
      if (lhs == null) {
        return rhs;
      }

      if (rhs == null) {
        return lhs;
      }

      return new IntPair(lhs.lhs, lhs.rhs + rhs.rhs);
    };

    final CombiningSequence<IntPair> combiningSequence = CombiningSequence.create(
        new MergeSequence<>(ordering, Sequences.simple(sequences)),
        ordering,
        mergeFn
    );

    final ParallelMergeCombineSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombineSequence<>(
        service,
        sequences,
        ordering,
        mergeFn,
        resourcePool.takeBatch(2),
        10240,
        true,
        1000,
        0
    );

    Yielder<IntPair> combiningYielder = Yielders.each(combiningSequence);
    Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);

    IntPair prev = null;

    while (!combiningYielder.isDone() && !parallelMergeCombineYielder.isDone()) {
      Assert.assertEquals(combiningYielder.get(), parallelMergeCombineYielder.get());
      Assert.assertNotEquals(parallelMergeCombineYielder.get(), prev);
      prev = parallelMergeCombineYielder.get();
      combiningYielder = combiningYielder.next(combiningYielder.get());
      parallelMergeCombineYielder = parallelMergeCombineYielder.next(parallelMergeCombineYielder.get());
    }

    Assert.assertTrue(combiningYielder.isDone());
    Assert.assertTrue(parallelMergeCombineYielder.isDone());
    while (resourcePool.available() < 2) {
      Thread.sleep(100);
    }
    Assert.assertEquals(2, resourcePool.available());
  }
  
  private static class IntPair extends Pair<Integer, Integer>
  {
    IntPair(@Nullable Integer lhs, @Nullable Integer rhs)
    {
      super(lhs, rhs);
    }
  }
}
