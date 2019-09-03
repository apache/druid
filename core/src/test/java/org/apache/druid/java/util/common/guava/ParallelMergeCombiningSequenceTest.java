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
import org.apache.druid.common.guava.CombiningSequence;
import org.apache.druid.java.util.common.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BinaryOperator;

public class ParallelMergeCombiningSequenceTest
{
  private ForkJoinPool pool;

  @Before
  public void setup()
  {
    pool = new ForkJoinPool(4, ForkJoinPool.defaultForkJoinWorkerThreadFactory, null, true);
  }

  @After
  public void teardown()
  {
    pool.shutdown();
  }

  @Test
  public void testSimple() throws Exception
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
        new IntPair(5, 2)
    );


    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.simple(pairs1));
    input.add(Sequences.simple(pairs2));

    assertResult(input);
  }

  @Test
  public void testLongBoy() throws Exception
  {
    List<IntPair> pairs1 = Arrays.asList(
        new IntPair(0, 6),
        new IntPair(1, 1),
        new IntPair(2, 1),
        new IntPair(5, 11),
        new IntPair(6, 11),
        new IntPair(7, 11),
        new IntPair(9, 11),
        new IntPair(11, 11),
        new IntPair(16, 11),
        new IntPair(24, 11),
        new IntPair(25, 11),
        new IntPair(27, 11),
        new IntPair(28, 1)
    );

    List<IntPair> pairs2 = Arrays.asList(
        new IntPair(0, 1),
        new IntPair(1, 13),
        new IntPair(4, 1),
        new IntPair(5, 1),
        new IntPair(7, 1),
        new IntPair(9, 1),
        new IntPair(10, 1),
        new IntPair(13, 1),
        new IntPair(14, 1),
        new IntPair(23, 1),
        new IntPair(25, 1),
        new IntPair(27, 1),
        new IntPair(28, 2)
    );


    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.simple(pairs1));
    input.add(Sequences.simple(pairs2));

    assertResult(input);
  }

  @Test
  public void testSomeStuff() throws Exception
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

    List<IntPair> pairs4 = Arrays.asList(
        new IntPair(0, 1),
        new IntPair(1, 13),
        new IntPair(4, 1),
        new IntPair(6, 2),
        new IntPair(10, 2)
    );

    List<IntPair> pairs5 = Arrays.asList(
        new IntPair(0, 6),
        new IntPair(1, 1),
        new IntPair(2, 1),
        new IntPair(5, 11),
        new IntPair(6, 1)
    );

    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.simple(pairs1));
    input.add(Sequences.simple(pairs2));
    input.add(Sequences.simple(pairs3));
    input.add(Sequences.simple(pairs4));
    input.add(Sequences.simple(pairs5));

    assertResult(input);
  }

  private void assertResult(List<Sequence<IntPair>> sequences) throws InterruptedException
  {
    final Ordering<IntPair> ordering = Ordering.natural().onResultOf(p -> p.lhs);
    final BinaryOperator<IntPair> mergeFn = (lhs, rhs) -> {
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

    final ParallelMergeCombiningSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombiningSequence<>(
        pool,
        sequences,
        ordering,
        mergeFn,
        64,
        false,
        5000,
        0,
        Runtime.getRuntime().availableProcessors() - 1,
        8,
        4
    );

    Yielder<IntPair> combiningYielder = Yielders.each(combiningSequence);
    Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);

    IntPair prev = null;

    while (!combiningYielder.isDone() && !parallelMergeCombineYielder.isDone()) {
      Assert.assertEquals(combiningYielder.get(), parallelMergeCombineYielder.get());
      System.out.println(parallelMergeCombineYielder.get());
      Assert.assertNotEquals(parallelMergeCombineYielder.get(), prev);
      prev = parallelMergeCombineYielder.get();
      combiningYielder = combiningYielder.next(combiningYielder.get());
      parallelMergeCombineYielder = parallelMergeCombineYielder.next(parallelMergeCombineYielder.get());
    }

    Assert.assertTrue(combiningYielder.isDone());
    Assert.assertTrue(parallelMergeCombineYielder.isDone());
    while (pool.getRunningThreadCount() > 0) {
      Thread.sleep(100);
    }
    Assert.assertEquals(0, pool.getRunningThreadCount());
  }

  static class IntPair extends Pair<Integer, Integer>
  {
    IntPair(@Nullable Integer lhs, @Nullable Integer rhs)
    {
      super(lhs, rhs);
    }
  }
}
