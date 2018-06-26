/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package io.druid.java.util.common.guava;

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import io.druid.java.util.common.concurrent.Execs;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MergeWorkTaskTest
{
  @Test
  public void testNotParallelSequence() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );
    final List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9);

    SequenceTestHelper.testAll(() -> MergeWorkTask.parallelMerge(
        Ordering.natural(),
        testSeqs.stream(),
        999,
        ForkJoinPool.commonPool()
    ), expected);
  }

  @Test
  public void testOneBatchParallelSequence() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );
    final List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9);

    SequenceTestHelper.testAll(() -> MergeWorkTask.parallelMerge(
        Ordering.natural(),
        testSeqs.stream().parallel(),
        999,
        ForkJoinPool.commonPool()
    ), expected);
  }

  @Test
  public void testAllBatchParallelSequence() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );
    final List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9);

    SequenceTestHelper.testAll(() -> MergeWorkTask.parallelMerge(
        Ordering.natural(),
        testSeqs.stream().parallel(),
        1,
        ForkJoinPool.commonPool()
    ), expected);
  }

  @Test
  public void testSomeBatchParallelSequence() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );
    final List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9);

    SequenceTestHelper.testAll(() -> MergeWorkTask.parallelMerge(
        Ordering.natural(),
        testSeqs.stream().parallel(),
        2,
        ForkJoinPool.commonPool()
    ), expected);
  }


  @Test
  public void testFJPChoke() throws Exception
  {
    final ArrayList<TestSequence<Integer>> testSeqs = Lists.newArrayList(
        TestSequence.create(1, 3, 5, 7, 9),
        TestSequence.create(2, 8),
        TestSequence.create(4, 6, 8)
    );
    final List<Integer> expected = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 8, 9);
    final AtomicReference<Throwable> exception = new AtomicReference<>(null);
    final ForkJoinPool fjp = new ForkJoinPool(
        1,
        pool -> Execs.makeWorkerThread("test-%s", pool),
        (t, e) -> exception.set(e),
        false
    );
    SequenceTestHelper.testAll(() -> MergeWorkTask.parallelMerge(
        Ordering.natural(),
        testSeqs.stream().parallel(),
        1,
        fjp
    ), expected);
    fjp.shutdown();
    Assert.assertTrue(fjp.awaitTermination(5, TimeUnit.SECONDS));
    Assert.assertNull(exception.get());
  }

  @Test
  public void testBigMerge() throws Exception
  {
    final AtomicReference<Throwable> exception = new AtomicReference<>(null);
    final ForkJoinPool fjp = new ForkJoinPool(
        4,
        pool -> Execs.makeWorkerThread("test-%s", pool),
        (t, e) -> exception.set(e),
        false
    );

    // Take a big list of numbers, scatter them among a bunch of different buckets, then make sure the parallel merge
    // returns the original list

    final List<Integer> intList = IntStream.range(0, 10000).boxed().collect(Collectors.toList());
    final List<List<Integer>> listList = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      listList.add(new ArrayList<>());
    }
    final Random r = new Random(37489165L);
    intList.forEach(i -> listList.get(r.nextInt(listList.size())).add(i));
    SequenceTestHelper.testAll(() -> MergeWorkTask.parallelMerge(
        Ordering.natural(),
        listList.stream(
        ).map(
            TestSequence::create
        ).parallel(),
        10,
        fjp
    ), intList);
    fjp.shutdown();
    Assert.assertTrue(fjp.awaitTermination(5, TimeUnit.SECONDS));
    Assert.assertNull(exception.get());
  }
}
