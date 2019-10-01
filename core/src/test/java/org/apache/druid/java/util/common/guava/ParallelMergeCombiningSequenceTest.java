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
import org.apache.druid.java.util.common.logger.Logger;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;

public class ParallelMergeCombiningSequenceTest
{
  private static final Logger LOG = new Logger(ParallelMergeCombiningSequenceTest.class);
  private static final int DEFAULT_TEST_YIELD_AFTER = 1024;
  private static final int DEFAULT_TEST_BATCH_SIZE = 128;

  private ForkJoinPool pool;

  private final Ordering<IntPair> ordering = Ordering.natural().onResultOf(p -> p.lhs);
  private final BinaryOperator<IntPair> mergeFn = (lhs, rhs) -> {
    if (lhs == null) {
      return rhs;
    }

    if (rhs == null) {
      return lhs;
    }

    return new IntPair(lhs.lhs, lhs.rhs + rhs.rhs);
  };

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

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testOrderedResultBatchFromSequence() throws IOException
  {
    Sequence<IntPair> rawSequence = generateOrderedPairsSequence(5000);
    Yielder<ParallelMergeCombiningSequence.OrderedResultBatch<IntPair>> batchYielder =
        ParallelMergeCombiningSequence.OrderedResultBatch.fromSequence(rawSequence, 128);

    ParallelMergeCombiningSequence.YielderBatchedResultsCursor<IntPair> cursor =
        new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(batchYielder, ordering);

    Yielder<IntPair> rawYielder = Yielders.each(rawSequence);

    IntPair prev = null;
    while (!rawYielder.isDone() && !cursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), cursor.get());
      LOG.debug("%s", cursor.get());
      Assert.assertNotEquals(cursor.get(), prev);
      prev = cursor.get();
      rawYielder = rawYielder.next(rawYielder.get());
      cursor.advance();
    }
    cursor.close();
    rawYielder.close();
  }

  @Test
  public void testOrderedResultBatchFromSequenceBackToYielderOnSequence() throws IOException
  {
    final int batchSize = 128;
    final int sequenceSize = 5_000;
    Sequence<IntPair> rawSequence = generateOrderedPairsSequence(sequenceSize);
    Yielder<ParallelMergeCombiningSequence.OrderedResultBatch<IntPair>> batchYielder =
        ParallelMergeCombiningSequence.OrderedResultBatch.fromSequence(rawSequence, batchSize);

    ParallelMergeCombiningSequence.YielderBatchedResultsCursor<IntPair> cursor =
        new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(batchYielder, ordering);


    Yielder<IntPair> rawYielder = Yielders.each(rawSequence);

    ArrayBlockingQueue<ParallelMergeCombiningSequence.OrderedResultBatch<IntPair>> outputQueue =
        new ArrayBlockingQueue<>((int) Math.ceil(((double) sequenceSize / batchSize) + 2));

    IntPair prev = null;
    ParallelMergeCombiningSequence.OrderedResultBatch<IntPair> currentBatch =
        new ParallelMergeCombiningSequence.OrderedResultBatch<>(batchSize);
    int batchCounter = 0;
    while (!rawYielder.isDone() && !cursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), cursor.get());
      LOG.debug("%s", cursor.get());
      Assert.assertNotEquals(cursor.get(), prev);
      prev = cursor.get();
      currentBatch.add(prev);
      batchCounter++;
      if (batchCounter >= batchSize) {
        outputQueue.offer(currentBatch);
        currentBatch = new ParallelMergeCombiningSequence.OrderedResultBatch<>(batchSize);
        batchCounter = 0;
      }
      rawYielder = rawYielder.next(rawYielder.get());
      cursor.advance();
    }
    if (!currentBatch.isDrained()) {
      outputQueue.offer(currentBatch);
    }
    outputQueue.offer(new ParallelMergeCombiningSequence.OrderedResultBatch<>());

    rawYielder.close();
    cursor.close();

    rawYielder = Yielders.each(rawSequence);

    Sequence<IntPair> queueAsSequence = ParallelMergeCombiningSequence.makeOutputSequenceForQueue(
        outputQueue,
        true,
        System.currentTimeMillis() + 10_000,
        new ParallelMergeCombiningSequence.CancellationGizmo()
    );

    Yielder<IntPair> queueYielder = Yielders.each(queueAsSequence);

    int rowCtr = 0;
    while (!rawYielder.isDone() && !queueYielder.isDone()) {
      Assert.assertEquals(rawYielder.get(), queueYielder.get());
      LOG.debug("row %s: %s", rowCtr++, queueYielder.get());
      Assert.assertNotEquals(queueYielder.get(), prev);
      prev = queueYielder.get();
      rawYielder = rawYielder.next(rawYielder.get());
      queueYielder = queueYielder.next(queueYielder.get());
    }

    rawYielder.close();
    queueYielder.close();
  }

  @Test
  public void testOrderedResultBatchFromSequenceToBlockingQueueCursor() throws IOException
  {
    final int batchSize = 128;
    final int sequenceSize = 5_000;
    Sequence<IntPair> rawSequence = generateOrderedPairsSequence(sequenceSize);
    Yielder<ParallelMergeCombiningSequence.OrderedResultBatch<IntPair>> batchYielder =
        ParallelMergeCombiningSequence.OrderedResultBatch.fromSequence(rawSequence, batchSize);

    ParallelMergeCombiningSequence.YielderBatchedResultsCursor<IntPair> cursor =
        new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(batchYielder, ordering);


    Yielder<IntPair> rawYielder = Yielders.each(rawSequence);

    ArrayBlockingQueue<ParallelMergeCombiningSequence.OrderedResultBatch<IntPair>> outputQueue =
        new ArrayBlockingQueue<>((int) Math.ceil(((double) sequenceSize / batchSize) + 2));

    IntPair prev = null;
    ParallelMergeCombiningSequence.OrderedResultBatch<IntPair> currentBatch =
        new ParallelMergeCombiningSequence.OrderedResultBatch<>(batchSize);
    int batchCounter = 0;
    while (!rawYielder.isDone() && !cursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), cursor.get());
      LOG.debug("%s", cursor.get());
      Assert.assertNotEquals(cursor.get(), prev);
      prev = cursor.get();
      currentBatch.add(prev);
      batchCounter++;
      if (batchCounter >= batchSize) {
        outputQueue.offer(currentBatch);
        currentBatch = new ParallelMergeCombiningSequence.OrderedResultBatch<>(batchSize);
        batchCounter = 0;
      }
      rawYielder = rawYielder.next(rawYielder.get());
      cursor.advance();
    }
    if (!currentBatch.isDrained()) {
      outputQueue.offer(currentBatch);
    }
    outputQueue.offer(new ParallelMergeCombiningSequence.OrderedResultBatch<>());

    rawYielder.close();
    cursor.close();

    rawYielder = Yielders.each(rawSequence);

    ParallelMergeCombiningSequence.BlockingQueueuBatchedResultsCursor<IntPair> queueCursor =
        new ParallelMergeCombiningSequence.BlockingQueueuBatchedResultsCursor<>(
            outputQueue,
            ordering,
            false,
            -1L
        );
    queueCursor.initialize();
    prev = null;
    while (!rawYielder.isDone() && !queueCursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), queueCursor.get());
      LOG.debug("%s", queueCursor.get());
      Assert.assertNotEquals(queueCursor.get(), prev);
      prev = queueCursor.get();
      rawYielder = rawYielder.next(rawYielder.get());
      queueCursor.advance();
    }
    rawYielder.close();
    queueCursor.close();
  }

  @Test
  public void testNone() throws Exception
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    assertResult(input);
  }

  @Test
  public void testEmpties() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    assertResult(input);

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    assertResult(input);
  }

  @Test
  public void testEmptiesAndNonEmpty() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(Sequences.empty());
    input.add(generateOrderedPairsSequence(5));
    assertResult(input);

    input.clear();

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(generateOrderedPairsSequence(5));
    assertResult(input);
  }

  @Test
  public void testAllInSingleBatch() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(6));
    assertResult(input, 10, 20);

    input.clear();

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(6));
    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(8));
    input.add(generateOrderedPairsSequence(4));
    input.add(generateOrderedPairsSequence(6));
    assertResult(input, 10, 20);
  }

  @Test
  public void testAllInSingleYield() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(6));
    assertResult(input, 4, 20);

    input.clear();

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(6));
    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(8));
    input.add(generateOrderedPairsSequence(4));
    input.add(generateOrderedPairsSequence(6));
    assertResult(input, 4, 20);
  }


  @Test
  public void testMultiBatchMultiYield() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(generateOrderedPairsSequence(15));
    input.add(generateOrderedPairsSequence(26));

    assertResult(input, 5, 10);

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(generateOrderedPairsSequence(15));
    input.add(generateOrderedPairsSequence(33));
    input.add(generateOrderedPairsSequence(17));
    input.add(generateOrderedPairsSequence(14));

    assertResult(input, 5, 10);
  }

  @Test
  public void testMixedSingleAndMultiYield() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(generateOrderedPairsSequence(60));
    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(8));

    assertResult(input, 5, 10);

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(generateOrderedPairsSequence(1));
    input.add(generateOrderedPairsSequence(8));
    input.add(generateOrderedPairsSequence(32));

    assertResult(input, 5, 10);
  }

  @Test
  public void testLongerSequencesJustForFun() throws Exception
  {

    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(generateOrderedPairsSequence(10_000));
    input.add(generateOrderedPairsSequence(9_001));

    assertResult(input, 128, 1024);

    input.add(generateOrderedPairsSequence(7_777));
    input.add(generateOrderedPairsSequence(8_500));
    input.add(generateOrderedPairsSequence(5_000));
    input.add(generateOrderedPairsSequence(8_888));

    assertResult(input, 128, 1024);
  }

  @Test
  public void testExceptionOnInputSequenceRead() throws Exception
  {
    List<Sequence<IntPair>> input = new ArrayList<>();

    input.add(explodingSequence(15));
    input.add(generateOrderedPairsSequence(25));


    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "exploded"
    );
    assertException(input);

    input.add(generateOrderedPairsSequence(5));
    input.add(generateOrderedPairsSequence(25));
    input.add(explodingSequence(11));
    input.add(generateOrderedPairsSequence(12));

    assertException(input);
  }

  @Test
  public void testExceptionFirstResultFromSequence() throws Exception
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(explodingSequence(0));
    input.add(generateOrderedPairsSequence(2));
    input.add(generateOrderedPairsSequence(2));
    input.add(generateOrderedPairsSequence(2));

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "exploded"
    );
    assertException(input);
  }

  @Test
  public void testExceptionFirstResultFromMultipleSequence() throws Exception
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(explodingSequence(0));
    input.add(explodingSequence(0));
    input.add(explodingSequence(0));
    input.add(generateOrderedPairsSequence(2));
    input.add(generateOrderedPairsSequence(2));
    input.add(generateOrderedPairsSequence(2));

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "exploded"
    );
    assertException(input);
  }

  @Test
  public void testTimeoutExceptionDueToStalledInput() throws Exception
  {
    final int someSize = 2048;
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(generateOrderedPairsSequence(someSize));
    input.add(generateOrderedPairsSequence(someSize));
    input.add(generateOrderedPairsSequence(someSize));
    input.add(slowSequence(someSize, 500));

    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(Matchers.instanceOf(TimeoutException.class));
    expectedException.expectMessage("Sequence iterator timed out waiting for data");

    assertException(input, DEFAULT_TEST_BATCH_SIZE, DEFAULT_TEST_YIELD_AFTER, 1000L, 0);
  }

  @Test
  public void testTimeoutExceptionDueToStalledReader() throws Exception
  {
    final int someSize = 2048;
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(generateOrderedPairsSequence(someSize));
    input.add(generateOrderedPairsSequence(someSize));
    input.add(generateOrderedPairsSequence(someSize));
    input.add(generateOrderedPairsSequence(someSize));

    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(Matchers.instanceOf(TimeoutException.class));
    expectedException.expectMessage("Sequence iterator timed out");
    assertException(input, 8, 64, 1000, 500);
  }

  private void assertResult(List<Sequence<IntPair>> sequences) throws InterruptedException, IOException
  {
    assertResult(sequences, DEFAULT_TEST_BATCH_SIZE, DEFAULT_TEST_YIELD_AFTER);
  }

  private void assertResult(List<Sequence<IntPair>> sequences, int batchSize, int yieldAfter)
      throws InterruptedException, IOException
  {
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
        true,
        5000,
        0,
        Runtime.getRuntime().availableProcessors() - 1,
        yieldAfter,
        batchSize
    );

    Yielder<IntPair> combiningYielder = Yielders.each(combiningSequence);
    Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);

    IntPair prev = null;

    while (!combiningYielder.isDone() && !parallelMergeCombineYielder.isDone()) {
      Assert.assertEquals(combiningYielder.get(), parallelMergeCombineYielder.get());
      LOG.debug("%s", parallelMergeCombineYielder.get());
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
    combiningYielder.close();
    parallelMergeCombineYielder.close();
  }

  private void assertException(List<Sequence<IntPair>> sequences) throws Exception
  {
    assertException(sequences, DEFAULT_TEST_BATCH_SIZE, DEFAULT_TEST_YIELD_AFTER, 5000L, 0);
  }

  private void assertException(
      List<Sequence<IntPair>> sequences,
      int batchSize,
      int yieldAfter,
      long timeout,
      int readDelayMillis
  )
      throws Exception
  {
    try {
      final ParallelMergeCombiningSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombiningSequence<>(
          pool,
          sequences,
          ordering,
          mergeFn,
          true,
          timeout,
          0,
          Runtime.getRuntime().availableProcessors() - 1,
          yieldAfter,
          batchSize
      );

      Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);

      IntPair prev = null;

      while (!parallelMergeCombineYielder.isDone()) {
        LOG.debug("%s", parallelMergeCombineYielder.get());
        Assert.assertNotEquals(parallelMergeCombineYielder.get(), prev);
        prev = parallelMergeCombineYielder.get();
        if (readDelayMillis > 0 && ThreadLocalRandom.current().nextBoolean()) {
          Thread.sleep(readDelayMillis);
        }
        parallelMergeCombineYielder = parallelMergeCombineYielder.next(parallelMergeCombineYielder.get());
      }
      parallelMergeCombineYielder.close();
    }
    catch (Exception ex) {
      LOG.warn(ex, "actual exception:");
      throw ex;
    }
  }

  static class IntPair extends Pair<Integer, Integer>
  {
    IntPair(@Nullable Integer lhs, @Nullable Integer rhs)
    {
      super(lhs, rhs);
    }
  }

  private static List<IntPair> generateOrderedPairs(int length)
  {
    int counter = 0;
    int i = 0;
    List<IntPair> generatedSequence = new ArrayList<>(length);
    while (counter < length) {
      i++;
      if (ThreadLocalRandom.current().nextBoolean()) {
        generatedSequence.add(new IntPair(i, ThreadLocalRandom.current().nextInt(1, 100)));
        counter++;
      }
    }
    return generatedSequence;
  }

  private static Sequence<IntPair> generateOrderedPairsSequence(int length)
  {
    return Sequences.simple(generateOrderedPairs(length));
  }

  private static Sequence<IntPair> explodingSequence(int explodeAfter)
  {
    List<IntPair> items = generateOrderedPairs(explodeAfter + 1);
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<IntPair, Iterator<IntPair>>()
        {
          @Override
          public Iterator<IntPair> make()
          {
            return new Iterator<IntPair>()
            {
              int i = 0;
              @Override
              public boolean hasNext()
              {
                return i < items.size();
              }

              @Override
              public IntPair next()
              {
                if (i == explodeAfter) {
                  throw new RuntimeException("exploded");
                }
                return items.get(i++);
              }
            };
          }

          @Override
          public void cleanup(Iterator<IntPair> iterFromMake)
          {
            // nothing to cleanup
          }
        }
    );
  }

  private static Sequence<IntPair> slowSequence(int size, int delay)
  {
    List<IntPair> items = generateOrderedPairs(size);
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<IntPair, Iterator<IntPair>>()
        {
          @Override
          public Iterator<IntPair> make()
          {
            return new Iterator<IntPair>()
            {
              int i = 0;
              @Override
              public boolean hasNext()
              {
                return i < items.size();
              }

              @Override
              public IntPair next()
              {
                if (ThreadLocalRandom.current().nextBoolean()) {
                  try {
                    Thread.sleep(delay);
                  }
                  catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                  }
                }
                return items.get(i++);
              }
            };
          }

          @Override
          public void cleanup(Iterator<IntPair> iterFromMake)
          {
            // nothing to cleanup
          }
        }
    );
  }
}
