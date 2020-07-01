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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;

public class ParallelMergeCombiningSequenceTest
{
  private static final int TEST_POOL_SIZE = 4;
  private static final Logger LOG = new Logger(ParallelMergeCombiningSequenceTest.class);

  public static final Ordering<IntPair> INT_PAIR_ORDERING = Ordering.natural().onResultOf(p -> p.lhs);
  public static final BinaryOperator<IntPair> INT_PAIR_MERGE_FN = (lhs, rhs) -> {
    if (lhs == null) {
      return rhs;
    }

    if (rhs == null) {
      return lhs;
    }

    return new IntPair(lhs.lhs, lhs.rhs + rhs.rhs);
  };

  private ForkJoinPool pool;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    pool = new ForkJoinPool(
        TEST_POOL_SIZE,
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        (t, e) -> LOG.error(e, "Unhandled exception in thread [%s]", t),
        true
    );
  }

  @After
  public void teardown()
  {
    pool.shutdown();
  }


  @Test
  public void testOrderedResultBatchFromSequence() throws IOException
  {
    Sequence<IntPair> rawSequence = nonBlockingSequence(5000);
    ParallelMergeCombiningSequence.YielderBatchedResultsCursor<IntPair> cursor =
        new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(
            new ParallelMergeCombiningSequence.SequenceBatcher<>(rawSequence, 128),
            INT_PAIR_ORDERING
        );
    cursor.initialize();
    Yielder<IntPair> rawYielder = Yielders.each(rawSequence);

    IntPair prev = null;
    while (!rawYielder.isDone() && !cursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), cursor.get());
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
    Sequence<IntPair> rawSequence = nonBlockingSequence(sequenceSize);
    ParallelMergeCombiningSequence.YielderBatchedResultsCursor<IntPair> cursor =
        new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(
            new ParallelMergeCombiningSequence.SequenceBatcher<>(rawSequence, 128),
            INT_PAIR_ORDERING
        );

    cursor.initialize();
    Yielder<IntPair> rawYielder = Yielders.each(rawSequence);

    ArrayBlockingQueue<ParallelMergeCombiningSequence.ResultBatch<IntPair>> outputQueue =
        new ArrayBlockingQueue<>((int) Math.ceil(((double) sequenceSize / batchSize) + 2));

    IntPair prev = null;
    ParallelMergeCombiningSequence.ResultBatch<IntPair> currentBatch =
        new ParallelMergeCombiningSequence.ResultBatch<>(batchSize);
    int batchCounter = 0;
    while (!rawYielder.isDone() && !cursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), cursor.get());
      Assert.assertNotEquals(cursor.get(), prev);
      prev = cursor.get();
      currentBatch.add(prev);
      batchCounter++;
      if (batchCounter >= batchSize) {
        outputQueue.offer(currentBatch);
        currentBatch = new ParallelMergeCombiningSequence.ResultBatch<>(batchSize);
        batchCounter = 0;
      }
      rawYielder = rawYielder.next(rawYielder.get());
      cursor.advance();
    }
    if (!currentBatch.isDrained()) {
      outputQueue.offer(currentBatch);
    }
    outputQueue.offer(ParallelMergeCombiningSequence.ResultBatch.TERMINAL);

    rawYielder.close();
    cursor.close();

    rawYielder = Yielders.each(rawSequence);

    Sequence<IntPair> queueAsSequence = ParallelMergeCombiningSequence.makeOutputSequenceForQueue(
        outputQueue,
        true,
        System.nanoTime() + TimeUnit.NANOSECONDS.convert(10_000, TimeUnit.MILLISECONDS),
        new ParallelMergeCombiningSequence.CancellationGizmo()
    );

    Yielder<IntPair> queueYielder = Yielders.each(queueAsSequence);

    while (!rawYielder.isDone() && !queueYielder.isDone()) {
      Assert.assertEquals(rawYielder.get(), queueYielder.get());
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
    Sequence<IntPair> rawSequence = nonBlockingSequence(sequenceSize);
    ParallelMergeCombiningSequence.YielderBatchedResultsCursor<IntPair> cursor =
        new ParallelMergeCombiningSequence.YielderBatchedResultsCursor<>(
            new ParallelMergeCombiningSequence.SequenceBatcher<>(rawSequence, 128),
            INT_PAIR_ORDERING
        );

    cursor.initialize();

    Yielder<IntPair> rawYielder = Yielders.each(rawSequence);

    ArrayBlockingQueue<ParallelMergeCombiningSequence.ResultBatch<IntPair>> outputQueue =
        new ArrayBlockingQueue<>((int) Math.ceil(((double) sequenceSize / batchSize) + 2));

    IntPair prev = null;
    ParallelMergeCombiningSequence.ResultBatch<IntPair> currentBatch =
        new ParallelMergeCombiningSequence.ResultBatch<>(batchSize);
    int batchCounter = 0;
    while (!rawYielder.isDone() && !cursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), cursor.get());
      Assert.assertNotEquals(cursor.get(), prev);
      prev = cursor.get();
      currentBatch.add(prev);
      batchCounter++;
      if (batchCounter >= batchSize) {
        outputQueue.offer(currentBatch);
        currentBatch = new ParallelMergeCombiningSequence.ResultBatch<>(batchSize);
        batchCounter = 0;
      }
      rawYielder = rawYielder.next(rawYielder.get());
      cursor.advance();
    }
    if (!currentBatch.isDrained()) {
      outputQueue.offer(currentBatch);
    }
    outputQueue.offer(ParallelMergeCombiningSequence.ResultBatch.TERMINAL);

    rawYielder.close();
    cursor.close();

    rawYielder = Yielders.each(rawSequence);

    ParallelMergeCombiningSequence.BlockingQueueuBatchedResultsCursor<IntPair> queueCursor =
        new ParallelMergeCombiningSequence.BlockingQueueuBatchedResultsCursor<>(
            outputQueue,
            INT_PAIR_ORDERING,
            false,
            -1L
        );
    queueCursor.initialize();
    prev = null;
    while (!rawYielder.isDone() && !queueCursor.isDone()) {
      Assert.assertEquals(rawYielder.get(), queueCursor.get());
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

    // above min sequence count threshold, so will merge in parallel
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
    input.add(nonBlockingSequence(5));
    assertResult(input);

    input.clear();

    // above min sequence count threshold, so will merge in parallel
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(Sequences.empty());
    input.add(nonBlockingSequence(5));
    assertResult(input);
  }

  @Test
  public void testAllInSingleBatch() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(6));
    assertResult(input, 10, 20, reportMetrics -> {
      Assert.assertEquals(1, reportMetrics.getParallelism());
      Assert.assertEquals(2, reportMetrics.getInputSequences());
      Assert.assertEquals(11, reportMetrics.getInputRows());
      // deltas because it depends how much result combining is happening, which is random
      Assert.assertEquals(6, reportMetrics.getOutputRows(), 5);
      Assert.assertEquals(4, reportMetrics.getTaskCount());
    });

    input.clear();

    // above min sequence count threshold, so will merge in parallel
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(6));
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(8));
    input.add(nonBlockingSequence(4));
    input.add(nonBlockingSequence(6));
    assertResult(input, 10, 20, reportMetrics -> {
      Assert.assertEquals(2, reportMetrics.getParallelism());
      Assert.assertEquals(6, reportMetrics.getInputSequences());
      Assert.assertEquals(34, reportMetrics.getInputRows());
      // deltas because it depends how much result combining is happening, which is random
      Assert.assertEquals(16, reportMetrics.getOutputRows(), 15);
      Assert.assertEquals(10, reportMetrics.getTaskCount(), 2);
    });
  }

  @Test
  public void testAllInSingleYield() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(6));
    assertResult(input, 4, 20, reportMetrics -> {
      Assert.assertEquals(1, reportMetrics.getParallelism());
      Assert.assertEquals(2, reportMetrics.getInputSequences());
      Assert.assertEquals(11, reportMetrics.getInputRows());
      // deltas because it depends how much result combining is happening, which is random
      Assert.assertEquals(6, reportMetrics.getOutputRows(), 5);
      Assert.assertEquals(4, reportMetrics.getTaskCount());
    });

    input.clear();

    // above min sequence count threshold, so will merge in parallel
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(6));
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(8));
    input.add(nonBlockingSequence(4));
    input.add(nonBlockingSequence(6));
    assertResult(input, 4, 20, reportMetrics -> {
      Assert.assertEquals(2, reportMetrics.getParallelism());
      Assert.assertEquals(6, reportMetrics.getInputSequences());
      Assert.assertEquals(34, reportMetrics.getInputRows());
      // deltas because it depends how much result combining is happening, which is random
      Assert.assertEquals(16, reportMetrics.getOutputRows(), 15);
      Assert.assertEquals(10, reportMetrics.getTaskCount(), 2);
    });
  }


  @Test
  public void testMultiBatchMultiYield() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(15));
    input.add(nonBlockingSequence(26));

    assertResult(input, 5, 10, reportMetrics -> {
      Assert.assertEquals(1, reportMetrics.getParallelism());
      Assert.assertEquals(2, reportMetrics.getInputSequences());
      Assert.assertEquals(41, reportMetrics.getInputRows());
      // deltas because it depends how much result combining is happening, which is random
      Assert.assertEquals(21, reportMetrics.getOutputRows(), 20);
      Assert.assertEquals(4, reportMetrics.getTaskCount(), 2);
    });

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(nonBlockingSequence(15));
    input.add(nonBlockingSequence(33));
    input.add(nonBlockingSequence(17));
    input.add(nonBlockingSequence(14));

    assertResult(input, 5, 10, reportMetrics -> {
      Assert.assertEquals(2, reportMetrics.getParallelism());
      Assert.assertEquals(6, reportMetrics.getInputSequences());
      Assert.assertEquals(120, reportMetrics.getInputRows());
      // deltas because it depends how much result combining is happening, which is random
      Assert.assertEquals(60, reportMetrics.getOutputRows(), 59);
      Assert.assertEquals(10, reportMetrics.getTaskCount(), 5);
    });
  }

  @Test
  public void testMixedSingleAndMultiYield() throws Exception
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(60));
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(8));

    assertResult(input, 5, 10);

    // above min sequence count threshold, so will merge in parallel (if enough cores)
    input.add(nonBlockingSequence(1));
    input.add(nonBlockingSequence(8));
    input.add(nonBlockingSequence(32));

    assertResult(input, 5, 10);
  }

  @Test
  public void testLongerSequencesJustForFun() throws Exception
  {

    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(10_000));
    input.add(nonBlockingSequence(9_001));

    assertResult(input, 128, 1024, reportMetrics -> {
      Assert.assertEquals(1, reportMetrics.getParallelism());
      Assert.assertEquals(2, reportMetrics.getInputSequences());
      Assert.assertEquals(19_001, reportMetrics.getInputRows());
    });

    input.add(nonBlockingSequence(7_777));
    input.add(nonBlockingSequence(8_500));
    input.add(nonBlockingSequence(5_000));
    input.add(nonBlockingSequence(8_888));

    assertResult(input, 128, 1024, reportMetrics -> {
      Assert.assertEquals(2, reportMetrics.getParallelism());
      Assert.assertEquals(6, reportMetrics.getInputSequences());
      Assert.assertEquals(49166, reportMetrics.getInputRows());
    });
  }

  @Test
  public void testExceptionOnInputSequenceRead() throws Exception
  {
    List<Sequence<IntPair>> input = new ArrayList<>();

    input.add(explodingSequence(15));
    input.add(nonBlockingSequence(25));


    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "exploded"
    );
    assertException(input);
  }

  @Test
  public void testExceptionOnInputSequenceRead2() throws Exception
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(25));
    input.add(explodingSequence(11));
    input.add(nonBlockingSequence(12));

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage(
        "exploded"
    );
    assertException(input);
  }

  @Test
  public void testExceptionFirstResultFromSequence() throws Exception
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(explodingSequence(0));
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));

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
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));

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
    input.add(nonBlockingSequence(someSize));
    input.add(nonBlockingSequence(someSize));
    input.add(nonBlockingSequence(someSize));
    input.add(blockingSequence(someSize, 400, 500, 1, 500, true));
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(Matchers.instanceOf(TimeoutException.class));
    expectedException.expectMessage("Sequence iterator timed out waiting for data");

    assertException(
        input,
        ParallelMergeCombiningSequence.DEFAULT_TASK_SMALL_BATCH_NUM_ROWS,
        ParallelMergeCombiningSequence.DEFAULT_TASK_INITIAL_YIELD_NUM_ROWS,
        1000L,
        0
    );
  }

  @Test
  public void testTimeoutExceptionDueToStalledReader() throws Exception
  {
    final int someSize = 2048;
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(someSize));
    input.add(nonBlockingSequence(someSize));
    input.add(nonBlockingSequence(someSize));
    input.add(nonBlockingSequence(someSize));

    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(Matchers.instanceOf(TimeoutException.class));
    expectedException.expectMessage("Sequence iterator timed out");
    assertException(input, 8, 64, 1000, 500);
  }

  @Test
  public void testGracefulCloseOfYielderCancelsPool() throws Exception
  {

    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(10_000));
    input.add(nonBlockingSequence(9_001));
    input.add(nonBlockingSequence(7_777));
    input.add(nonBlockingSequence(8_500));
    input.add(nonBlockingSequence(5_000));
    input.add(nonBlockingSequence(8_888));

    assertResultWithEarlyClose(input, 128, 1024, 256, reportMetrics -> {
      Assert.assertEquals(2, reportMetrics.getParallelism());
      Assert.assertEquals(6, reportMetrics.getInputSequences());
      // 49166 is total set of results if yielder were fully processed, expect somewhere more than 0 but less than that
      // this isn't super indicative of anything really, since closing the yielder would have triggered the baggage
      // to run, which runs this metrics reporter function, while the actual processing could still be occuring on the
      // pool in the background and the yielder still operates as intended if cancellation isn't in fact happening.
      // other tests ensure that this is true though (yielder.next throwing an exception for example)
      Assert.assertTrue(49166 > reportMetrics.getInputRows());
      Assert.assertTrue(0 < reportMetrics.getInputRows());
    });
  }


  private void assertResult(List<Sequence<IntPair>> sequences) throws InterruptedException, IOException
  {
    assertResult(
        sequences,
        ParallelMergeCombiningSequence.DEFAULT_TASK_SMALL_BATCH_NUM_ROWS,
        ParallelMergeCombiningSequence.DEFAULT_TASK_INITIAL_YIELD_NUM_ROWS,
        null
    );
  }

  private void assertResult(List<Sequence<IntPair>> sequences, int batchSize, int yieldAfter)
      throws InterruptedException, IOException
  {
    assertResult(
        sequences,
        batchSize,
        yieldAfter,
        null
    );
  }

  private void assertResult(
      List<Sequence<IntPair>> sequences,
      int batchSize,
      int yieldAfter,
      Consumer<ParallelMergeCombiningSequence.MergeCombineMetrics> reporter
  )
      throws InterruptedException, IOException
  {
    final CombiningSequence<IntPair> combiningSequence = CombiningSequence.create(
        new MergeSequence<>(INT_PAIR_ORDERING, Sequences.simple(sequences)),
        INT_PAIR_ORDERING,
        INT_PAIR_MERGE_FN
    );

    final ParallelMergeCombiningSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombiningSequence<>(
        pool,
        sequences,
        INT_PAIR_ORDERING,
        INT_PAIR_MERGE_FN,
        true,
        5000,
        0,
        TEST_POOL_SIZE,
        yieldAfter,
        batchSize,
        ParallelMergeCombiningSequence.DEFAULT_TASK_TARGET_RUN_TIME_MILLIS,
        reporter
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
    while (pool.getRunningThreadCount() > 0) {
      Thread.sleep(100);
    }
    Assert.assertEquals(0, pool.getRunningThreadCount());
    combiningYielder.close();
    parallelMergeCombineYielder.close();
    // cancellation trigger should not be set if sequence was fully yielded and close is called
    // (though shouldn't actually matter even if it was...)
    Assert.assertFalse(parallelMergeCombineSequence.getCancellationGizmo().isCancelled());
  }

  private void assertResultWithEarlyClose(
      List<Sequence<IntPair>> sequences,
      int batchSize,
      int yieldAfter,
      int closeYielderAfter,
      Consumer<ParallelMergeCombiningSequence.MergeCombineMetrics> reporter
  )
      throws InterruptedException, IOException
  {
    final CombiningSequence<IntPair> combiningSequence = CombiningSequence.create(
        new MergeSequence<>(INT_PAIR_ORDERING, Sequences.simple(sequences)),
        INT_PAIR_ORDERING,
        INT_PAIR_MERGE_FN
    );

    final ParallelMergeCombiningSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombiningSequence<>(
        pool,
        sequences,
        INT_PAIR_ORDERING,
        INT_PAIR_MERGE_FN,
        true,
        5000,
        0,
        TEST_POOL_SIZE,
        yieldAfter,
        batchSize,
        ParallelMergeCombiningSequence.DEFAULT_TASK_TARGET_RUN_TIME_MILLIS,
        reporter
    );

    Yielder<IntPair> combiningYielder = Yielders.each(combiningSequence);
    Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);

    IntPair prev = null;

    int yields = 0;
    while (!combiningYielder.isDone() && !parallelMergeCombineYielder.isDone()) {
      if (yields >= closeYielderAfter) {
        parallelMergeCombineYielder.close();
        combiningYielder.close();
        break;
      } else {
        yields++;
        Assert.assertEquals(combiningYielder.get(), parallelMergeCombineYielder.get());
        Assert.assertNotEquals(parallelMergeCombineYielder.get(), prev);
        prev = parallelMergeCombineYielder.get();
        combiningYielder = combiningYielder.next(combiningYielder.get());
        parallelMergeCombineYielder = parallelMergeCombineYielder.next(parallelMergeCombineYielder.get());
      }
    }
    // trying to next the yielder creates sadness for you
    final String expectedExceptionMsg = "Already closed";
    try {
      Assert.assertEquals(combiningYielder.get(), parallelMergeCombineYielder.get());
      parallelMergeCombineYielder.next(parallelMergeCombineYielder.get());
      // this should explode so the contradictory next statement should not be reached
      Assert.assertTrue(false);
    }
    catch (RuntimeException rex) {
      Assert.assertEquals(expectedExceptionMsg, rex.getMessage());
    }

    // cancellation gizmo of sequence should be cancelled, and also should contain our expected message
    Assert.assertTrue(parallelMergeCombineSequence.getCancellationGizmo().isCancelled());
    Assert.assertEquals(
        expectedExceptionMsg,
        parallelMergeCombineSequence.getCancellationGizmo().getRuntimeException().getMessage()
    );

    while (pool.getRunningThreadCount() > 0) {
      Thread.sleep(100);
    }
    Assert.assertEquals(0, pool.getRunningThreadCount());

    Assert.assertFalse(combiningYielder.isDone());
    Assert.assertFalse(parallelMergeCombineYielder.isDone());
  }

  private void assertException(List<Sequence<IntPair>> sequences) throws Exception
  {
    assertException(
        sequences,
        ParallelMergeCombiningSequence.DEFAULT_TASK_SMALL_BATCH_NUM_ROWS,
        ParallelMergeCombiningSequence.DEFAULT_TASK_INITIAL_YIELD_NUM_ROWS,
        5000L,
        0
    );
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
          INT_PAIR_ORDERING,
          INT_PAIR_MERGE_FN,
          true,
          timeout,
          0,
          TEST_POOL_SIZE,
          yieldAfter,
          batchSize,
          ParallelMergeCombiningSequence.DEFAULT_TASK_TARGET_RUN_TIME_MILLIS,
          null
      );

      Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);

      IntPair prev = null;

      while (!parallelMergeCombineYielder.isDone()) {
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
      sequences.forEach(sequence -> {
        if (sequence instanceof ExplodingSequence) {
          ExplodingSequence exploder = (ExplodingSequence) sequence;
          Assert.assertEquals(1, exploder.getCloseCount());
        }
      });
      LOG.warn(ex, "exception:");
      throw ex;
    }
  }

  public static class IntPair extends Pair<Integer, Integer>
  {
    private IntPair(Integer lhs, Integer rhs)
    {
      super(lhs, rhs);
    }
  }

  /**
   * Generate an ordered, random valued, non-blocking sequence of {@link IntPair}, optionally lazy generated with
   * the implication that every time a sequence is accumulated or yielded it produces <b>different</b> results,
   * which sort of breaks the {@link Sequence} contract, and makes this method useless for tests in lazy mode,
   * however it is useful for benchmarking, where having a sequence without having to materialize the entire thing
   * up front on heap with a {@link List} backing is preferable.
   */
  public static Sequence<IntPair> nonBlockingSequence(int size, boolean lazyGenerate)
  {
    List<IntPair> pairs = lazyGenerate ? null : generateOrderedPairs(size);
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<IntPair, Iterator<IntPair>>()
        {
          @Override
          public Iterator<IntPair> make()
          {
            return new Iterator<IntPair>()
            {
              int mergeKey = 0;
              int rowCounter = 0;
              @Override
              public boolean hasNext()
              {
                return rowCounter < size;
              }

              @Override
              public IntPair next()
              {
                if (lazyGenerate) {
                  rowCounter++;
                  mergeKey += incrementMergeKeyAmount();
                  return makeIntPair(mergeKey);
                } else {
                  return pairs.get(rowCounter++);
                }
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

  /**
   * Generate an ordered, random valued, blocking sequence of {@link IntPair}, optionally lazy generated. See
   * {@link ParallelMergeCombiningSequenceTest#nonBlockingSequence(int)} for the implications of lazy generating a
   * sequence, to summarize each time the sequence is accumulated or yielded it produces different results.
   *
   * This sequence simulates blocking using {@link Thread#sleep(long)}, with an initial millisecond delay range defined
   * by {@param startDelayStartMillis} and {@param startDelayEndMillis} that defines how long to block before the first
   * sequence value will be produced, and {@param maxIterationDelayMillis} that defines how long to block every
   * {@param iterationDelayFrequency} rows.
   */
  public static Sequence<IntPair> blockingSequence(
      int size,
      int startDelayStartMillis,
      int startDelayEndMillis,
      int iterationDelayFrequency,
      int maxIterationDelayMillis,
      boolean lazyGenerate
  )
  {
    final List<IntPair> pairs = lazyGenerate ? null : generateOrderedPairs(size);
    final long startDelayMillis = ThreadLocalRandom.current().nextLong(startDelayStartMillis, startDelayEndMillis);
    final long delayUntil = System.nanoTime() + TimeUnit.NANOSECONDS.convert(startDelayMillis, TimeUnit.MILLISECONDS);
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<IntPair, Iterator<IntPair>>()
        {
          @Override
          public Iterator<IntPair> make()
          {
            return new Iterator<IntPair>()
            {
              int mergeKey = 0;
              int rowCounter = 0;
              @Override
              public boolean hasNext()
              {
                return rowCounter < size;
              }

              @Override
              public IntPair next()
              {
                try {
                  final long currentNano = System.nanoTime();
                  if (rowCounter == 0 && currentNano < delayUntil) {
                    final long sleepMillis = Math.max(
                        TimeUnit.MILLISECONDS.convert(delayUntil - currentNano, TimeUnit.NANOSECONDS),
                        1
                    );
                    Thread.sleep(sleepMillis);
                  } else if (maxIterationDelayMillis > 0
                             && rowCounter % iterationDelayFrequency == 0
                             && ThreadLocalRandom.current().nextBoolean()) {
                    final int delayMillis = Math.max(ThreadLocalRandom.current().nextInt(maxIterationDelayMillis), 1);
                    Thread.sleep(delayMillis);
                  }
                }
                catch (InterruptedException ex) {
                  throw new RuntimeException(ex);
                }
                if (lazyGenerate) {
                  rowCounter++;
                  mergeKey += incrementMergeKeyAmount();
                  return makeIntPair(mergeKey);
                } else {
                  return pairs.get(rowCounter++);
                }
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

  /**
   * Genenerate non-blocking sequence for tests, non-lazy so the sequence produces consistent results
   */
  private static Sequence<IntPair> nonBlockingSequence(int size)
  {
    return nonBlockingSequence(size, false);
  }

  /**
   * Genenerate a sequence that explodes after {@param explodeAfter} rows
   */
  private static Sequence<IntPair> explodingSequence(int explodeAfter)
  {
    final int explodeAt = explodeAfter + 1;

    // we start at one because we only need to close if the sequence is actually made
    AtomicInteger explodedIteratorMakerCleanup = new AtomicInteger(1);

    // just hijacking this class to use it's interface... which i override..
    return new ExplodingSequence(
        new BaseSequence<>(
          new BaseSequence.IteratorMaker<IntPair, Iterator<IntPair>>()
          {
            @Override
            public Iterator<IntPair> make()
            {
              // we got yielder, decrement so we expect it to be incremented again on cleanup
              explodedIteratorMakerCleanup.decrementAndGet();
              return new Iterator<IntPair>()
              {
                int mergeKey = 0;
                int rowCounter = 0;
                @Override
                public boolean hasNext()
                {
                  return rowCounter < explodeAt;
                }

                @Override
                public IntPair next()
                {
                  if (rowCounter == explodeAfter) {
                    throw new RuntimeException("exploded");
                  }
                  mergeKey += incrementMergeKeyAmount();
                  rowCounter++;
                  return makeIntPair(mergeKey);
                }
              };
            }

            @Override
            public void cleanup(Iterator<IntPair> iterFromMake)
            {
              explodedIteratorMakerCleanup.incrementAndGet();
            }
          }
        ),
        false,
        false
    )
    {
      @Override
      public long getCloseCount()
      {
        return explodedIteratorMakerCleanup.get();
      }
    };
  }

  private static List<IntPair> generateOrderedPairs(int length)
  {
    int rowCounter = 0;
    int mergeKey = 0;
    List<IntPair> generatedSequence = new ArrayList<>(length);
    while (rowCounter < length) {
      mergeKey += incrementMergeKeyAmount();
      generatedSequence.add(makeIntPair(mergeKey));
      rowCounter++;
    }
    return generatedSequence;
  }

  private static int incrementMergeKeyAmount()
  {
    return ThreadLocalRandom.current().nextInt(1, 3);
  }

  private static IntPair makeIntPair(int mergeKey)
  {
    return new IntPair(mergeKey, ThreadLocalRandom.current().nextInt(1, 100));
  }
}
