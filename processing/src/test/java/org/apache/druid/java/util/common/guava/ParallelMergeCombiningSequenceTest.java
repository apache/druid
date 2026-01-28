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
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.utils.JvmUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
  public void testOrderedResultBatchFromSequenceBacktoYielderOnSequence() throws IOException
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
    outputQueue.offer(ParallelMergeCombiningSequence.ResultBatch.terminal());

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
    outputQueue.offer(ParallelMergeCombiningSequence.ResultBatch.terminal());

    rawYielder.close();
    cursor.close();

    rawYielder = Yielders.each(rawSequence);

    ParallelMergeCombiningSequence.CancellationGizmo gizmo = new ParallelMergeCombiningSequence.CancellationGizmo();
    ParallelMergeCombiningSequence.BlockingQueueuBatchedResultsCursor<IntPair> queueCursor =
        new ParallelMergeCombiningSequence.BlockingQueueuBatchedResultsCursor<>(
            outputQueue,
            gizmo,
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
  public void testNone() throws IOException
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    assertResult(input);
  }

  @Test
  public void testEmpties() throws IOException
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
  public void testEmptiesAndNonEmpty() throws IOException
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
  public void testMergeCombineMetricsAccumulatorNPEOnBadExecutorPool()
  {
    // below min threshold, so will merge serially
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(6));
    // Simulates the bad/occupied executor pool, it does not execute any task submitted to it
    ForkJoinPool customBadPool = new ForkJoinPool(
        1,
        pool -> null,
        (t, e) -> LOG.error(e, "Unhandled exception in thread [%s]", t),
        true
    );
    Throwable t = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> assertResultWithCustomPool(input, 10, 20, reportMetrics -> {}, customBadPool)
    );
    Assert.assertEquals(
        "Query did not complete within configured timeout period. You can increase query timeout or tune the performance of query.",
        t.getMessage()
    );
    customBadPool.shutdown();
  }

  @Test
  public void testAllInSingleBatch() throws IOException
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
  public void testAllInSingleYield() throws IOException
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
  public void testMultiBatchMultiYield() throws IOException
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
  public void testMixedSingleAndMultiYield() throws IOException
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
  public void testLongerSequencesJustForFun() throws IOException
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
  public void testExceptionOnInputSequenceRead()
  {
    List<Sequence<IntPair>> input = new ArrayList<>();

    input.add(explodingSequence(15));
    input.add(nonBlockingSequence(25));

    Throwable t = Assert.assertThrows(RuntimeException.class, () -> assertException(input));
    Assert.assertEquals("exploded", t.getMessage());
    Assert.assertTrue(pool.awaitQuiescence(1, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
  }

  @Test
  public void testExceptionOnInputSequenceRead2()
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(5));
    input.add(nonBlockingSequence(25));
    input.add(explodingSequence(11));
    input.add(nonBlockingSequence(12));

    Throwable t = Assert.assertThrows(RuntimeException.class, () -> assertException(input));
    Assert.assertEquals("exploded", t.getMessage());
    Assert.assertTrue(pool.awaitQuiescence(1, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
  }

  @Test
  public void testExceptionFirstResultFromSequence()
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(explodingSequence(0));
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));

    Throwable t = Assert.assertThrows(RuntimeException.class, () -> assertException(input));
    Assert.assertEquals("exploded", t.getMessage());
    Assert.assertTrue(pool.awaitQuiescence(1, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
  }

  @Test
  public void testExceptionFirstResultFromMultipleSequence()
  {
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(explodingSequence(0));
    input.add(explodingSequence(0));
    input.add(explodingSequence(0));
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));
    input.add(nonBlockingSequence(2));

    Throwable t = Assert.assertThrows(RuntimeException.class, () -> assertException(input));
    Assert.assertEquals("exploded", t.getMessage());
    Assert.assertTrue(pool.awaitQuiescence(1, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
  }

  @Test
  public void testTimeoutExceptionDueToStalledInput()
  {
    final int someSize = 2048;
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(someSize));
    input.add(nonBlockingSequence(someSize));
    input.add(nonBlockingSequence(someSize));
    input.add(blockingSequence(someSize, 400, 500, 1, 500, true));

    Throwable t = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> assertException(
            input,
            ParallelMergeCombiningSequence.DEFAULT_TASK_SMALL_BATCH_NUM_ROWS,
            ParallelMergeCombiningSequence.DEFAULT_TASK_INITIAL_YIELD_NUM_ROWS,
            1000L,
            0
        )
    );
    Assert.assertEquals("Query did not complete within configured timeout period. " +
                        "You can increase query timeout or tune the performance of query.", t.getMessage());


    // these tests when run in java 11, 17 and maybe others in between 8 and 20 don't seem to correctly clean up the
    // pool, however this behavior is flaky and doesn't always happen so we can't definitively assert that the pool is
    // or isn't
    if (JvmUtils.majorVersion() >= 20 || JvmUtils.majorVersion() < 9) {
      Assert.assertTrue(pool.awaitQuiescence(3, TimeUnit.SECONDS));
      // good result, we want the pool to always be idle if an exception occurred during processing
      Assert.assertTrue(pool.isQuiescent());
    }
  }

  @Test
  public void testTimeoutExceptionDueToSlowReader()
  {
    final int someSize = 50_000;
    List<Sequence<IntPair>> input = new ArrayList<>();
    input.add(nonBlockingSequence(someSize, true));
    input.add(nonBlockingSequence(someSize, true));
    input.add(nonBlockingSequence(someSize, true));
    input.add(nonBlockingSequence(someSize, true));

    Throwable t = Assert.assertThrows(QueryTimeoutException.class, () -> assertException(input, 8, 64, 1000, 1500));
    Assert.assertEquals("Query did not complete within configured timeout period. " +
                        "You can increase query timeout or tune the performance of query.", t.getMessage());
    Assert.assertTrue(pool.awaitQuiescence(1, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
  }

  @Test
  public void testTimeoutExceptionDueToStoppedReader() throws InterruptedException
  {
    final int someSize = 150_000;
    final int timeout = 5_000;
    List<TestingReporter> reporters = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      List<Sequence<IntPair>> input = new ArrayList<>();
      input.add(nonBlockingSequence(someSize, true));
      input.add(nonBlockingSequence(someSize, true));
      input.add(nonBlockingSequence(someSize, true));
      input.add(nonBlockingSequence(someSize, true));

      TestingReporter reporter = new TestingReporter();
      final ParallelMergeCombiningSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombiningSequence<>(
          pool,
          input,
          INT_PAIR_ORDERING,
          INT_PAIR_MERGE_FN,
          true,
          timeout,
          0,
          TEST_POOL_SIZE,
          512,
          128,
          ParallelMergeCombiningSequence.DEFAULT_TASK_TARGET_RUN_TIME_MILLIS,
          reporter
      );
      Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);
      reporter.future = parallelMergeCombineSequence.getCancellationFuture();
      reporter.yielder = parallelMergeCombineYielder;
      reporter.yielder = parallelMergeCombineYielder.next(null);
      Assert.assertFalse(parallelMergeCombineYielder.isDone());
      reporters.add(reporter);
    }

    // sleep until timeout
    Thread.sleep(timeout);
    Assert.assertTrue(pool.awaitQuiescence(10, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
    Assert.assertFalse(pool.hasQueuedSubmissions());
    for (TestingReporter reporter : reporters) {
      Assert.assertThrows(QueryTimeoutException.class, () -> reporter.yielder.next(null));
      Assert.assertTrue(reporter.future.isCancelled());
      Assert.assertTrue(reporter.future.getCancellationGizmo().isCanceled());
    }
    Assert.assertTrue(pool.awaitQuiescence(10, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
  }

  @Test
  public void testManyBigSequencesAllAtOnce() throws IOException
  {
    final int someSize = 50_000;
    List<TestingReporter> reporters = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      List<Sequence<IntPair>> input = new ArrayList<>();
      input.add(nonBlockingSequence(someSize, true));
      input.add(nonBlockingSequence(someSize, true));
      input.add(nonBlockingSequence(someSize, true));
      input.add(nonBlockingSequence(someSize, true));

      TestingReporter reporter = new TestingReporter();
      final ParallelMergeCombiningSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombiningSequence<>(
          pool,
          input,
          INT_PAIR_ORDERING,
          INT_PAIR_MERGE_FN,
          true,
          30 * 1000,
          0,
          TEST_POOL_SIZE,
          512,
          128,
          ParallelMergeCombiningSequence.DEFAULT_TASK_TARGET_RUN_TIME_MILLIS,
          reporter
      );
      Yielder<IntPair> parallelMergeCombineYielder = Yielders.each(parallelMergeCombineSequence);
      reporter.future = parallelMergeCombineSequence.getCancellationFuture();
      reporter.yielder = parallelMergeCombineYielder;
      parallelMergeCombineYielder.next(null);
      Assert.assertFalse(parallelMergeCombineYielder.isDone());
      reporters.add(reporter);
    }

    for (TestingReporter testingReporter : reporters) {
      Yielder<IntPair> parallelMergeCombineYielder = testingReporter.yielder;
      while (!parallelMergeCombineYielder.isDone()) {
        parallelMergeCombineYielder = parallelMergeCombineYielder.next(parallelMergeCombineYielder.get());
      }
      Assert.assertTrue(parallelMergeCombineYielder.isDone());
      parallelMergeCombineYielder.close();
      Assert.assertTrue(testingReporter.future.isDone());
    }

    Assert.assertTrue(pool.awaitQuiescence(10, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
    Assert.assertEquals(0, pool.getRunningThreadCount());
    Assert.assertFalse(pool.hasQueuedSubmissions());
    Assert.assertEquals(0, pool.getActiveThreadCount());
    for (TestingReporter reporter : reporters) {
      Assert.assertTrue(reporter.done);
    }
  }

  @Test
  public void testGracefulCloseOfYielderCancelsPool() throws IOException
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

  private void assertResult(List<Sequence<IntPair>> sequences) throws IOException
  {
    assertResult(
        sequences,
        ParallelMergeCombiningSequence.DEFAULT_TASK_SMALL_BATCH_NUM_ROWS,
        ParallelMergeCombiningSequence.DEFAULT_TASK_INITIAL_YIELD_NUM_ROWS,
        null
    );
  }

  private void assertResult(List<Sequence<IntPair>> sequences, int batchSize, int yieldAfter)
      throws IOException
  {
    assertResult(
        sequences,
        batchSize,
        yieldAfter,
        null
    );
  }

  private void assertResultWithCustomPool(
          List<Sequence<IntPair>> sequences,
          int batchSize,
          int yieldAfter,
          Consumer<ParallelMergeCombiningSequence.MergeCombineMetrics> reporter,
          ForkJoinPool customPool
  )
          throws InterruptedException, IOException
  {
    final CombiningSequence<IntPair> combiningSequence = CombiningSequence.create(
            new MergeSequence<>(INT_PAIR_ORDERING, Sequences.simple(sequences)),
            INT_PAIR_ORDERING,
            INT_PAIR_MERGE_FN
    );

    final ParallelMergeCombiningSequence<IntPair> parallelMergeCombineSequence = new ParallelMergeCombiningSequence<>(
            customPool,
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
    Assert.assertFalse(parallelMergeCombineSequence.getCancellationFuture().isCancelled());
    Assert.assertTrue(parallelMergeCombineSequence.getCancellationFuture().isDone());
    Assert.assertFalse(parallelMergeCombineSequence.getCancellationFuture().getCancellationGizmo().isCanceled());
  }

  private void assertResult(
      List<Sequence<IntPair>> sequences,
      int batchSize,
      int yieldAfter,
      Consumer<ParallelMergeCombiningSequence.MergeCombineMetrics> reporter
  )
      throws IOException
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
    Assert.assertTrue(pool.awaitQuiescence(5, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());
    combiningYielder.close();
    parallelMergeCombineYielder.close();
    // cancellation trigger should not be set if sequence was fully yielded and close is called
    // (though shouldn't actually matter even if it was...)
    Assert.assertFalse(parallelMergeCombineSequence.getCancellationFuture().isCancelled());
    Assert.assertFalse(parallelMergeCombineSequence.getCancellationFuture().getCancellationGizmo().isCanceled());
    Assert.assertTrue(parallelMergeCombineSequence.getCancellationFuture().isDone());
  }

  private void assertResultWithEarlyClose(
      List<Sequence<IntPair>> sequences,
      int batchSize,
      int yieldAfter,
      int closeYielderAfter,
      Consumer<ParallelMergeCombiningSequence.MergeCombineMetrics> reporter
  )
      throws IOException
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
    final String expectedExceptionMsg = "Sequence canceled";
    Assert.assertEquals(combiningYielder.get(), parallelMergeCombineYielder.get());
    final Yielder<IntPair> finalYielder = parallelMergeCombineYielder;
    Throwable t = Assert.assertThrows(RuntimeException.class, () -> finalYielder.next(finalYielder.get()));
    Assert.assertEquals(expectedExceptionMsg, t.getMessage());

    // cancellation gizmo of sequence should be cancelled, and also should contain our expected message
    Assert.assertTrue(parallelMergeCombineSequence.getCancellationFuture().getCancellationGizmo().isCanceled());
    Assert.assertEquals(
        expectedExceptionMsg,
        parallelMergeCombineSequence.getCancellationFuture().getCancellationGizmo().getRuntimeException().getMessage()
    );
    Assert.assertTrue(parallelMergeCombineSequence.getCancellationFuture().isCancelled());

    Assert.assertTrue(pool.awaitQuiescence(10, TimeUnit.SECONDS));
    Assert.assertTrue(pool.isQuiescent());

    Assert.assertFalse(combiningYielder.isDone());
    Assert.assertFalse(parallelMergeCombineYielder.isDone());
  }

  private void assertException(List<Sequence<IntPair>> sequences) throws Throwable
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
      throws Throwable
  {
    Throwable t = Assert.assertThrows(Exception.class, () -> {
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
    });

    sequences.forEach(sequence -> {
      if (sequence instanceof ExplodingSequence) {
        ExplodingSequence exploder = (ExplodingSequence) sequence;
        Assert.assertEquals(1, exploder.getCloseCount());
      }
    });
    LOG.warn(t, "exception:");
    throw t;
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
        new BaseSequence.IteratorMaker<>()
        {
          @Override
          public Iterator<IntPair> make()
          {
            return new Iterator<>()
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
        new BaseSequence.IteratorMaker<>()
        {
          @Override
          public Iterator<IntPair> make()
          {
            return new Iterator<>()
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
              return new Iterator<>()
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

  static class TestingReporter implements Consumer<ParallelMergeCombiningSequence.MergeCombineMetrics>
  {
    ParallelMergeCombiningSequence.CancellationFuture future;
    Yielder<IntPair> yielder;
    volatile ParallelMergeCombiningSequence.MergeCombineMetrics metrics;
    volatile boolean done = false;

    @Override
    public void accept(ParallelMergeCombiningSequence.MergeCombineMetrics mergeCombineMetrics)
    {
      metrics = mergeCombineMetrics;
      done = true;
    }
  }
}
