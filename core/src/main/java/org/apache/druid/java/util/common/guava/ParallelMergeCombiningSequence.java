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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;

/**
 * Artisanal, locally-sourced, hand-crafted, gluten and GMO free, bespoke, free-range, organic, small-batch parallel
 * merge combining sequence.
 *
 * See proposal: https://github.com/apache/druid/issues/8577
 *
 * Functionally equivalent to wrapping {@link org.apache.druid.common.guava.CombiningSequence} around a
 * {@link MergeSequence}, but done in parallel on a {@link ForkJoinPool} running in 'async' mode.
 */
public class ParallelMergeCombiningSequence<T> extends YieldingSequenceBase<T>
{
  private static final Logger LOG = new Logger(ParallelMergeCombiningSequence.class);

  // these values were chosen carefully via feedback from benchmarks,
  // see PR https://github.com/apache/druid/pull/8578 for details
  public static final int DEFAULT_TASK_TARGET_RUN_TIME_MILLIS = 100;
  public static final int DEFAULT_TASK_INITIAL_YIELD_NUM_ROWS = 16384;
  public static final int DEFAULT_TASK_SMALL_BATCH_NUM_ROWS = 4096;

  private final ForkJoinPool workerPool;
  private final List<Sequence<T>> inputSequences;
  private final Ordering<T> orderingFn;
  private final BinaryOperator<T> combineFn;
  private final int queueSize;
  private final boolean hasTimeout;
  private final long timeoutAtNanos;
  private final int queryPriority; // not currently used :(
  private final int yieldAfter;
  private final int batchSize;
  private final int parallelism;
  private final long targetTimeNanos;
  private final Consumer<MergeCombineMetrics> metricsReporter;

  private final CancellationGizmo cancellationGizmo;

  public ParallelMergeCombiningSequence(
      ForkJoinPool workerPool,
      List<Sequence<T>> inputSequences,
      Ordering<T> orderingFn,
      BinaryOperator<T> combineFn,
      boolean hasTimeout,
      long timeoutMillis,
      int queryPriority,
      int parallelism,
      int yieldAfter,
      int batchSize,
      int targetTimeMillis,
      Consumer<MergeCombineMetrics> reporter
  )
  {
    this.workerPool = workerPool;
    this.inputSequences = inputSequences;
    this.orderingFn = orderingFn;
    this.combineFn = combineFn;
    this.hasTimeout = hasTimeout;
    this.timeoutAtNanos = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeoutMillis, TimeUnit.MILLISECONDS);
    this.queryPriority = queryPriority;
    this.parallelism = parallelism;
    this.yieldAfter = yieldAfter;
    this.batchSize = batchSize;
    this.targetTimeNanos = TimeUnit.NANOSECONDS.convert(targetTimeMillis, TimeUnit.MILLISECONDS);
    this.queueSize = 4 * (yieldAfter / batchSize);
    this.metricsReporter = reporter;
    this.cancellationGizmo = new CancellationGizmo();
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    if (inputSequences.isEmpty()) {
      return Sequences.<T>empty().toYielder(initValue, accumulator);
    }

    final BlockingQueue<ResultBatch<T>> outputQueue = new ArrayBlockingQueue<>(queueSize);
    final MergeCombineMetricsAccumulator metricsAccumulator = new MergeCombineMetricsAccumulator(inputSequences.size());
    MergeCombinePartitioningAction<T> mergeCombineAction = new MergeCombinePartitioningAction<>(
        inputSequences,
        orderingFn,
        combineFn,
        outputQueue,
        queueSize,
        parallelism,
        yieldAfter,
        batchSize,
        targetTimeNanos,
        hasTimeout,
        timeoutAtNanos,
        metricsAccumulator,
        cancellationGizmo
    );
    workerPool.execute(mergeCombineAction);
    Sequence<T> finalOutSequence = makeOutputSequenceForQueue(
        outputQueue,
        hasTimeout,
        timeoutAtNanos,
        cancellationGizmo
    ).withBaggage(() -> {
      if (metricsReporter != null) {
        metricsReporter.accept(metricsAccumulator.build());
      }
    });
    return finalOutSequence.toYielder(initValue, accumulator);
  }

  @VisibleForTesting
  public CancellationGizmo getCancellationGizmo()
  {
    return cancellationGizmo;
  }

  /**
   * Create an output {@link Sequence} that wraps the output {@link BlockingQueue} of a
   * {@link MergeCombinePartitioningAction}
   */
  static <T> Sequence<T> makeOutputSequenceForQueue(
      BlockingQueue<ResultBatch<T>> queue,
      boolean hasTimeout,
      long timeoutAtNanos,
      CancellationGizmo cancellationGizmo
  )
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          private boolean shouldCancelOnCleanup = true;
          @Override
          public Iterator<T> make()
          {
            return new Iterator<T>()
            {
              private ResultBatch<T> currentBatch;

              @Override
              public boolean hasNext()
              {
                final long thisTimeoutNanos = timeoutAtNanos - System.nanoTime();
                if (hasTimeout && thisTimeoutNanos < 0) {
                  throw new RE(new TimeoutException("Sequence iterator timed out"));
                }

                if (currentBatch != null && !currentBatch.isTerminalResult() && !currentBatch.isDrained()) {
                  return true;
                }
                try {
                  if (currentBatch == null || currentBatch.isDrained()) {
                    if (hasTimeout) {
                      currentBatch = queue.poll(thisTimeoutNanos, TimeUnit.NANOSECONDS);
                    } else {
                      currentBatch = queue.take();
                    }
                  }
                  if (currentBatch == null) {
                    throw new RE(new TimeoutException("Sequence iterator timed out waiting for data"));
                  }

                  if (cancellationGizmo.isCancelled()) {
                    throw cancellationGizmo.getRuntimeException();
                  }

                  if (currentBatch.isTerminalResult()) {
                    shouldCancelOnCleanup = false;
                    return false;
                  }
                  return true;
                }
                catch (InterruptedException e) {
                  throw new RE(e);
                }
              }

              @Override
              public T next()
              {
                if (cancellationGizmo.isCancelled()) {
                  throw cancellationGizmo.getRuntimeException();
                }

                if (currentBatch == null || currentBatch.isDrained() || currentBatch.isTerminalResult()) {
                  throw new NoSuchElementException();
                }
                return currentBatch.next();
              }
            };
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            if (shouldCancelOnCleanup) {
              cancellationGizmo.cancel(new RuntimeException("Already closed"));
            }
          }
        }
    );
  }

  /**
   * This {@link RecursiveAction} is the initial task of the parallel merge-combine process. Capacity and input sequence
   * count permitting, it will partition the input set of {@link Sequence} to do 2 layer parallel merge.
   *
   * For the first layer, the partitions of input sequences are each wrapped in {@link YielderBatchedResultsCursor}, and
   * for each partition a {@link PrepareMergeCombineInputsAction} will be executed to wait for each of the yielders to
   * yield {@link ResultBatch}. After the cursors all have an initial set of results, the
   * {@link PrepareMergeCombineInputsAction} will execute a {@link MergeCombineAction}
   * to perform the actual work of merging sequences and combining results. The merged and combined output of each
   * partition will itself be put into {@link ResultBatch} and pushed to a {@link BlockingQueue} with a
   * {@link ForkJoinPool} {@link QueuePusher}.
   *
   * The second layer will execute a single {@link PrepareMergeCombineInputsAction} to wait for the {@link ResultBatch}
   * from each partition to be available in their 'output' {@link BlockingQueue} which each is wrapped in
   * {@link BlockingQueueuBatchedResultsCursor}. Like the first layer, after the {@link PrepareMergeCombineInputsAction}
   * is complete and some {@link ResultBatch} are ready to merge from each partition, it will execute a
   * {@link MergeCombineAction} do a final merge combine of all the parallel computed results, again pushing
   * {@link ResultBatch} into a {@link BlockingQueue} with a {@link QueuePusher}.
   */
  private static class MergeCombinePartitioningAction<T> extends RecursiveAction
  {
    private final List<Sequence<T>> sequences;
    private final Ordering<T> orderingFn;
    private final BinaryOperator<T> combineFn;
    private final BlockingQueue<ResultBatch<T>> out;
    private final int queueSize;
    private final int parallelism;
    private final int yieldAfter;
    private final int batchSize;
    private final long targetTimeNanos;
    private final boolean hasTimeout;
    private final long timeoutAt;
    private final MergeCombineMetricsAccumulator metricsAccumulator;
    private final CancellationGizmo cancellationGizmo;

    private MergeCombinePartitioningAction(
        List<Sequence<T>> sequences,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        BlockingQueue<ResultBatch<T>> out,
        int queueSize,
        int parallelism,
        int yieldAfter,
        int batchSize,
        long targetTimeNanos,
        boolean hasTimeout,
        long timeoutAt,
        MergeCombineMetricsAccumulator metricsAccumulator,
        CancellationGizmo cancellationGizmo
    )
    {
      this.sequences = sequences;
      this.combineFn = combineFn;
      this.orderingFn = orderingFn;
      this.out = out;
      this.queueSize = queueSize;
      this.parallelism = parallelism;
      this.yieldAfter = yieldAfter;
      this.batchSize = batchSize;
      this.targetTimeNanos = targetTimeNanos;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
      this.metricsAccumulator = metricsAccumulator;
      this.cancellationGizmo = cancellationGizmo;
    }

    @Override
    protected void compute()
    {
      List<BatchedResultsCursor<T>> sequenceCursors = new ArrayList<>(sequences.size());
      try {
        final int parallelTaskCount = computeNumTasks();

        // if we have a small number of sequences to merge, or computed paralellism is too low, do not run in parallel,
        // just serially perform the merge-combine with a single task
        if (parallelTaskCount < 2) {
          LOG.debug(
              "Input sequence count (%s) or available parallel merge task count (%s) too small to perform parallel"
              + " merge-combine, performing serially with a single merge-combine task",
              sequences.size(),
              parallelTaskCount
          );

          QueuePusher<ResultBatch<T>> resultsPusher = new QueuePusher<>(out, hasTimeout, timeoutAt);

          for (Sequence<T> s : sequences) {
            sequenceCursors.add(new YielderBatchedResultsCursor<>(new SequenceBatcher<>(s, batchSize), orderingFn));
          }
          MergeCombineActionMetricsAccumulator soloAccumulator = new MergeCombineActionMetricsAccumulator();
          metricsAccumulator.setPartitions(Collections.emptyList());
          metricsAccumulator.setMergeMetrics(soloAccumulator);
          PrepareMergeCombineInputsAction<T> blockForInputsAction = new PrepareMergeCombineInputsAction<>(
              sequenceCursors,
              resultsPusher,
              orderingFn,
              combineFn,
              yieldAfter,
              batchSize,
              targetTimeNanos,
              soloAccumulator,
              cancellationGizmo
          );
          getPool().execute(blockForInputsAction);
        } else {
          // 2 layer parallel merge done in fjp
          LOG.debug("Spawning %s parallel merge-combine tasks for %s sequences", parallelTaskCount, sequences.size());
          spawnParallelTasks(parallelTaskCount);
        }
      }
      catch (Throwable t) {
        closeAllCursors(sequenceCursors);
        cancellationGizmo.cancel(t);
        out.offer(ResultBatch.TERMINAL);
      }
    }

    private void spawnParallelTasks(int parallelMergeTasks)
    {
      List<RecursiveAction> tasks = new ArrayList<>(parallelMergeTasks);
      List<MergeCombineActionMetricsAccumulator> taskMetrics = new ArrayList<>(parallelMergeTasks);

      List<BlockingQueue<ResultBatch<T>>> intermediaryOutputs = new ArrayList<>(parallelMergeTasks);

      List<? extends List<Sequence<T>>> partitions =
          Lists.partition(sequences, sequences.size() / parallelMergeTasks);

      for (List<Sequence<T>> partition : partitions) {
        BlockingQueue<ResultBatch<T>> outputQueue = new ArrayBlockingQueue<>(queueSize);
        intermediaryOutputs.add(outputQueue);
        QueuePusher<ResultBatch<T>> pusher = new QueuePusher<>(outputQueue, hasTimeout, timeoutAt);

        List<BatchedResultsCursor<T>> partitionCursors = new ArrayList<>(sequences.size());
        for (Sequence<T> s : partition) {
          partitionCursors.add(new YielderBatchedResultsCursor<>(new SequenceBatcher<>(s, batchSize), orderingFn));
        }
        MergeCombineActionMetricsAccumulator partitionAccumulator = new MergeCombineActionMetricsAccumulator();
        PrepareMergeCombineInputsAction<T> blockForInputsAction = new PrepareMergeCombineInputsAction<>(
            partitionCursors,
            pusher,
            orderingFn,
            combineFn,
            yieldAfter,
            batchSize,
            targetTimeNanos,
            partitionAccumulator,
            cancellationGizmo
        );
        tasks.add(blockForInputsAction);
        taskMetrics.add(partitionAccumulator);
      }

      metricsAccumulator.setPartitions(taskMetrics);

      for (RecursiveAction task : tasks) {
        getPool().execute(task);
      }

      QueuePusher<ResultBatch<T>> outputPusher = new QueuePusher<>(out, hasTimeout, timeoutAt);
      List<BatchedResultsCursor<T>> intermediaryOutputsCursors = new ArrayList<>(intermediaryOutputs.size());
      for (BlockingQueue<ResultBatch<T>> queue : intermediaryOutputs) {
        intermediaryOutputsCursors.add(
            new BlockingQueueuBatchedResultsCursor<>(queue, orderingFn, hasTimeout, timeoutAt)
        );
      }
      MergeCombineActionMetricsAccumulator finalMergeMetrics = new MergeCombineActionMetricsAccumulator();

      metricsAccumulator.setMergeMetrics(finalMergeMetrics);
      PrepareMergeCombineInputsAction<T> finalMergeAction = new PrepareMergeCombineInputsAction<>(
          intermediaryOutputsCursors,
          outputPusher,
          orderingFn,
          combineFn,
          yieldAfter,
          batchSize,
          targetTimeNanos,
          finalMergeMetrics,
          cancellationGizmo
      );

      getPool().execute(finalMergeAction);
    }

    /**
     * Computes maximum number of layer 1 parallel merging tasks given available processors and an estimate of current
     * {@link ForkJoinPool} utilization. A return value of 1 or less indicates that a serial merge will be done on
     * the pool instead.
     */
    private int computeNumTasks()
    {
      final int runningThreadCount = getPool().getRunningThreadCount();
      final int submissionCount = getPool().getQueuedSubmissionCount();

      // max is smaller of either:
      // - parallelism passed into sequence (number of physical cores by default)
      // - pool parallelism (number of physical cores * 1.5 by default)
      final int maxParallelism = Math.min(parallelism, getPool().getParallelism());

      // we consider 'utilization' to be the number of running threads + submitted tasks that have not yet started
      // running, minus 1 for the task that is running this calculation (as it will be replaced with the parallel tasks)
      final int utilizationEstimate = runningThreadCount + submissionCount - 1;

      // 'computed parallelism' is the remaineder of the 'max parallelism' less current 'utilization estimate'
      final int computedParallelismForUtilization = maxParallelism - utilizationEstimate;

      // try to balance partition size with partition count so we don't end up with layer 2 'final merge' task that has
      // significantly more work to do than the layer 1 'parallel' tasks.
      final int computedParallelismForSequences = (int) Math.floor(Math.sqrt(sequences.size()));

      // compute total number of layer 1 'parallel' tasks, for the utilzation parallelism, subtract 1 as the final merge
      // task will take the remaining slot
      final int computedOptimalParallelism = Math.min(
          computedParallelismForSequences,
          computedParallelismForUtilization - 1
      );

      final int computedNumParallelTasks = Math.max(computedOptimalParallelism, 1);

      LOG.debug("Computed parallel tasks: [%s]; ForkJoinPool details - sequence parallelism: [%s] "
                + "active threads: [%s] running threads: [%s] queued submissions: [%s] queued tasks: [%s] "
                + "pool parallelism: [%s] pool size: [%s] steal count: [%s]",
                computedNumParallelTasks,
                parallelism,
                getPool().getActiveThreadCount(),
                runningThreadCount,
                submissionCount,
                getPool().getQueuedTaskCount(),
                getPool().getParallelism(),
                getPool().getPoolSize(),
                getPool().getStealCount()
      );

      return computedNumParallelTasks;
    }
  }


  /**
   * This {@link RecursiveAction} is the work-horse of the {@link ParallelMergeCombiningSequence}, it merge-combines
   * a set of {@link BatchedResultsCursor} and produces output to a {@link BlockingQueue} with the help of a
   * {@link QueuePusher}. This is essentially a composite of logic taken from {@link MergeSequence} and
   * {@link org.apache.druid.common.guava.CombiningSequence}, where the {@link Ordering} is used to both set the sort
   * order for a {@link PriorityQueue}, and as a comparison to determine if 'same' ordered results need to be combined
   * with a supplied {@link BinaryOperator} combining function.
   *
   * This task takes a {@link #yieldAfter} parameter which controls how many input result rows will be processed before
   * this task completes and executes a new task to continue where it left off. This value is initially set by the
   * {@link MergeCombinePartitioningAction} to a default value, but after that this process is timed to try and compute
   * an 'optimal' number of rows to yield to achieve a task runtime of ~10ms, on the assumption that the time to process
   * n results will be approximately the same. {@link MergeCombineActionMetricsAccumulator#taskCount} is used to track
   * how many times a task has continued executing, and utilized to compute a cumulative moving average of task run time
   * per amount yielded in order to 'smooth' out the continual adjustment.
   */
  private static class MergeCombineAction<T> extends RecursiveAction
  {
    private final PriorityQueue<BatchedResultsCursor<T>> pQueue;
    private final Ordering<T> orderingFn;
    private final BinaryOperator<T> combineFn;
    private final QueuePusher<ResultBatch<T>> outputQueue;
    private final T initialValue;
    private final int yieldAfter;
    private final int batchSize;
    private final long targetTimeNanos;
    private final MergeCombineActionMetricsAccumulator metricsAccumulator;
    private final CancellationGizmo cancellationGizmo;

    private MergeCombineAction(
        PriorityQueue<BatchedResultsCursor<T>> pQueue,
        QueuePusher<ResultBatch<T>> outputQueue,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        T initialValue,
        int yieldAfter,
        int batchSize,
        long targetTimeNanos,
        MergeCombineActionMetricsAccumulator metricsAccumulator,
        CancellationGizmo cancellationGizmo
    )
    {
      this.pQueue = pQueue;
      this.orderingFn = orderingFn;
      this.combineFn = combineFn;
      this.outputQueue = outputQueue;
      this.initialValue = initialValue;
      this.yieldAfter = yieldAfter;
      this.batchSize = batchSize;
      this.targetTimeNanos = targetTimeNanos;
      this.metricsAccumulator = metricsAccumulator;
      this.cancellationGizmo = cancellationGizmo;
    }

    @Override
    protected void compute()
    {
      try {
        long start = System.nanoTime();
        long startCpuNanos = JvmUtils.safeGetThreadCpuTime();

        int counter = 0;
        int batchCounter = 0;
        ResultBatch<T> outputBatch = new ResultBatch<>(batchSize);

        T currentCombinedValue = initialValue;
        while (counter < yieldAfter && !pQueue.isEmpty()) {
          BatchedResultsCursor<T> cursor = pQueue.poll();

          // push the queue along
          if (!cursor.isDone()) {
            T nextValueToAccumulate = cursor.get();

            cursor.advance();
            if (!cursor.isDone()) {
              pQueue.offer(cursor);
            } else {
              cursor.close();
            }

            counter++;
            // if current value is null, combine null with next value
            if (currentCombinedValue == null) {
              currentCombinedValue = combineFn.apply(null, nextValueToAccumulate);
              continue;
            }

            // if current value is "same" as next value, combine them
            if (orderingFn.compare(currentCombinedValue, nextValueToAccumulate) == 0) {
              currentCombinedValue = combineFn.apply(currentCombinedValue, nextValueToAccumulate);
              continue;
            }

            // else, push accumulated value to the queue, accumulate again with next value as initial
            outputBatch.add(currentCombinedValue);
            batchCounter++;
            if (batchCounter >= batchSize) {
              outputQueue.offer(outputBatch);
              outputBatch = new ResultBatch<>(batchSize);
              metricsAccumulator.incrementOutputRows(batchCounter);
              batchCounter = 0;
            }

            // next value is now current value
            currentCombinedValue = combineFn.apply(null, nextValueToAccumulate);
          } else {
            cursor.close();
          }
        }

        final long elapsedCpuNanos = JvmUtils.safeGetThreadCpuTime() - startCpuNanos;
        metricsAccumulator.incrementInputRows(counter);
        metricsAccumulator.incrementCpuTimeNanos(elapsedCpuNanos);
        metricsAccumulator.incrementTaskCount();

        if (!pQueue.isEmpty() && !cancellationGizmo.isCancelled()) {
          // if there is still work to be done, execute a new task with the current accumulated value to continue
          // combining where we left off
          if (!outputBatch.isDrained()) {
            outputQueue.offer(outputBatch);
            metricsAccumulator.incrementOutputRows(batchCounter);
          }

          // measure the time it took to process 'yieldAfter' elements in order to project a next 'yieldAfter' value
          // which we want to target a 10ms task run time. smooth this value with a cumulative moving average in order
          // to prevent normal jitter in processing time from skewing the next yield value too far in any direction
          final long elapsedNanos = System.nanoTime() - start;
          final double nextYieldAfter = Math.max((double) targetTimeNanos * ((double) yieldAfter / elapsedCpuNanos), 1.0);
          final long recursionDepth = metricsAccumulator.getTaskCount();
          final double cumulativeMovingAverage =
              (nextYieldAfter + (recursionDepth * yieldAfter)) / (recursionDepth + 1);
          final int adjustedNextYieldAfter = (int) Math.ceil(cumulativeMovingAverage);

          LOG.debug(
              "task recursion %s yielded %s results ran for %s millis (%s nanos), %s cpu nanos, next task yielding every %s operations",
              recursionDepth,
              yieldAfter,
              TimeUnit.MILLISECONDS.convert(elapsedNanos, TimeUnit.NANOSECONDS),
              elapsedNanos,
              elapsedCpuNanos,
              adjustedNextYieldAfter
          );
          getPool().execute(new MergeCombineAction<>(
              pQueue,
              outputQueue,
              orderingFn,
              combineFn,
              currentCombinedValue,
              adjustedNextYieldAfter,
              batchSize,
              targetTimeNanos,
              metricsAccumulator,
              cancellationGizmo
          ));
        } else if (cancellationGizmo.isCancelled()) {
          // if we got the cancellation signal, go ahead and write terminal value into output queue to help gracefully
          // allow downstream stuff to stop
          LOG.debug("cancelled after %s tasks", metricsAccumulator.getTaskCount());
          // make sure to close underlying cursors
          closeAllCursors(pQueue);
          outputQueue.offer(ResultBatch.TERMINAL);
        } else {
          // if priority queue is empty, push the final accumulated value into the output batch and push it out
          outputBatch.add(currentCombinedValue);
          metricsAccumulator.incrementOutputRows(batchCounter + 1L);
          outputQueue.offer(outputBatch);
          // ... and the terminal value to indicate the blocking queue holding the values is complete
          outputQueue.offer(ResultBatch.TERMINAL);
          LOG.debug("merge combine complete after %s tasks", metricsAccumulator.getTaskCount());
        }
      }
      catch (Throwable t) {
        closeAllCursors(pQueue);
        cancellationGizmo.cancel(t);
        outputQueue.offer(ResultBatch.TERMINAL);
      }
    }
  }


  /**
   * This {@link RecursiveAction}, given a set of uninitialized {@link BatchedResultsCursor}, will initialize each of
   * them (which is a potentially managed blocking operation) so that each will produce a {@link ResultBatch}
   * from the {@link Yielder} or {@link BlockingQueue} that backs the cursor.
   *
   * Once initialized with a {@link ResultBatch}, the cursors are inserted into a {@link PriorityQueue} and
   * fed into a {@link MergeCombineAction} which will do the actual work of merging and combining the result batches.
   * This happens as soon as all cursors are initialized, as long as there is at least 1 cursor that is not 'done'
   * ({@link BatchedResultsCursor#isDone()}).
   *
   * This task may take longer than other tasks on the {@link ForkJoinPool}, but is doing little actual work, the
   * majority of its time will be spent managed blocking until results are ready for each cursor, or will be incredibly
   * short lived if all inputs are already available.
   */
  private static class PrepareMergeCombineInputsAction<T> extends RecursiveAction
  {
    private final List<BatchedResultsCursor<T>> partition;
    private final Ordering<T> orderingFn;
    private final BinaryOperator<T> combineFn;
    private final QueuePusher<ResultBatch<T>> outputQueue;
    private final int yieldAfter;
    private final int batchSize;
    private final long targetTimeNanos;
    private final MergeCombineActionMetricsAccumulator metricsAccumulator;
    private final CancellationGizmo cancellationGizmo;

    private PrepareMergeCombineInputsAction(
        List<BatchedResultsCursor<T>> partition,
        QueuePusher<ResultBatch<T>> outputQueue,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        int yieldAfter,
        int batchSize,
        long targetTimeNanos,
        MergeCombineActionMetricsAccumulator metricsAccumulator,
        CancellationGizmo cancellationGizmo
    )
    {
      this.partition = partition;
      this.orderingFn = orderingFn;
      this.combineFn = combineFn;
      this.outputQueue = outputQueue;
      this.yieldAfter = yieldAfter;
      this.batchSize = batchSize;
      this.targetTimeNanos = targetTimeNanos;
      this.metricsAccumulator = metricsAccumulator;
      this.cancellationGizmo = cancellationGizmo;
    }

    @Override
    protected void compute()
    {
      PriorityQueue<BatchedResultsCursor<T>> cursors = new PriorityQueue<>(partition.size());
      try {
        for (BatchedResultsCursor<T> cursor : partition) {
          // this is blocking
          cursor.initialize();
          if (!cursor.isDone()) {
            cursors.offer(cursor);
          } else {
            cursor.close();
          }
        }

        if (cursors.size() > 0) {
          getPool().execute(new MergeCombineAction<T>(
              cursors,
              outputQueue,
              orderingFn,
              combineFn,
              null,
              yieldAfter,
              batchSize,
              targetTimeNanos,
              metricsAccumulator,
              cancellationGizmo
          ));
        } else {
          outputQueue.offer(ResultBatch.TERMINAL);
        }
      }
      catch (Throwable t) {
        closeAllCursors(partition);
        cancellationGizmo.cancel(t);
        outputQueue.offer(ResultBatch.TERMINAL);
      }
    }
  }


  /**
   * {@link ForkJoinPool} friendly {@link BlockingQueue} feeder, adapted from 'QueueTaker' of Java documentation on
   * {@link ForkJoinPool.ManagedBlocker},
   * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ForkJoinPool.ManagedBlocker.html
   */
  static class QueuePusher<E> implements ForkJoinPool.ManagedBlocker
  {
    final boolean hasTimeout;
    final long timeoutAtNanos;
    final BlockingQueue<E> queue;
    volatile E item = null;

    QueuePusher(BlockingQueue<E> q, boolean hasTimeout, long timeoutAtNanos)
    {
      this.queue = q;
      this.hasTimeout = hasTimeout;
      this.timeoutAtNanos = timeoutAtNanos;
    }

    @Override
    public boolean block() throws InterruptedException
    {
      boolean success = false;
      if (item != null) {
        if (hasTimeout) {
          final long thisTimeoutNanos = timeoutAtNanos - System.nanoTime();
          if (thisTimeoutNanos < 0) {
            throw new RE(new TimeoutException("QueuePusher timed out offering data"));
          }
          success = queue.offer(item, thisTimeoutNanos, TimeUnit.NANOSECONDS);
        } else {
          success = queue.offer(item);
        }
        if (success) {
          item = null;
        }
      }
      return success;
    }

    @Override
    public boolean isReleasable()
    {
      return item == null;
    }

    public void offer(E item)
    {
      try {
        this.item = item;
        ForkJoinPool.managedBlock(this);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Failed to offer result to output queue", e);
      }
    }
  }


  /**
   * Holder object for an ordered batch of results from a sequence. Batching the results vastly reduces the amount of
   * blocking that is needed to move results between stages of {@link MergeCombineAction} done in parallel, allowing
   * the fork join tasks to focus on doing actual work instead of dealing with managed blocking.
   */
  static class ResultBatch<E>
  {
    static final ResultBatch TERMINAL = new ResultBatch();

    @Nullable
    private final Queue<E> values;

    ResultBatch(int batchSize)
    {
      this.values = new ArrayDeque<>(batchSize);
    }

    private ResultBatch()
    {
      this.values = null;
    }

    public void add(E in)
    {
      assert values != null;
      values.offer(in);
    }

    public E get()
    {
      assert values != null;
      return values.peek();
    }

    public E next()
    {
      assert values != null;
      return values.poll();
    }

    boolean isDrained()
    {
      return values != null && values.isEmpty();
    }

    boolean isTerminalResult()
    {
      return values == null;
    }

    /**
     * Convert sequence to yielder that accumulates results into ordered 'batches'
     */
    static <E> Yielder<ResultBatch<E>> fromSequence(Sequence<E> sequence, int batchSize)
    {
      return sequence.toYielder(
          new ResultBatch<>(batchSize),
          new YieldingAccumulator<ResultBatch<E>, E>()
          {
            int count = 0;

            @Override
            public ResultBatch<E> accumulate(ResultBatch<E> accumulated, E in)
            {
              accumulated.add(in);
              count++;
              if (count % batchSize == 0) {
                yield();
              }
              return accumulated;
            }
          }
      );
    }
  }


  /**
   * {@link ForkJoinPool} friendly conversion of {@link Sequence} to {@link Yielder< ResultBatch >}
   */
  static class SequenceBatcher<E> implements ForkJoinPool.ManagedBlocker
  {
    private final Sequence<E> sequence;
    private final int batchSize;
    private volatile Yielder<ResultBatch<E>> batchYielder;

    SequenceBatcher(Sequence<E> sequence, int batchSize)
    {
      this.sequence = sequence;
      this.batchSize = batchSize;
    }

    Yielder<ResultBatch<E>> getBatchYielder()
    {
      try {
        ForkJoinPool.managedBlock(this);
        return batchYielder;
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Failed to load initial batch of results", e);
      }
    }

    @Override
    public boolean block()
    {
      batchYielder = ResultBatch.fromSequence(sequence, batchSize);
      return true;
    }

    @Override
    public boolean isReleasable()
    {
      return batchYielder != null;
    }
  }


  /**
   * Provides a higher level cursor interface to provide individual results out {@link ResultBatch} provided by
   * a {@link Yielder} or {@link BlockingQueue}. This is the mechanism that powers {@link MergeCombineAction}, where
   * a set of {@link BatchedResultsCursor} are placed in a {@link PriorityQueue} to facilitate ordering to merge results
   * from these cursors, and combine results with the same ordering using the combining function.
   */
  abstract static class BatchedResultsCursor<E>
      implements ForkJoinPool.ManagedBlocker, Comparable<BatchedResultsCursor<E>>, Closeable
  {
    final Ordering<E> ordering;
    volatile ResultBatch<E> resultBatch;

    BatchedResultsCursor(Ordering<E> ordering)
    {
      this.ordering = ordering;
    }

    public abstract void initialize();

    public abstract void advance();

    public abstract boolean isDone();

    void nextBatch()
    {
      try {
        ForkJoinPool.managedBlock(this);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Failed to load next batch of results", e);
      }
    }

    @Override
    public void close() throws IOException
    {
      // nothing to close for blocking queue, but yielders will need to clean up or they will leak resources
    }

    public E get()
    {
      return resultBatch.get();
    }

    @Override
    public int compareTo(BatchedResultsCursor<E> o)
    {
      return ordering.compare(get(), o.get());
    }

    @Override
    public boolean equals(Object o)
    {
      if (!(o instanceof BatchedResultsCursor)) {
        return false;
      }
      return compareTo((BatchedResultsCursor) o) == 0;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(ordering);
    }
  }


  /**
   * {@link BatchedResultsCursor} that wraps a {@link Yielder} of {@link ResultBatch} to provide individual rows
   * of the result batch.
   */
  static class YielderBatchedResultsCursor<E> extends BatchedResultsCursor<E>
  {
    final SequenceBatcher<E> batcher;
    Yielder<ResultBatch<E>> yielder;

    YielderBatchedResultsCursor(SequenceBatcher<E> batcher, Ordering<E> ordering)
    {
      super(ordering);
      this.batcher = batcher;
    }

    @Override
    public void initialize()
    {
      yielder = batcher.getBatchYielder();
      resultBatch = yielder.get();
    }

    @Override
    public void advance()
    {
      if (!resultBatch.isDrained()) {
        resultBatch.next();
      }
      if (resultBatch.isDrained() && !yielder.isDone()) {
        nextBatch();
      }
    }

    @Override
    public boolean isDone()
    {
      // yielder will never produce a 'terminal' result batch, so only check that we drain the final batch when the
      // yielder is done
      return resultBatch == null || (yielder.isDone() && resultBatch.isDrained());
    }

    @Override
    public boolean block()
    {
      if (yielder.isDone()) {
        return true;
      }
      if (resultBatch == null || resultBatch.isDrained()) {
        resultBatch = new ResultBatch<>(batcher.batchSize);
        final Yielder<ResultBatch<E>> nextYielder = yielder.next(resultBatch);
        yielder = nextYielder;
      }
      return true;
    }

    @Override
    public boolean isReleasable()
    {
      return resultBatch != null && !resultBatch.isDrained();
    }

    @Override
    public void close() throws IOException
    {
      if (yielder != null) {
        yielder.close();
      }
    }
  }


  /**
   * {@link BatchedResultsCursor} that wraps a {@link BlockingQueue} of {@link ResultBatch} to provide individual
   * rows from the result batch.
   */
  static class BlockingQueueuBatchedResultsCursor<E> extends BatchedResultsCursor<E>
  {
    final BlockingQueue<ResultBatch<E>> queue;
    final boolean hasTimeout;
    final long timeoutAtNanos;

    BlockingQueueuBatchedResultsCursor(
        BlockingQueue<ResultBatch<E>> blockingQueue,
        Ordering<E> ordering,
        boolean hasTimeout,
        long timeoutAtNanos
    )
    {
      super(ordering);
      this.queue = blockingQueue;
      this.hasTimeout = hasTimeout;
      this.timeoutAtNanos = timeoutAtNanos;
    }

    @Override
    public void initialize()
    {
      if (queue.isEmpty()) {
        nextBatch();
      } else {
        resultBatch = queue.poll();
      }
    }

    @Override
    public void advance()
    {
      if (!resultBatch.isDrained()) {
        resultBatch.next();
      }
      if (resultBatch.isDrained()) {
        nextBatch();
      }
    }

    @Override
    public boolean isDone()
    {
      // blocking queue cursors always will finish the queue with a 'terminal' result batch to indicate that the queue
      // is finished and no additional values are expected.
      return resultBatch.isTerminalResult();
    }

    @Override
    public boolean block() throws InterruptedException
    {
      if (resultBatch == null || resultBatch.isDrained()) {
        if (hasTimeout) {
          final long thisTimeoutNanos = timeoutAtNanos - System.nanoTime();
          if (thisTimeoutNanos < 0) {
            throw new RE(new TimeoutException("BlockingQueue cursor timed out waiting for data"));
          }
          resultBatch = queue.poll(thisTimeoutNanos, TimeUnit.NANOSECONDS);
        } else {
          resultBatch = queue.take();
        }
      }
      return resultBatch != null;
    }

    @Override
    public boolean isReleasable()
    {
      // if result batch is 'terminal' or still has values, no need to block
      if (resultBatch != null && (resultBatch.isTerminalResult() || !resultBatch.isDrained())) {
        return true;
      }
      // if we can get a result immediately without blocking, also no need to block
      resultBatch = queue.poll();
      return resultBatch != null;
    }
  }


  /**
   * Token to allow any {@link RecursiveAction} signal the others and the output sequence that something bad happened
   * and processing should cancel, such as a timeout or connection loss.
   */
  static class CancellationGizmo
  {
    private final AtomicReference<Throwable> throwable = new AtomicReference<>(null);

    void cancel(Throwable t)
    {
      throwable.compareAndSet(null, t);
    }

    boolean isCancelled()
    {
      return throwable.get() != null;
    }

    RuntimeException getRuntimeException()
    {
      Throwable ex = throwable.get();
      if (ex instanceof RuntimeException) {
        return (RuntimeException) ex;
      }
      return new RE(ex);
    }
  }

  /**
   * Metrics for the execution of a {@link ParallelMergeCombiningSequence} on the {@link ForkJoinPool}
   */
  public static class MergeCombineMetrics
  {
    private final int parallelism;
    private final int inputSequences;
    private final long inputRows;
    private final long outputRows;
    private final long taskCount;
    private final long totalCpuTime;

    MergeCombineMetrics(
        int parallelism,
        int inputSequences,
        long inputRows,
        long outputRows,
        long taskCount,
        long totalCpuTime
    )
    {
      this.parallelism = parallelism;
      this.inputSequences = inputSequences;
      this.inputRows = inputRows;
      this.outputRows = outputRows;
      this.taskCount = taskCount;
      this.totalCpuTime = totalCpuTime;
    }

    /**
     * Total number of layer 1 parallel tasks (+ 1 for total number of concurrent tasks for this query)
     */
    public int getParallelism()
    {
      return parallelism;
    }

    /**
     * Total number of input {@link Sequence} processed by {@link ParallelMergeCombiningSequence}
     */
    public long getInputSequences()
    {
      return inputSequences;
    }

    /**
     * Total number of input 'rows' processed by the {@link ParallelMergeCombiningSequence}
     */
    public long getInputRows()
    {
      return inputRows;
    }

    /**
     * Total number of output 'rows' produced by merging and combining the set of input {@link Sequence}s
     */
    public long getOutputRows()
    {
      return outputRows;
    }

    /**
     * Total number of {@link ForkJoinPool} tasks involved in executing the {@link ParallelMergeCombiningSequence},
     * including {@link MergeCombinePartitioningAction}, {@link PrepareMergeCombineInputsAction}, and
     * {@link MergeCombineAction}.
     */
    public long getTaskCount()
    {
      return taskCount;
    }

    /**
     * Total CPU time in nanoseconds during the 'hot loop' of doing actual merging and combining
     * in {@link MergeCombineAction}
     */
    public long getTotalCpuTime()
    {
      return totalCpuTime;
    }
  }

  /**
   * Holder to accumlate metrics for all work done {@link ParallelMergeCombiningSequence}, containing layer 1 task
   * metrics in {@link #partitionMetrics} and final merge task metrics in {@link #mergeMetrics}, in order to compute
   * {@link MergeCombineMetrics} after the {@link ParallelMergeCombiningSequence} is completely consumed.
   */
  static class MergeCombineMetricsAccumulator
  {
    List<MergeCombineActionMetricsAccumulator> partitionMetrics;
    MergeCombineActionMetricsAccumulator mergeMetrics;
    private final int inputSequences;

    MergeCombineMetricsAccumulator(int inputSequences)
    {
      this.inputSequences = inputSequences;
    }

    void setMergeMetrics(MergeCombineActionMetricsAccumulator mergeMetrics)
    {
      this.mergeMetrics = mergeMetrics;
    }

    void setPartitions(List<MergeCombineActionMetricsAccumulator> partitionMetrics)
    {
      this.partitionMetrics = partitionMetrics;
    }

    MergeCombineMetrics build()
    {
      long numInputRows = 0;
      long cpuTimeNanos = 0;
      // 1 partition task, 1 layer two prepare merge inputs task, 1 layer one prepare merge inputs task for each
      // partition
      long totalPoolTasks = 1 + 1 + partitionMetrics.size();

      // accumulate input row count, cpu time, and total number of tasks from each partition
      for (MergeCombineActionMetricsAccumulator partition : partitionMetrics) {
        numInputRows += partition.getInputRows();
        cpuTimeNanos += partition.getTotalCpuTimeNanos();
        totalPoolTasks += partition.getTaskCount();
      }
      // if serial merge done, only mergeMetrics is populated, get input rows from there instead. otherwise, ignore the
      // value as it is only the number of intermediary input rows to the layer 2 task
      if (partitionMetrics.isEmpty()) {
        numInputRows = mergeMetrics.getInputRows();
      }
      // number of fjp tasks and cpu time is interesting though
      totalPoolTasks += mergeMetrics.getTaskCount();
      cpuTimeNanos += mergeMetrics.getTotalCpuTimeNanos();

      final long numOutputRows = mergeMetrics.getOutputRows();

      return new MergeCombineMetrics(
          Math.max(partitionMetrics.size(), 1),
          inputSequences,
          numInputRows,
          numOutputRows,
          totalPoolTasks,
          cpuTimeNanos
      );
    }
  }

  /**
   * Accumulate metrics about a single chain of{@link MergeCombineAction}
   */
  static class MergeCombineActionMetricsAccumulator
  {
    private long taskCount = 1;
    private long inputRows = 0;
    private long outputRows = 0;
    private long totalCpuTimeNanos = 0;

    void incrementTaskCount()
    {
      taskCount++;
    }

    void incrementInputRows(long numInputRows)
    {
      inputRows += numInputRows;
    }

    void incrementOutputRows(long numOutputRows)
    {
      outputRows += numOutputRows;
    }

    void incrementCpuTimeNanos(long nanos)
    {
      totalCpuTimeNanos += nanos;
    }

    long getTaskCount()
    {
      return taskCount;
    }

    long getInputRows()
    {
      return inputRows;
    }

    long getOutputRows()
    {
      return outputRows;
    }

    long getTotalCpuTimeNanos()
    {
      return totalCpuTimeNanos;
    }
  }

  private static <T> void closeAllCursors(final Collection<BatchedResultsCursor<T>> cursors)
  {
    Closer closer = Closer.create();
    closer.registerAll(cursors);
    CloseQuietly.close(closer);
  }
}
