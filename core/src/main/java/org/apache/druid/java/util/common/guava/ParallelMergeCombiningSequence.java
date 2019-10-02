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

import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import java.util.function.BinaryOperator;

/**
 * Artisanal, locally-sourced, hand-crafted, gluten and GMO free, bespoke, small-batch parallel merge combinining sequence
 */
public class ParallelMergeCombiningSequence<T> extends YieldingSequenceBase<T>
{
  private static final Logger LOG = new Logger(ParallelMergeCombiningSequence.class);

  private final ForkJoinPool workerPool;
  private final List<Sequence<T>> baseSequences;
  private final Ordering<T> orderingFn;
  private final BinaryOperator<T> combineFn;
  private final int queueSize;
  private final boolean hasTimeout;
  private final long timeoutAt;
  private final int queryPriority; // not currently used :(
  private final int yieldAfter;
  private final int batchSize;
  private final int parallelism;
  private final CancellationGizmo cancellationGizmo;

  public ParallelMergeCombiningSequence(
      ForkJoinPool workerPool,
      List<Sequence<T>> baseSequences,
      Ordering<T> orderingFn,
      BinaryOperator<T> combineFn,
      boolean hasTimeout,
      long timeoutMillis,
      int queryPriority,
      int parallelism,
      int yieldAfter,
      int batchSize
  )
  {
    this.workerPool = workerPool;
    this.baseSequences = baseSequences;
    this.orderingFn = orderingFn;
    this.combineFn = combineFn;
    this.hasTimeout = hasTimeout;
    this.timeoutAt = System.currentTimeMillis() + timeoutMillis;
    this.queryPriority = queryPriority;
    this.parallelism = parallelism;
    this.yieldAfter = yieldAfter;
    this.batchSize = batchSize;
    this.queueSize = 4 * (yieldAfter / batchSize);
    this.cancellationGizmo = new CancellationGizmo();
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    if (baseSequences.isEmpty()) {
      return Sequences.<T>empty().toYielder(initValue, accumulator);
    }

    final BlockingQueue<ResultBatch<T>> outputQueue = new ArrayBlockingQueue<>(queueSize);
    MergeCombinePartitioningAction<T> finalMergeAction = new MergeCombinePartitioningAction<>(
        baseSequences,
        orderingFn,
        combineFn,
        outputQueue,
        queueSize,
        parallelism,
        yieldAfter,
        batchSize,
        hasTimeout,
        timeoutAt,
        cancellationGizmo
    );
    workerPool.execute(finalMergeAction);
    Sequence<T> finalOutSequence = makeOutputSequenceForQueue(outputQueue, hasTimeout, timeoutAt, cancellationGizmo);
    return finalOutSequence.toYielder(initValue, accumulator);
  }

  /**
   * Create an output {@link Sequence} that wraps the output {@link BlockingQueue} of a
   * {@link MergeCombinePartitioningAction}
   */
  static <T> Sequence<T> makeOutputSequenceForQueue(
      BlockingQueue<ResultBatch<T>> queue,
      boolean hasTimeout,
      long timeoutAt,
      CancellationGizmo cancellationGizmo
  )
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return new Iterator<T>()
            {
              private ResultBatch<T> currentBatch;

              @Override
              public boolean hasNext()
              {
                final int thisTimeout = Ints.checkedCast(timeoutAt - System.currentTimeMillis());
                if (thisTimeout < 0) {
                  throw new RE(new TimeoutException("Sequence iterator timed out"));
                }

                if (currentBatch != null && !currentBatch.isTerminalResult() && !currentBatch.isDrained()) {
                  return true;
                }
                try {
                  if (currentBatch == null || currentBatch.isDrained()) {
                    if (hasTimeout) {
                      currentBatch = queue.poll(thisTimeout, TimeUnit.MILLISECONDS);
                    } else {
                      currentBatch = queue.take();
                    }
                  }
                  if (currentBatch == null) {
                    throw new RE(new TimeoutException("Sequence iterator timed out waiting for data"));
                  }

                  if (cancellationGizmo.isCancelled()) {
                    if (cancellationGizmo.getRuntimeException() != null) {
                      throw cancellationGizmo.getRuntimeException();
                    } else {
                      throw new RuntimeException("Failed to merge results, unknown error");
                    }
                  }

                  if (currentBatch.isTerminalResult()) {
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
                  if (cancellationGizmo.getRuntimeException() != null) {
                    throw cancellationGizmo.getRuntimeException();
                  } else {
                    throw new RuntimeException("Failed to merge results, unknown error");
                  }
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
            // nothing to cleanup
          }
        }
    );
  }


  /**
   * This {@link RecursiveAction} is the initial task of the parallel merge-combine process. Capacity and input sequence
   * count permitting, it will partition the input set of {@link Sequence} to do 2 layer parallel merge.
   *
   * For the first layer, the partitions of input sequences are each wrapped in {@link YielderBatchedResultsCursor}, and
   * for each partition a {@link PrepareMergeCombineInputsAction} will be executed to to wait for each of the yielders to
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
    private final boolean hasTimeout;
    private final long timeoutAt;
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
        boolean hasTimeout,
        long timeoutAt,
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
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
      this.cancellationGizmo = cancellationGizmo;
    }

    @Override
    protected void compute()
    {
      try {
        final int parallelTaskCount = computeNumTasks();

        // if we have a small number of sequences to merge, or computed paralellism is too low, do not run in parallel,
        // just serially perform the merge-combine with a single task
        if (sequences.size() < 4 || parallelTaskCount < 2) {
          LOG.info(
              "Input sequence count (%s) or available parallel merge task count (%s) too small to perform parallel"
              + " merge-combine, performing serially with a single merge-combine task",
              sequences.size(),
              parallelTaskCount
          );

          QueuePusher<ResultBatch<T>> resultsPusher = new QueuePusher<>(out, hasTimeout, timeoutAt);

          List<BatchedResultsCursor<T>> sequenceCursors = new ArrayList<>(sequences.size());
          for (Sequence<T> s : sequences) {
            sequenceCursors.add(new YielderBatchedResultsCursor<>(new SequenceBatcher<>(s, batchSize), orderingFn));
          }
          PrepareMergeCombineInputsAction<T> blockForInputsAction = new PrepareMergeCombineInputsAction<>(
              sequenceCursors,
              resultsPusher,
              orderingFn,
              combineFn,
              yieldAfter,
              batchSize,
              cancellationGizmo
          );
          getPool().execute(blockForInputsAction);
        } else {
          // 2 layer parallel merge done in fjp
          LOG.info("Spawning %s parallel merge-combine tasks for %s sequences", parallelTaskCount, sequences.size());
          spawnParallelTasks(parallelTaskCount);
        }
      }
      catch (Exception ex) {
        cancellationGizmo.cancel(ex);
        out.offer(new ResultBatch<>());
      }
    }

    void spawnParallelTasks(int parallelMergeTasks)
    {
      List<RecursiveAction> tasks = new ArrayList<>();
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
        PrepareMergeCombineInputsAction<T> blockForInputsAction = new PrepareMergeCombineInputsAction<>(
            partitionCursors,
            pusher,
            orderingFn,
            combineFn,
            yieldAfter,
            batchSize,
            cancellationGizmo
        );
        tasks.add(blockForInputsAction);
      }

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
      PrepareMergeCombineInputsAction<T> finalMergeAction = new PrepareMergeCombineInputsAction<>(
          intermediaryOutputsCursors,
          outputPusher,
          orderingFn,
          combineFn,
          yieldAfter,
          batchSize,
          cancellationGizmo
      );

      getPool().execute(finalMergeAction);
    }

    /**
     * Computes maximum number of layer 1 parallel merging tasks given available processors and an estimate of current
     * {@link ForkJoinPool} utilization. A return value of 1 or less indicates that a serial merge will be done on
     * the pool instead.
     */
    int computeNumTasks()
    {
      // max is minimum of either number of processors or user suggested parallelism
      final int maxParallelism = Math.min(JvmUtils.getRuntimeInfo().getAvailableProcessors(), parallelism);
      // adjust max to be no more than total pool parallelism less the number of running threads + submitted tasks
      final int utilizationEstimate = getPool().getRunningThreadCount() + getPool().getQueuedSubmissionCount();
      // minimum of 'max computed parallelism' and pool parallelism less current 'utilization estimate'
      final int computedParallelism = Math.min(maxParallelism, getPool().getParallelism() - utilizationEstimate);
      // compute total number of layer 1 'parallel' tasks, the final merge task will take the remaining slot
      final int computedOptimalParallelism = Math.min(
          (int) Math.floor((double) sequences.size() / 2.0),
          computedParallelism - 1
      );
      return computedOptimalParallelism;
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
   * This task takes a 'yieldAfter' parameter which controls how many input result rows will be processed before this
   * task completes and executes a new task to continue where it left off. This value is initially set by the
   * {@link MergeCombinePartitioningAction} to a default value, but after that this process is timed to try and compute
   * an 'optimal' number of rows to yield to achieve a task runtime of ~10ms, on the assumption that the time to process
   * n results will be approximately the same.
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
    private final int depth;
    private final CancellationGizmo cancellationGizmo;

    private MergeCombineAction(
        PriorityQueue<BatchedResultsCursor<T>> pQueue,
        QueuePusher<ResultBatch<T>> outputQueue,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        T initialValue,
        int yieldAfter,
        int batchSize,
        int depth,
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
      this.depth = depth;
      this.cancellationGizmo = cancellationGizmo;
    }

    @Override
    protected void compute()
    {
      try {
        long start = System.nanoTime();

        int counter = 0;
        int batchCounter = 0;
        ResultBatch<T> outputBatch = new ResultBatch<>(batchSize);

        T currentCombinedValue = initialValue;
        while (counter++ < yieldAfter && !pQueue.isEmpty()) {
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
              batchCounter = 0;
            }

            // next value is now current value
            currentCombinedValue = combineFn.apply(null, nextValueToAccumulate);
          } else {
            cursor.close();
          }
        }

        if (!pQueue.isEmpty() && !cancellationGizmo.isCancelled()) {
          // if there is still work to be done, execute a new task with the current accumulated value to continue
          // combining where we left off
          if (!outputBatch.isDrained()) {
            outputQueue.offer(outputBatch);
          }

          // measure the time it took to process 'yieldAfter' elements in order to project a next 'yieldAfter' value
          // which we want to target a 10ms task run time. smooth this value with a cumulative moving average in order
          // to prevent normal jitter in processing time from skewing the next yield value too far in any direction
          final long elapsedMillis = Math.max(
              TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS),
              1L
          );
          final double nextYieldAfter = Math.max(10.0 * ((double) yieldAfter / elapsedMillis), 1.0);
          final double cumulativeMovingAverage = (nextYieldAfter + (depth * yieldAfter)) / (depth + 1);
          final int adjustedNextYieldAfter = (int) Math.ceil(cumulativeMovingAverage);

          getPool().execute(new MergeCombineAction<>(
              pQueue,
              outputQueue,
              orderingFn,
              combineFn,
              currentCombinedValue,
              adjustedNextYieldAfter,
              batchSize,
              depth + 1,
              cancellationGizmo
          ));
        } else if (cancellationGizmo.isCancelled()) {
          // if we got the cancellation signal, go ahead and write terminal value into output queue to help gracefully
          // allow downstream stuff to stop
          outputQueue.offer(new ResultBatch<>());
        } else {
          // if priority queue is empty, push the final accumulated value into the output batch and push it out
          outputBatch.add(currentCombinedValue);
          outputQueue.offer(outputBatch);
          // ... and the terminal value to indicate the blocking queue holding the values is complete
          outputQueue.offer(new ResultBatch<>());
        }
      }
      catch (Exception ex) {
        cancellationGizmo.cancel(ex);
        outputQueue.offer(new ResultBatch<>());
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
    private final CancellationGizmo cancellationGizmo;

    private PrepareMergeCombineInputsAction(
        List<BatchedResultsCursor<T>> partition,
        QueuePusher<ResultBatch<T>> outputQueue,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        int yieldAfter,
        int batchSize,
        CancellationGizmo cancellationGizmo
    )
    {
      this.partition = partition;
      this.orderingFn = orderingFn;
      this.combineFn = combineFn;
      this.outputQueue = outputQueue;
      this.yieldAfter = yieldAfter;
      this.batchSize = batchSize;
      this.cancellationGizmo = cancellationGizmo;
    }

    @Override
    protected void compute()
    {
      try {
        PriorityQueue<BatchedResultsCursor<T>> cursors = new PriorityQueue<>(partition.size());
        for (BatchedResultsCursor<T> cursor : partition) {
          // this is blocking
          cursor.initialize();
          if (!cursor.isDone()) {
            cursors.offer(cursor);
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
              1,
              cancellationGizmo
          ));
        } else {
          outputQueue.offer(new ResultBatch<>());
        }
      }
      catch (Exception ex) {
        cancellationGizmo.cancel(ex);
        outputQueue.offer(new ResultBatch<>());
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
    final long timeoutAt;
    final BlockingQueue<E> queue;
    volatile E item = null;

    QueuePusher(BlockingQueue<E> q, boolean hasTimeout, long timeoutAt)
    {
      this.queue = q;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
    }

    @Override
    public boolean block() throws InterruptedException
    {
      boolean success = false;
      if (item != null) {
        if (hasTimeout) {
          final int thisTimeout = Ints.checkedCast(timeoutAt - System.currentTimeMillis());
          if (thisTimeout < 0) {
            throw new RE(new TimeoutException("QueuePusher timed out offering data"));
          }
          success = queue.offer(item, thisTimeout, TimeUnit.MILLISECONDS);
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

    void addItem(E item)
    {
      this.item = item;
    }

    public void offer(E item)
    {
      try {
        addItem(item);
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
    @Nullable
    private final Queue<E> values;
    private final boolean isTerminal;

    ResultBatch(int batchSize)
    {
      this.values = new ArrayDeque<>(batchSize);
      this.isTerminal = false;
    }

    ResultBatch()
    {
      this.values = null;
      this.isTerminal = true;
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
      return !isTerminal && values.isEmpty();
    }

    boolean isTerminalResult()
    {
      return isTerminal;
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
              count++;
              if (count % batchSize == 0) {
                yield();
              }
              accumulated.add(in);
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
      implements ForkJoinPool.ManagedBlocker, Comparable<BatchedResultsCursor<E>>
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


    public void close()
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
    final SequenceBatcher<E> sequenceYielder;
    Yielder<ResultBatch<E>> yielder;

    YielderBatchedResultsCursor(SequenceBatcher<E> sequenceYielder, Ordering<E> ordering)
    {
      super(ordering);
      this.sequenceYielder = sequenceYielder;
    }

    @Override
    public void initialize()
    {
      yielder = sequenceYielder.getBatchYielder();
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
    public void close()
    {
      try {
        yielder.close();
      }
      catch (IOException e) {
        throw new RuntimeException("Failed to close yielder", e);
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
    final long timeoutAt;

    BlockingQueueuBatchedResultsCursor(
        BlockingQueue<ResultBatch<E>> blockingQueue,
        Ordering<E> ordering,
        boolean hasTimeout,
        long timeoutAt
    )
    {
      super(ordering);
      this.queue = blockingQueue;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
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
          final int thisTimeout = Ints.checkedCast(timeoutAt - System.currentTimeMillis());
          if (thisTimeout < 0) {
            throw new RE(new TimeoutException("BlockingQueue cursor timed out waiting for data"));
          }
          resultBatch = queue.poll(thisTimeout, TimeUnit.MILLISECONDS);
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
    // volatile instead of AtomicBoolean because it is never unset
    private volatile boolean cancelled;
    private volatile Exception exception;

    void cancel(Exception ex)
    {
      if (cancelled) {
        return;
      }
      cancelled = true;
      exception = ex;
    }

    boolean isCancelled()
    {
      return cancelled;
    }

    RuntimeException getRuntimeException()
    {
      if (exception instanceof RuntimeException) {
        return (RuntimeException) exception;
      }
      return new RE(exception);
    }
  }
}
