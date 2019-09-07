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
import java.util.stream.Collectors;

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

  public ParallelMergeCombiningSequence(
      ForkJoinPool workerPool,
      List<Sequence<T>> baseSequences,
      Ordering<T> orderingFn,
      BinaryOperator<T> combineFn,
      int queueSize,
      boolean hasTimeout,
      long timeout,
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
    this.queueSize = queueSize;
    this.hasTimeout = hasTimeout;
    this.timeoutAt = System.currentTimeMillis() + timeout;
    this.queryPriority = queryPriority;
    this.parallelism = parallelism;
    this.yieldAfter = yieldAfter;
    this.batchSize = batchSize;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final BlockingQueue<OrderedResultBatch<T>> outputQueue = new ArrayBlockingQueue<>(queueSize);

    List<Yielder<OrderedResultBatch<T>>> yielders =
        baseSequences.stream()
                     .map(s -> OrderedResultBatch.fromSequence(s, batchSize))
                     .filter(y -> !(y.get() == null || (y.get().isDrained() &&  y.isDone())))
                     .collect(Collectors.toList());
    if (yielders.size() > 0) {
      // 2 layer parallel merge done in fjp
      ParallelMergeCombineAction<T> finalMergeAction = new ParallelMergeCombineAction<>(
          yielders,
          orderingFn,
          combineFn,
          outputQueue,
          queueSize,
          parallelism,
          yieldAfter,
          batchSize,
          hasTimeout,
          timeoutAt
      );
      workerPool.execute(finalMergeAction);
      Sequence<T> finalOutSequence = makeOutputSequenceForQueue(finalMergeAction, outputQueue, hasTimeout, timeoutAt);
      return finalOutSequence.toYielder(initValue, accumulator);
    }
    // empty result
    return Sequences.<T>empty().toYielder(initValue, accumulator);
  }

  /**
   * Create an output {@link Sequence} that wraps the output {@link BlockingQueue} of a {@link RecursiveAction} task
   */
  private static <T> Sequence<T> makeOutputSequenceForQueue(
      RecursiveAction task,
      BlockingQueue<OrderedResultBatch<T>> queue,
      boolean hasTimeout,
      long timeoutAt
  )
  {
    final Sequence<T> backgroundCombineSequence = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return new Iterator<T>()
            {
              private OrderedResultBatch<T> currentBatch;

              @Override
              public boolean hasNext()
              {
                if (currentBatch != null && !currentBatch.isTerminalResult() && !currentBatch.isDrained()) {
                  return true;
                }
                try {
                  if (currentBatch == null || currentBatch.isDrained()) {
                    if (hasTimeout) {
                      final int thisTimeout = Ints.checkedCast(timeoutAt - System.currentTimeMillis());
                      currentBatch = queue.poll(thisTimeout, TimeUnit.MILLISECONDS);
                    } else {
                      currentBatch = queue.take();
                    }
                  }

                  if (currentBatch == null) {
                    throw new RuntimeException(new TimeoutException());
                  }
                  if (currentBatch.isTerminalResult()) {
                    return false;
                  }
                  return true;
                }
                catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public T next()
              {
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
            try {
              // todo: hmm.. this probably does nothing since this is only the first task and it doesn't run until all
              // recursive tasks that are spawned complete, maybe we need a collection of 'active' tasks which the tasks
              // themselves update?
              if (!task.isDone()) {
                task.cancel(true);
              }
            }
            catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        }
    );

    return backgroundCombineSequence;
  }

  /**
   * This {@link RecursiveAction} is the initial task of the parallel merge-combine process, it will partition the
   * batched result yielders to do 2 layer parallel merge, spawning some number of {@link MergeCombineAction} directly
   * for the yielders for the first layer, and spawning a single {@link FinalMergeCombineSeedAction} to wait for results
   * to be available in the 'output' {@link BlockingQueue} of the first layer to do a final merge combine of all the
   * parallel computed results.
   */
  private static class ParallelMergeCombineAction<T> extends RecursiveAction
  {
    private final List<Yielder<OrderedResultBatch<T>>> yielders;
    private final Ordering<T> orderingFn;
    private final BinaryOperator<T> combineFn;
    private final BlockingQueue<OrderedResultBatch<T>> out;
    private final int queueSize;
    private final int parallelism;
    private final int yieldAfter;
    private final int batchSize;
    private final boolean hasTimeout;
    private final long timeoutAt;

    private ParallelMergeCombineAction(
        List<Yielder<OrderedResultBatch<T>>> yielders,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        BlockingQueue<OrderedResultBatch<T>> out,
        int queueSize,
        int parallelism,
        int yieldAfter,
        int batchSize,
        boolean hasTimeout,
        long timeoutAt
    )
    {
      this.yielders = yielders;
      this.combineFn = combineFn;
      this.orderingFn = orderingFn;
      this.out = out;
      this.queueSize = queueSize;
      this.parallelism = parallelism;
      this.yieldAfter = yieldAfter;
      this.batchSize = batchSize;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
    }

    @Override
    protected void compute()
    {
      final int parallelMergeTasks = computeNumTasks();

      // if we have a small number of sequences to merge, or computed paralellism is too low, do not run in parallel,
      // just serially perform the merge-combine with a single task
      if (yielders.size() < 4 || parallelMergeTasks < 2) {
        LOG.info(
            "Input sequence count: %s or available parallel merge task count: %s too small to perform parallel"
            + " merge-combine, performing serially with a single merge-combine task",
            yielders.size(),
            parallelMergeTasks
        );
        final PriorityQueue<BatchedResultsCursor<T>> mergeQueue = new PriorityQueue<>(yielders.size());

        for (Yielder<OrderedResultBatch<T>> s : yielders) {
          YielderBatchedResultsCursor<T> batchedSequenceYielder = new YielderBatchedResultsCursor<>(
              s,
              orderingFn
          );
          mergeQueue.offer(batchedSequenceYielder);
        }

        QueuePusher<OrderedResultBatch<T>> resultsPusher = new QueuePusher<>(out, hasTimeout, timeoutAt);
        MergeCombineAction<T> mergeAction = new MergeCombineAction<T>(
            mergeQueue,
            resultsPusher,
            orderingFn,
            combineFn,
            null,
            yieldAfter,
            batchSize
        );
        getPool().execute(mergeAction);
      } else {
        LOG.info("Spawning %s parallel merge-combine tasks", parallelMergeTasks);
        spawnParallelTasks(parallelMergeTasks);
      }
    }

    int computeNumTasks()
    {
      final int numProcessors = JvmUtils.getRuntimeInfo().getAvailableProcessors();
      final int poolParallelism = getPool().getParallelism();
      final int activeThreadCount = getPool().getActiveThreadCount();
      final int runningThreadCount = getPool().getRunningThreadCount();
      final int submissionCount = getPool().getQueuedSubmissionCount();
      final long queuedTaskCount = getPool().getQueuedTaskCount();
      LOG.info(
          "processors: [%s] pool parallelism: [%s] active thread count: [%s] running thread count: [%s] submission count: [%s] queued task count: [%s]",
          numProcessors,
          poolParallelism,
          activeThreadCount,
          runningThreadCount,
          submissionCount,
          queuedTaskCount
      );
      // max is minimum of either number of processors - 1 or user suggested parallelism
      final int maxParallelism = Math.min(numProcessors, parallelism);
      // adjust max to be no more than total pool parallelism less the number of active threads
      final int computedParallelism = Math.min(maxParallelism, poolParallelism - activeThreadCount);
      // compute total number of layer 1 'parallel' tasks, the final merge task will take the remaining slot, we need at least 2
      final int computedOptimalParallelism = Math.min((int) Math.floor((double) yielders.size() / 2.0), computedParallelism - 1);
      return computedOptimalParallelism;


      /* original manual control
      final int suggestedParallelism = Math.max(2, parallelism - 1);
      final int suggestedOptimalParallelism = Math.min((int) Math.floor((double) yielders.size() / 2.0), suggestedParallelism);
      return suggestedOptimalParallelism;
       */
    }

    void spawnParallelTasks(int parallelMergeTasks)
    {
      List<RecursiveAction> tasks = new ArrayList<>();
      List<BlockingQueue<OrderedResultBatch<T>>> intermediaryOutputs = new ArrayList<>(parallelMergeTasks);

      List<? extends List<Yielder<OrderedResultBatch<T>>>> partitions =
          Lists.partition(yielders, yielders.size() / parallelMergeTasks);


      for (List<Yielder<OrderedResultBatch<T>>> partition : partitions) {
        final PriorityQueue<BatchedResultsCursor<T>> mergeQueue = new PriorityQueue<>(yielders.size());
        for (Yielder<OrderedResultBatch<T>> yielder : partition) {
          mergeQueue.offer(new YielderBatchedResultsCursor<>(yielder, orderingFn));
        }

        BlockingQueue<OrderedResultBatch<T>> outputQueue = new ArrayBlockingQueue<>(queueSize);
        intermediaryOutputs.add(outputQueue);
        QueuePusher<OrderedResultBatch<T>> pusher = new QueuePusher<>(outputQueue, hasTimeout, timeoutAt);

        MergeCombineAction<T> mergeAction = new MergeCombineAction<T>(
            mergeQueue,
            pusher,
            orderingFn,
            combineFn,
            null,
            yieldAfter,
            batchSize
        );
        tasks.add(mergeAction);
      }

      for (RecursiveAction task : tasks) {
        getPool().execute(task);
      }

      QueuePusher<OrderedResultBatch<T>> outputPusher = new QueuePusher<>(out, hasTimeout, timeoutAt);
      FinalMergeCombineSeedAction<T> finalMergeAction = new FinalMergeCombineSeedAction<>(
          intermediaryOutputs,
          outputPusher,
          orderingFn,
          combineFn,
          yieldAfter,
          batchSize,
          hasTimeout,
          timeoutAt
      );

      getPool().execute(finalMergeAction);
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
   * {@link ParallelMergeCombineAction} to a default value, but after that this process is timed to try and compute
   * an 'optimal' number of rows to yield to achieve a task runtime of ~10ms, on the assumption that the time to process
   * n results will be approximately the same.
   */
  private static class MergeCombineAction<T> extends RecursiveAction
  {
    private final PriorityQueue<BatchedResultsCursor<T>> pQueue;
    private final Ordering<T> orderingFn;
    private final BinaryOperator<T> combineFn;
    private final QueuePusher<OrderedResultBatch<T>> outputQueue;
    private final T initialValue;
    private final int yieldAfter;
    private final int batchSize;

    private MergeCombineAction(
        PriorityQueue<BatchedResultsCursor<T>> pQueue,
        QueuePusher<OrderedResultBatch<T>> outputQueue,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        T initialValue,
        int yieldAfter,
        int batchSize
    )
    {
      this.pQueue = pQueue;
      this.orderingFn = orderingFn;
      this.combineFn = combineFn;
      this.outputQueue = outputQueue;
      this.initialValue = initialValue;
      this.yieldAfter = yieldAfter;
      this.batchSize = batchSize;
    }

    @Override
    protected void compute()
    {
      long start = System.nanoTime();

      int counter = 0;
      int batchCounter = 0;
      OrderedResultBatch<T> outputBatch = new OrderedResultBatch<>(batchSize);

      T currentCombinedValue = initialValue;
      while (counter++ < yieldAfter && !pQueue.isEmpty()) {

        BatchedResultsCursor<T> cursor = pQueue.poll();
        // get the next value to accumulate

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
          if (batchCounter < batchSize) {
            outputBatch.add(currentCombinedValue);
            batchCounter++;
          } else {
            outputQueue.offer(outputBatch);
            outputBatch = new OrderedResultBatch<>(batchSize);
            outputBatch.add(currentCombinedValue);
            batchCounter = 1;
          }

          // next value is now current value
          currentCombinedValue = combineFn.apply(null, nextValueToAccumulate);
        } else {
          cursor.close();
        }
      }

      if (!pQueue.isEmpty()) {
        // if there is still work to be done, execute a new task with the current accumulated value to continue
        // combining where we left off
        if (!outputBatch.isDrained()) {
          outputQueue.offer(outputBatch);
        }
        final long elapsedMillis = Math.max(
            TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS),
            1L
        );
        final int nextYieldAfter = Math.max(Ints.checkedCast(10 * (yieldAfter / elapsedMillis)), 1);
        LOG.debug(
            "%s yielded results ran for %s millis, next task yielding every %s operations",
            yieldAfter,
            elapsedMillis,
            nextYieldAfter
        );
        getPool().execute(new MergeCombineAction<>(
            pQueue,
            outputQueue,
            orderingFn,
            combineFn,
            currentCombinedValue,
            nextYieldAfter,
            batchSize
        ));
      } else {
        // if priority queue is empty, push the final accumulated value
        outputBatch.add(currentCombinedValue);
        outputQueue.offer(outputBatch);
        // ... and the terminal value to indicate the blocking queue holding the values is complete
        outputQueue.offer(new OrderedResultBatch<>());
      }
    }
  }

  /**
   * This {@link RecursiveAction} waits for {@link OrderedResultBatch} to be available in a set of {@link BlockingQueue}
   * in order to construct a set of {@link BlockingQueueuBatchedResultsCursor} to feed to a {@link MergeCombineAction}
   * which will do the actual work of merging and combining the result batches. This is not needed for
   * {@link OrderedResultBatch} that are provided by a {@link Yielder} because they will have the initial result batch
   * available by virtue of translating the sequence.
   */
  private static class FinalMergeCombineSeedAction<T> extends RecursiveAction
  {
    private final List<BlockingQueue<OrderedResultBatch<T>>> queues;
    private final Ordering<T> orderingFn;
    private final BinaryOperator<T> combineFn;
    private final QueuePusher<OrderedResultBatch<T>> outputQueue;
    private final int yieldAfter;
    private final int batchSize;
    private final boolean hasTimeout;
    private final long timeoutAt;

    private FinalMergeCombineSeedAction(
        List<BlockingQueue<OrderedResultBatch<T>>> queues,
        QueuePusher<OrderedResultBatch<T>> outputQueue,
        Ordering<T> orderingFn,
        BinaryOperator<T> combineFn,
        int yieldAfter,
        int batchSize,
        boolean hasTimeout,
        long timeoutAt
    )
    {
      this.queues = queues;
      this.orderingFn = orderingFn;
      this.combineFn = combineFn;
      this.outputQueue = outputQueue;
      this.yieldAfter = yieldAfter;
      this.batchSize = batchSize;
      this.hasTimeout = hasTimeout;
      this.timeoutAt = timeoutAt;
    }

    @Override
    protected void compute()
    {
      PriorityQueue<BatchedResultsCursor<T>> cursors = new PriorityQueue<>(queues.size());
      for (BlockingQueue<OrderedResultBatch<T>> queue : queues) {
        BatchedResultsCursor<T> outputCursor =
            new BlockingQueueuBatchedResultsCursor<>(queue, orderingFn, hasTimeout, timeoutAt);
        // seed the Drainer before we can add to pQueue, this is because a blocking queue starts empty where
        // a yielder starts with the first value

        outputCursor.initialize();
        cursors.offer(outputCursor);
      }

      getPool().execute(new MergeCombineAction<T>(
          cursors,
          outputQueue,
          orderingFn,
          combineFn,
          null,
          yieldAfter,
          batchSize
      ));
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
      if (item != null) {
        if (hasTimeout) {
          final int thisTimeout = Ints.checkedCast(timeoutAt - System.currentTimeMillis());
          queue.offer(item, thisTimeout, TimeUnit.MILLISECONDS);
        } else {
          queue.offer(item);
        }
        item = null;
      }
      return true;
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
  private static class OrderedResultBatch<E>
  {
    @Nullable
    private final Queue<E> values;
    private final boolean isTerminal;

    private OrderedResultBatch(int batchSize)
    {
      this.values = new ArrayDeque<>(batchSize);
      this.isTerminal = false;
    }

    private OrderedResultBatch()
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

    public boolean isDrained()
    {
      return !isTerminal && values.isEmpty();
    }

    public boolean isTerminalResult()
    {
      return isTerminal;
    }

    /**
     * Convert sequence to yielder that accumulates results into ordered 'batches'
     */
    static <E> Yielder<OrderedResultBatch<E>> fromSequence(Sequence<E> sequence, int batchSize)
    {
      return sequence.toYielder(
          new OrderedResultBatch<>(batchSize),
          new YieldingAccumulator<OrderedResultBatch<E>, E>()
          {
            int count = 0;
            @Override
            public OrderedResultBatch<E> accumulate(OrderedResultBatch<E> accumulated, E in)
            {
              count++;
              if (/*count == 1 ||*/  count % batchSize == 0) {
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
   * Provides a higher level cursor interface to provide individual results out {@link OrderedResultBatch} provided by
   * a {@link Yielder} or {@link BlockingQueue}. This is the mechanism that powers {@link MergeCombineAction}, where
   * a set of {@link BatchedResultsCursor} are placed in a {@link PriorityQueue} to facilitate ordering to merge results
   * from these cursors, and combine results with the same ordering using the combining function.
   */
  abstract static class BatchedResultsCursor<E>
      implements ForkJoinPool.ManagedBlocker, Comparable<BatchedResultsCursor<E>>
  {
    final Ordering<E> ordering;
    volatile OrderedResultBatch<E> resultBatch;

    BatchedResultsCursor(Ordering<E> ordering)
    {
      this.ordering = ordering;
    }

    public void initialize()
    {
      // nothing to initialize for yielders since they come primed, blocking queue will need to block for some data
      // though so it is ready to go
    }

    public abstract void advance();

    public void nextBatch()
    {
      try {
        ForkJoinPool.managedBlock(this);
      }
      catch (InterruptedException e) {
        throw new RuntimeException("Failed to load next batch of results", e);
      }
    }

    public abstract boolean isDone();

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
      if (!(o instanceof ParallelMergeCombiningSequence.BatchedResultsCursor)) {
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
   * {@link BatchedResultsCursor} that wraps a {@link Yielder} of {@link OrderedResultBatch} to provide individual rows
   * of the result batch.
   */
  static class YielderBatchedResultsCursor<E> extends BatchedResultsCursor<E>
  {
    Yielder<OrderedResultBatch<E>> yielder;

    YielderBatchedResultsCursor(Yielder<OrderedResultBatch<E>> yielder, Ordering<E> ordering)
    {
      super(ordering);
      this.yielder = yielder;
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
        final Yielder<OrderedResultBatch<E>> nextYielder = yielder.next(resultBatch);
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
   * {@link BatchedResultsCursor} that wraps a {@link BlockingQueue} of {@link OrderedResultBatch} to provide individual
   * rows from the result batch.
   */
  static class BlockingQueueuBatchedResultsCursor<E> extends BatchedResultsCursor<E>
  {
    final BlockingQueue<OrderedResultBatch<E>> queue;
    final boolean hasTimeout;
    final long timeoutAt;

    BlockingQueueuBatchedResultsCursor(
        BlockingQueue<OrderedResultBatch<E>> blockingQueue,
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
      nextBatch();
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
          resultBatch = queue.poll(thisTimeout, TimeUnit.MILLISECONDS);
        } else {
          resultBatch = queue.take();
        }
      }
      return true;
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
}
