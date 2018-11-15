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
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.BaseSequence.IteratorMaker;
import org.apache.druid.java.util.common.guava.nary.BinaryFn;
import org.apache.druid.query.ParallelCombines;
import org.apache.druid.query.PrioritizedRunnable;
import org.apache.druid.query.ThreadResource;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * This sequence merges underlying {@link #baseSequences} and combines (aggregates) them in parallel.
 * It creates a tree to merge and combine input streams in parallel, consisting of processing threads connected by
 * blocking queues. In leaf nodes, the processing threads combines values from the {@link #baseSequences} and stores
 * aggregates in its blocking queue. Intermediate threads reads values from the blocking queue of their children and
 * stores computed aggregates its blocking queue. Finally, the caller thread of this class (usually it's an http thread
 * in query processing) reads values from the queue of the root thread. Filling blocking queue and reading from it are
 * done asynchronously.
 */
public class ParallelMergeCombineSequence<T> extends YieldingSequenceBase<T>
{
  private static final int MINIMUM_LEAF_COMBINE_DEGREE = 2;

  private final ExecutorService exec;
  private final List<Sequence<T>> baseSequences;
  private final Ordering<T> ordering;
  private final BinaryFn<T, T, T> combineFn;
  private final List<ReferenceCountingResourceHolder<ThreadResource>> processingThreadHolders;
  private final int queueSize;
  private final boolean hasTimeout;
  private final long timeoutAt;
  private final int queryPriority;

  public ParallelMergeCombineSequence(
      ExecutorService exec,
      List<? extends Sequence<? extends T>> baseSequences,
      Ordering<T> ordering,
      BinaryFn<T, T, T> combineFn,
      List<ReferenceCountingResourceHolder<ThreadResource>> processingThreadHolders,
      int queueSize,
      boolean hasTimeout,
      long timeout,
      int queryPriority
  )
  {
    this.exec = exec;
    this.baseSequences = (List<Sequence<T>>) baseSequences;
    this.ordering = ordering;
    this.combineFn = combineFn;
    this.processingThreadHolders = processingThreadHolders;
    this.queueSize = queueSize;
    this.hasTimeout = hasTimeout;
    this.timeoutAt = System.currentTimeMillis() + timeout;
    this.queryPriority = queryPriority;
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
  {
    final Pair<Integer, Integer> combineDegreeAndNumThreads = findCombineDegreeAndNumThreads(
        processingThreadHolders.size(),
        baseSequences.size()
    );
    final int combineDegree = combineDegreeAndNumThreads.lhs;
    final int numThreads = combineDegreeAndNumThreads.rhs;

    // Early release unnecessary processing threads
    IntStream.range(0, processingThreadHolders.size() - numThreads)
             .forEach(i -> processingThreadHolders.remove(0).close());

    final Supplier<ReferenceCountingResourceHolder<ThreadResource>> processingThreadSupplier =
        new Supplier<ReferenceCountingResourceHolder<ThreadResource>>()
        {
          private int next = 0;

          @Override
          public ReferenceCountingResourceHolder<ThreadResource> get()
          {
            if (next < processingThreadHolders.size()) {
              return processingThreadHolders.get(next++);
            } else {
              throw new ISE(
                  "WTH? current pointer[%d] is larger than available threads[%d]",
                  next,
                  processingThreadHolders.size()
              );
            }
          }
        };

    // In the result combine tree, nodes and edges are processing threads and blocking queues, respectively.
    // In the leaf nodes, processing threads read data from historicals and fill its blocking queue.
    // In the intermediate nodes, processing threads read data from the blocking queues of their children and fill
    // its blocking queue.
    // The caller thread reads the blocking queue of the root node.
    final Pair<Sequence<T>, List<Future>> rootAndFutures = ParallelCombines.buildCombineTree(
        baseSequences,
        combineDegree,
        combineDegree,
        sequences -> runCombine(processingThreadSupplier.get(), sequences)
    );

    // Ignore futures since they are handled in the generated BaseSequence.IteratorMaker.cleanup().
    return rootAndFutures.lhs.toYielder(initValue, accumulator);
  }

  private static Pair<Integer, Integer> findCombineDegreeAndNumThreads(int numAvailableThreads, int numLeafNodes)
  {
    for (int combineDegree = MINIMUM_LEAF_COMBINE_DEGREE; combineDegree <= numLeafNodes; combineDegree++) {
      final int numRequiredThreads = computeNumRequiredThreads(numLeafNodes, combineDegree);
      if (numRequiredThreads <= numAvailableThreads) {
        return Pair.of(combineDegree, numRequiredThreads);
      }
    }

    throw new ISE(
        "Cannot find a proper combine degree for the combining tree. "
        + "Each node of the combining tree requires a single thread. "
        + "Try increasing druid.processing.numThreads or "
        + "reducing numBrokerParallelCombineThreads[%d] in the query context for a smaller tree",
        numAvailableThreads
    );
  }

  private static int computeNumRequiredThreads(int numChildNodes, int combineDegree)
  {
    // numChildNodes is used to determine that the last node is needed for the current level.
    // Please see ParallelCombines.buildCombineTree() for more details.
    final int numChildrenForLastNode = numChildNodes % combineDegree;
    final int numCurLevelNodes = numChildNodes / combineDegree + (numChildrenForLastNode > 1 ? 1 : 0);
    final int numChildOfParentNodes = numCurLevelNodes + (numChildrenForLastNode == 1 ? 1 : 0);

    if (numChildOfParentNodes == 1) {
      return numCurLevelNodes;
    } else {
      return numCurLevelNodes +
             computeNumRequiredThreads(numChildOfParentNodes, combineDegree);
    }
  }

  private Pair<Sequence<T>, Future> runCombine(
      ReferenceCountingResourceHolder processingThreadHolder,
      List<Sequence<T>> sequenceList
  )
  {
    final Sequence<? extends Sequence<T>> sequences = Sequences.simple(sequenceList);
    final CombiningSequence<T> combiningSequence = CombiningSequence.create(
        new MergeSequence<>(ordering, sequences),
        ordering,
        combineFn
    );

    final BlockingQueue<ValueHolder> queue = new ArrayBlockingQueue<>(queueSize);

    final Future future = exec.submit(
        new PrioritizedRunnable()
        {
          @Override
          public int getPriority()
          {
            return queryPriority;
          }

          @Override
          public void run()
          {
            try {
              combiningSequence.accumulate(
                  queue,
                  (theQueue, v) -> {
                    try {
                      addToQueue(theQueue, new ValueHolder(v));
                    }
                    catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                    return theQueue;
                  }
              );
              // add a null to indicate this is the last one
              addToQueue(queue, new ValueHolder(null));
            }
            catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            finally {
              processingThreadHolder.close();
            }
          }
        }
    );

    final Sequence<T> backgroundCombineSequence = new BaseSequence<>(
        new IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return new Iterator<T>()
            {
              private T nextVal;

              @Override
              public boolean hasNext()
              {
                try {
                  final ValueHolder holder;
                  if (!hasTimeout) {
                    holder = queue.take();
                  } else {
                    final long timeout = timeoutAt - System.currentTimeMillis();
                    holder = queue.poll(timeout, TimeUnit.MILLISECONDS);
                  }

                  if (holder == null) {
                    throw new RuntimeException(new TimeoutException());
                  }
                  nextVal = holder.val;
                  return nextVal != null;
                }
                catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public T next()
              {
                if (nextVal == null) {
                  throw new NoSuchElementException();
                }
                return nextVal;
              }
            };
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            try {
              if (future.isDone()) {
                future.get();
              } else {
                future.cancel(true);
              }
            }
            catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          }
        }
    );

    return Pair.of(backgroundCombineSequence, future);
  }

  private void addToQueue(BlockingQueue<ValueHolder> queue, ValueHolder holder)
      throws InterruptedException
  {
    if (!hasTimeout) {
      queue.put(holder);
    } else {
      final long timeout = timeoutAt - System.currentTimeMillis();
      if (!queue.offer(holder, timeout, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException(new TimeoutException());
      }
    }
  }

  private class ValueHolder
  {
    @Nullable
    private final T val;

    private ValueHolder(@Nullable T val)
    {
      this.val = val;
    }
  }
}
