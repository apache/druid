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

import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class MergeWorkCallable<T> implements Callable<Sequence<T>>
{
  /**
   * Take a stream of sequences, split them as possible, and do intermediate merges. If the input stream is not
   * a parallel stream, ignore it. The stream attempts to use groups of {@code batchSize} to do its work, but this
   * goal is on a best effort basis. Input streams that cannot be split or are not sized or not subsized might not be
   * elligable for this parallelization. The intermediate merges are done in the passed in ForkJoinPool, but the final
   * merge is still done when the returned sequence accumulated.
   *
   * @param ordering      The ordering to pass into MergeSequence
   * @param baseSequences The sequences that need merged
   * @param batchSize     The input stream should be split down to this number if possible. This sets the target number of segments per merge thread work
   * @param mergeExecutor The ExecutorService to do the intermediate merges in.
   * @param <T>           The result type
   *
   * @return A Sequence that will be the merged results of the sub-sequences
   */
  public static <T> Sequence<T> parallelMerge(
      Ordering<? super T> ordering,
      Stream<? extends Sequence<? extends T>> baseSequences,
      long batchSize,
      ExecutorService mergeExecutor
  )
  {
    if (!baseSequences.isParallel()) {
      // Don't even try.
      return new MergeSequence<>(ordering, Sequences.simple(baseSequences));
    }
    @SuppressWarnings("unchecked") // Wildcard erasure is fine here
    final Spliterator<? extends Sequence<T>> baseSpliterator = (Spliterator<? extends Sequence<T>>) baseSequences.spliterator();

    final Deque<Spliterator<? extends Sequence<T>>> spliteratorStack = new LinkedList<>();

    final CompletionService<Sequence<T>> completionService = new ExecutorCompletionService<>(mergeExecutor);
    final Collection<Future> allFutures = new ArrayList<>();

    long additions = 0;

    // Push the base spliterator onto the stack, keep splitting until we can't or splits are small
    spliteratorStack.push(baseSpliterator);
    while (!spliteratorStack.isEmpty()) {
      final Spliterator<? extends Sequence<T>> pop = spliteratorStack.pop();
      if (pop.estimateSize() <= batchSize) {
        // Batch is small enough, yay!
        additions++;
        allFutures.add(completionService.submit(new MergeWorkCallable<T>(ordering, pop)));
        continue;
      }

      final Spliterator<? extends Sequence<T>> other = pop.trySplit();
      if (other == null) {
        // splits are too big, but we can't split any more
        additions++;
        allFutures.add(completionService.submit(new MergeWorkCallable<>(ordering, pop)));
        continue;
      }
      spliteratorStack.push(pop);
      spliteratorStack.push(other);
    }
    final long totalAdditions = additions;
    return new MergeSequence<>(
        ordering,
        new BaseSequence<>(
            new BaseSequence.IteratorMaker<Sequence<T>, Iterator<Sequence<T>>>()
            {
              @Override
              public Iterator<Sequence<T>> make()
              {
                return new Iterator<Sequence<T>>()
                {
                  long taken = 0L;

                  @Override
                  public boolean hasNext()
                  {
                    return taken < totalAdditions;
                  }

                  @Override
                  public Sequence<T> next() throws NoSuchElementException
                  {
                    if (taken >= totalAdditions) {
                      throw new NoSuchElementException();
                    }
                    try {
                      taken++;
                      return completionService.take().get();
                    }
                    catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException("Interrupted waiting for intermediate merge", e);
                    }
                    catch (ExecutionException e) {
                      throw new RuntimeException("Failed during intermediate merge", e);
                    }
                  }
                };
              }

              @Override
              public void cleanup(Iterator<Sequence<T>> iterFromMake)
              {
                allFutures.forEach(f -> f.cancel(true));
              }
            }
        )
    );
  }

  private final Ordering<? super T> ordering;
  private final Spliterator<? extends Sequence<T>> baseSpliterator;

  private MergeWorkCallable(Ordering<? super T> ordering, Spliterator<? extends Sequence<T>> baseSpliterator)
  {
    this.ordering = ordering;
    this.baseSpliterator = baseSpliterator;
  }

  @Override
  public Sequence<T> call()
  {
    final long estSize = baseSpliterator.estimateSize();
    final List<Sequence<T>> sequences = new ArrayList<>(estSize > 0 ? (int) estSize : 8);
    baseSpliterator.forEachRemaining((Consumer<Sequence<T>>) sequences::add);
    // Force materialization "work" in this thread
    // For singleton lists it is not clear it is even worth the optimization of short circuiting the merge for the
    // extra code maintenance overhead
    return Sequences.simple(new MergeSequence<>(ordering, Sequences.simple(sequences)).toList());
  }
}
