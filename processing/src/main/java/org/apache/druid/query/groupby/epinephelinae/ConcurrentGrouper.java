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

package org.apache.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.druid.collections.CombiningIterator;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.AbstractPrioritizedCallable;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Grouper based around a set of underlying {@link SpillingGrouper} instances. Thread-safe.
 * <p>
 * The passed-in buffer is cut up into concurrencyHint slices, and each slice is passed to a different underlying
 * grouper. Access to each slice is separately synchronized. As long as the result set fits in memory, keys are
 * partitioned between buffers based on their hash, and multiple threads can write into the same buffer. When
 * it becomes clear that the result set does not fit in memory, the table switches to a mode where each thread
 * gets its own buffer and its own spill files on disk.
 */
public class ConcurrentGrouper<KeyType> implements Grouper<KeyType>
{
  private final List<SpillingGrouper<KeyType>> groupers;
  private final ThreadLocal<SpillingGrouper<KeyType>> threadLocalGrouper;
  private final AtomicInteger threadNumber = new AtomicInteger();
  private volatile boolean spilling = false;
  private volatile boolean closed = false;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final ColumnSelectorFactory columnSelectorFactory;
  private final AggregatorFactory[] aggregatorFactories;
  private final int bufferGrouperMaxSize;
  private final float bufferGrouperMaxLoadFactor;
  private final int bufferGrouperInitialBuckets;
  private final LimitedTemporaryStorage temporaryStorage;
  private final ObjectMapper spillMapper;
  private final int concurrencyHint;
  private final KeySerdeFactory<KeyType> keySerdeFactory;
  private final DefaultLimitSpec limitSpec;
  private final boolean sortHasNonGroupingFields;
  private final Comparator<Grouper.Entry<KeyType>> keyObjComparator;
  private final ListeningExecutorService executor;
  private final int priority;
  private final boolean hasQueryTimeout;
  private final long queryTimeoutAt;
  private final long maxDictionarySizeForCombiner;
  @Nullable
  private final ParallelCombiner<KeyType> parallelCombiner;
  private final boolean mergeThreadLocal;

  private volatile boolean initialized = false;

  public ConcurrentGrouper(
      final GroupByQueryConfig groupByQueryConfig,
      final Supplier<ByteBuffer> bufferSupplier,
      @Nullable final ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final KeySerdeFactory<KeyType> combineKeySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final int concurrencyHint,
      final DefaultLimitSpec limitSpec,
      final boolean sortHasNonGroupingFields,
      final ListeningExecutorService executor,
      final int priority,
      final boolean hasQueryTimeout,
      final long queryTimeoutAt
  )
  {
    this(
        bufferSupplier,
        combineBufferHolder,
        keySerdeFactory,
        combineKeySerdeFactory,
        columnSelectorFactory,
        aggregatorFactories,
        groupByQueryConfig.getBufferGrouperMaxSize(),
        groupByQueryConfig.getBufferGrouperMaxLoadFactor(),
        groupByQueryConfig.getBufferGrouperInitialBuckets(),
        temporaryStorage,
        spillMapper,
        concurrencyHint,
        limitSpec,
        sortHasNonGroupingFields,
        executor,
        priority,
        hasQueryTimeout,
        queryTimeoutAt,
        groupByQueryConfig.getIntermediateCombineDegree(),
        groupByQueryConfig.getNumParallelCombineThreads(),
        groupByQueryConfig.isMergeThreadLocal()
    );
  }

  ConcurrentGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      @Nullable final ReferenceCountingResourceHolder<ByteBuffer> combineBufferHolder,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final KeySerdeFactory<KeyType> combineKeySerdeFactory,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float bufferGrouperMaxLoadFactor,
      final int bufferGrouperInitialBuckets,
      final LimitedTemporaryStorage temporaryStorage,
      final ObjectMapper spillMapper,
      final int concurrencyHint,
      final DefaultLimitSpec limitSpec,
      final boolean sortHasNonGroupingFields,
      final ListeningExecutorService executor,
      final int priority,
      final boolean hasQueryTimeout,
      final long queryTimeoutAt,
      final int intermediateCombineDegree,
      final int numParallelCombineThreads,
      final boolean mergeThreadLocal
  )
  {
    Preconditions.checkArgument(concurrencyHint > 0, "concurrencyHint > 0");
    Preconditions.checkArgument(
        concurrencyHint >= numParallelCombineThreads,
        "numParallelCombineThreads[%s] cannot larger than concurrencyHint[%s]",
        numParallelCombineThreads,
        concurrencyHint
    );

    this.groupers = new ArrayList<>(concurrencyHint);
    this.threadLocalGrouper = ThreadLocal.withInitial(() -> groupers.get(threadNumber.getAndIncrement()));

    this.bufferSupplier = bufferSupplier;
    this.columnSelectorFactory = columnSelectorFactory;
    this.aggregatorFactories = aggregatorFactories;
    this.bufferGrouperMaxSize = bufferGrouperMaxSize;
    this.bufferGrouperMaxLoadFactor = bufferGrouperMaxLoadFactor;
    this.bufferGrouperInitialBuckets = bufferGrouperInitialBuckets;
    this.temporaryStorage = temporaryStorage;
    this.spillMapper = spillMapper;
    this.concurrencyHint = concurrencyHint;
    this.keySerdeFactory = keySerdeFactory;
    this.limitSpec = limitSpec;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;
    this.keyObjComparator = keySerdeFactory.objectComparator(sortHasNonGroupingFields);
    this.executor = Preconditions.checkNotNull(executor);
    this.priority = priority;
    this.hasQueryTimeout = hasQueryTimeout;
    this.queryTimeoutAt = queryTimeoutAt;
    this.maxDictionarySizeForCombiner = combineKeySerdeFactory.getMaxDictionarySize();

    if (numParallelCombineThreads > 1) {
      this.parallelCombiner = new ParallelCombiner<>(
          Preconditions.checkNotNull(combineBufferHolder, "combineBufferHolder"),
          getCombiningFactories(aggregatorFactories),
          combineKeySerdeFactory,
          executor,
          sortHasNonGroupingFields,
          Math.min(numParallelCombineThreads, concurrencyHint),
          priority,
          queryTimeoutAt,
          intermediateCombineDegree
      );
    } else {
      this.parallelCombiner = null;
    }

    this.mergeThreadLocal = mergeThreadLocal;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      synchronized (bufferSupplier) {
        if (!initialized) {
          final ByteBuffer buffer = bufferSupplier.get();
          final int sliceSize = (buffer.capacity() / concurrencyHint);

          for (int i = 0; i < concurrencyHint; i++) {
            final ByteBuffer slice = Groupers.getSlice(buffer, sliceSize, i);
            final SpillingGrouper<KeyType> grouper = new SpillingGrouper<>(
                Suppliers.ofInstance(slice),
                keySerdeFactory,
                columnSelectorFactory,
                aggregatorFactories,
                bufferGrouperMaxSize,
                bufferGrouperMaxLoadFactor,
                bufferGrouperInitialBuckets,
                temporaryStorage,
                spillMapper,
                false,
                limitSpec,
                sortHasNonGroupingFields,
                sliceSize
            );
            grouper.init();
            groupers.add(grouper);

            if (mergeThreadLocal) {
              grouper.setSpillingAllowed(true);
            }
          }

          initialized = true;
        }
      }
    }
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregate(KeyType key, int keyHash)
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }

    if (closed) {
      throw new ISE("Grouper is closed");
    }

    final SpillingGrouper<KeyType> tlGrouper = threadLocalGrouper.get();

    if (mergeThreadLocal) {
      // Always thread-local grouping: expect to get more memory use, but no thread contention.
      return tlGrouper.aggregate(key, keyHash);
    } else if (spilling) {
      // Switch to thread-local grouping after spilling starts. No thread contention.
      synchronized (tlGrouper) {
        tlGrouper.setSpillingAllowed(true);
        return tlGrouper.aggregate(key, keyHash);
      }
    } else {
      // Use keyHash to find a grouper prior to spilling.
      // There is potential here for thread contention, but it reduces memory use.
      final SpillingGrouper<KeyType> subGrouper = groupers.get(grouperNumberForKeyHash(keyHash));

      synchronized (subGrouper) {
        if (subGrouper.isSpillingAllowed() && subGrouper != tlGrouper) {
          // Another thread already started treating this grouper as its thread-local grouper. So, switch to ours.
          // Fall through to release the lock on subGrouper and do the aggregation with tlGrouper.
        } else {
          final AggregateResult aggregateResult = subGrouper.aggregate(key, keyHash);

          if (aggregateResult.isOk()) {
            return AggregateResult.ok();
          } else {
            // Expecting all-or-nothing behavior.
            assert aggregateResult.getCount() == 0;
            spilling = true;

            // Fall through to release the lock on subGrouper and do the aggregation with tlGrouper.
          }
        }
      }

      synchronized (tlGrouper) {
        assert spilling;
        tlGrouper.setSpillingAllowed(true);
        return tlGrouper.aggregate(key, keyHash);
      }
    }
  }

  @Override
  public void reset()
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }

    if (closed) {
      throw new ISE("Grouper is closed");
    }

    groupers.forEach(Grouper::reset);
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }

    if (closed) {
      throw new ISE("Grouper is closed");
    }

    final List<CloseableIterator<Entry<KeyType>>> sortedIterators = sorted && isParallelizable() ?
                                                                    parallelSortAndGetGroupersIterator() :
                                                                    getGroupersIterator(sorted);

    if (sorted) {
      final boolean fullyCombined = !spilling && !mergeThreadLocal;

      // Parallel combine is used only when data is not fully merged.
      if (!fullyCombined && parallelCombiner != null) {
        // First try to merge dictionaries generated by all underlying groupers. If it is merged successfully, the same
        // merged dictionary is used for all combining threads. Otherwise, fall back to single-threaded merge.
        final List<String> dictionary = tryMergeDictionary();
        if (dictionary != null) {
          // Parallel combiner both merges and combines. Return its result directly.
          return parallelCombiner.combine(sortedIterators, dictionary);
        }
      }

      // Single-threaded merge. Still needs to be combined.
      final CloseableIterator<Entry<KeyType>> mergedIterator =
          CloseableIterators.mergeSorted(sortedIterators, keyObjComparator);

      if (fullyCombined) {
        return mergedIterator;
      } else {
        final ReusableEntry<KeyType> reusableEntry =
            ReusableEntry.create(keySerdeFactory.factorize(), aggregatorFactories.length);

        return CloseableIterators.wrap(
            new CombiningIterator<>(
                mergedIterator,
                keyObjComparator,
                (entry1, entry2) -> {
                  if (entry2 == null) {
                    // Copy key and value because we cannot retain the originals. They may be updated in-place after
                    // this method returns.
                    reusableEntry.setKey(keySerdeFactory.copyKey(entry1.getKey()));
                    System.arraycopy(entry1.getValues(), 0, reusableEntry.getValues(), 0, entry1.getValues().length);
                  } else {
                    for (int i = 0; i < aggregatorFactories.length; i++) {
                      reusableEntry.getValues()[i] = aggregatorFactories[i].combine(
                          reusableEntry.getValues()[i],
                          entry2.getValues()[i]
                      );
                    }
                  }

                  return reusableEntry;
                }
            ),
            mergedIterator
        );
      }
    } else {
      // Cannot fully combine if the caller did not request a sorted iterator. Concat and return.
      return CloseableIterators.concat(sortedIterators);
    }
  }

  private boolean isParallelizable()
  {
    return concurrencyHint > 1;
  }

  private List<CloseableIterator<Entry<KeyType>>> parallelSortAndGetGroupersIterator()
  {
    // The number of groupers is same with the number of processing threads in the executor
    final List<ListenableFuture<CloseableIterator<Entry<KeyType>>>> futures = groupers.stream()
                .map(grouper ->
                         executor.submit(
                             new AbstractPrioritizedCallable<CloseableIterator<Entry<KeyType>>>(priority)
                             {
                               @Override
                               public CloseableIterator<Entry<KeyType>> call()
                               {
                                 return grouper.iterator(true);
                               }
                             }
                         )
                )
                .collect(Collectors.toList()
    );

    ListenableFuture<List<CloseableIterator<Entry<KeyType>>>> future = Futures.allAsList(futures);
    try {
      if (!hasQueryTimeout) {
        return future.get();
      } else {
        final long timeout = queryTimeoutAt - System.currentTimeMillis();
        if (timeout > 0) {
          return future.get(timeout, TimeUnit.MILLISECONDS);
        } else {
          throw new TimeoutException();
        }
      }
    }
    catch (InterruptedException | CancellationException e) {
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryInterruptedException(e);
    }
    catch (TimeoutException e) {
      GuavaUtils.cancelAll(true, future, futures);
      throw new QueryTimeoutException();
    }
    catch (ExecutionException e) {
      GuavaUtils.cancelAll(true, future, futures);
      throw new RuntimeException(e.getCause());
    }
  }

  private List<CloseableIterator<Entry<KeyType>>> getGroupersIterator(boolean sorted)
  {
    return groupers.stream()
                   .map(grouper -> grouper.iterator(sorted))
                   .collect(Collectors.toList());
  }

  /**
   * Merge dictionaries of {@link Grouper.KeySerde}s of {@link Grouper}s.  The result dictionary contains unique string
   * keys.
   *
   * @return merged dictionary if its size does not exceed max dictionary size.  Otherwise null.
   */
  @Nullable
  private List<String> tryMergeDictionary()
  {
    final Set<String> mergedDictionary = new HashSet<>();
    long totalDictionarySize = 0L;

    for (SpillingGrouper<KeyType> grouper : groupers) {
      final List<String> dictionary = grouper.mergeAndGetDictionary();

      for (String key : dictionary) {
        if (mergedDictionary.add(key)) {
          totalDictionarySize += RowBasedGrouperHelper.estimateStringKeySize(key);
          if (totalDictionarySize > maxDictionarySizeForCombiner) {
            return null;
          }
        }
      }
    }

    return ImmutableList.copyOf(mergedDictionary);
  }

  @Override
  public void close()
  {
    if (!closed) {
      closed = true;
      groupers.forEach(Grouper::close);
    }
  }

  private int grouperNumberForKeyHash(int keyHash)
  {
    return keyHash % groupers.size();
  }

  private AggregatorFactory[] getCombiningFactories(AggregatorFactory[] aggregatorFactories)
  {
    final AggregatorFactory[] combiningFactories = new AggregatorFactory[aggregatorFactories.length];
    Arrays.setAll(combiningFactories, i -> aggregatorFactories[i].getCombiningFactory());
    return combiningFactories;
  }
}
