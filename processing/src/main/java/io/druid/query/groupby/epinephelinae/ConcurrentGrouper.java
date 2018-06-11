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

package io.druid.query.groupby.epinephelinae;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.collections.ReferenceCountingResourceHolder;
import io.druid.java.util.common.CloseableIterators;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.QueryInterruptedException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.segment.ColumnSelectorFactory;

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
        groupByQueryConfig.getNumParallelCombineThreads()
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
      final int numParallelCombineThreads
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

    if (!spilling) {
      final SpillingGrouper<KeyType> hashBasedGrouper = groupers.get(grouperNumberForKeyHash(keyHash));

      synchronized (hashBasedGrouper) {
        if (!spilling) {
          if (hashBasedGrouper.aggregate(key, keyHash).isOk()) {
            return AggregateResult.ok();
          } else {
            spilling = true;
          }
        }
      }
    }

    // At this point we know spilling = true
    final SpillingGrouper<KeyType> tlGrouper = threadLocalGrouper.get();

    synchronized (tlGrouper) {
      tlGrouper.setSpillingAllowed(true);
      return tlGrouper.aggregate(key, keyHash);
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

    // Parallel combine is used only when data is spilled. This is because ConcurrentGrouper uses two different modes
    // depending on data is spilled or not. If data is not spilled, all inputs are completely aggregated and no more
    // aggregation is required.
    if (sorted && spilling && parallelCombiner != null) {
      // First try to merge dictionaries generated by all underlying groupers. If it is merged successfully, the same
      // merged dictionary is used for all combining threads
      final List<String> dictionary = tryMergeDictionary();
      if (dictionary != null) {
        return parallelCombiner.combine(sortedIterators, dictionary);
      }
    }

    return sorted ?
           CloseableIterators.mergeSorted(sortedIterators, keyObjComparator) :
           CloseableIterators.concat(sortedIterators);
  }

  private boolean isParallelizable()
  {
    return concurrencyHint > 1;
  }

  private List<CloseableIterator<Entry<KeyType>>> parallelSortAndGetGroupersIterator()
  {
    // The number of groupers is same with the number of processing threads in the executor
    final ListenableFuture<List<CloseableIterator<Entry<KeyType>>>> future = Futures.allAsList(
        groupers.stream()
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
                .collect(Collectors.toList())
    );

    try {
      final long timeout = queryTimeoutAt - System.currentTimeMillis();
      return hasQueryTimeout ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
    }
    catch (InterruptedException | TimeoutException e) {
      future.cancel(true);
      throw new QueryInterruptedException(e);
    }
    catch (CancellationException e) {
      throw new QueryInterruptedException(e);
    }
    catch (ExecutionException e) {
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
