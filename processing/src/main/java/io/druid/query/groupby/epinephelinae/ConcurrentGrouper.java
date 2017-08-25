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
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.QueryInterruptedException;
import io.druid.query.ResourceLimitExceededException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.epinephelinae.RowBasedGrouperHelper.RowBasedKeySerde;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
  private static final int MINIMUM_COMBINE_DEGREE = 2;

  private final List<SpillingGrouper<KeyType>> groupers;
  private final ThreadLocal<SpillingGrouper<KeyType>> threadLocalGrouper;
  private final AtomicInteger threadNumber = new AtomicInteger();
  private volatile boolean spilling = false;
  private volatile boolean closed = false;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final Supplier<ResourceHolder<ByteBuffer>> combineBufferSupplier;
  private final ColumnSelectorFactory columnSelectorFactory;
  private final AggregatorFactory[] aggregatorFactories;
  private final int bufferGrouperMaxSize;
  private final float bufferGrouperMaxLoadFactor;
  private final int bufferGrouperInitialBuckets;
  private final LimitedTemporaryStorage temporaryStorage;
  private final ObjectMapper spillMapper;
  private final int concurrencyHint;
  private final KeySerdeFactory<KeyType> keySerdeFactory;
  private final KeySerdeFactory<KeyType> mergingKeySerdeFactory;
  private final DefaultLimitSpec limitSpec;
  private final boolean sortHasNonGroupingFields;
  private final Comparator<Grouper.Entry<KeyType>> keyObjComparator;
  private final ListeningExecutorService grouperSorter;
  private final int priority;
  private final boolean hasQueryTimeout;
  private final long queryTimeoutAt;
  private final boolean forceSingleThreadedCombine;

  private volatile boolean initialized = false;

  public ConcurrentGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final Supplier<ResourceHolder<ByteBuffer>> combineBufferSupplier,
      final KeySerdeFactory<KeyType> keySerdeFactory,
      final KeySerdeFactory<KeyType> mergingKeySerdeFactory,
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
      final ListeningExecutorService grouperSorter,
      final int priority,
      final boolean hasQueryTimeout,
      final long queryTimeoutAt,
      final boolean forceSingleThreadedCombine
  )
  {
    Preconditions.checkArgument(concurrencyHint > 0, "concurrencyHint > 0");

    this.groupers = new ArrayList<>(concurrencyHint);
    this.threadLocalGrouper = new ThreadLocal<SpillingGrouper<KeyType>>()
    {
      @Override
      protected SpillingGrouper<KeyType> initialValue()
      {
        return groupers.get(threadNumber.getAndIncrement());
      }
    };

    this.bufferSupplier = bufferSupplier;
    this.combineBufferSupplier = combineBufferSupplier;
    this.columnSelectorFactory = columnSelectorFactory;
    this.aggregatorFactories = aggregatorFactories;
    this.bufferGrouperMaxSize = bufferGrouperMaxSize;
    this.bufferGrouperMaxLoadFactor = bufferGrouperMaxLoadFactor;
    this.bufferGrouperInitialBuckets = bufferGrouperInitialBuckets;
    this.temporaryStorage = temporaryStorage;
    this.spillMapper = spillMapper;
    this.concurrencyHint = concurrencyHint;
    this.keySerdeFactory = keySerdeFactory;
    this.mergingKeySerdeFactory = mergingKeySerdeFactory;
    this.limitSpec = limitSpec;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;
    this.keyObjComparator = keySerdeFactory.objectComparator(sortHasNonGroupingFields);
    this.grouperSorter = Preconditions.checkNotNull(grouperSorter);
    this.priority = priority;
    this.hasQueryTimeout = hasQueryTimeout;
    this.queryTimeoutAt = queryTimeoutAt;
    this.forceSingleThreadedCombine = forceSingleThreadedCombine;
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
            final ByteBuffer slice = getSlice(buffer, sliceSize, i);
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
                sortHasNonGroupingFields
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

    for (Grouper<KeyType> grouper : groupers) {
      grouper.reset();
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    if (!initialized) {
      throw new ISE("Grouper is not initialized");
    }

    if (closed) {
      throw new ISE("Grouper is closed");
    }

    final List<Iterator<Entry<KeyType>>> sortedIterators = sorted && isParallelizable() ?
                                                           parallelSortAndGetGroupersIterator() :
                                                           getGroupersIterator(sorted);

    if (!forceSingleThreadedCombine && sorted && spilling && isParallelizable()) {
      // Parallel combine is used only when data are spilled. This is because ConcurrentGrouper uses two different modes
      // depending on data are spilled or not. If data is not spilled, all inputs are completely aggregated and no more
      // aggregation is required.
      final List<String> mergedDictionary = mergeDictionary();
      if (mergedDictionary != null) {
        return parallelCombine(sortedIterators, mergedDictionary);
      }
    }
    return Groupers.mergeIterators(sortedIterators, sorted ? keyObjComparator : null);
  }

  private boolean isParallelizable()
  {
    return concurrencyHint > 1;
  }

  private List<Iterator<Entry<KeyType>>> parallelSortAndGetGroupersIterator()
  {
    // The number of groupers is same with the number of processing threads in grouperSorter
    final ListenableFuture<List<Iterator<Entry<KeyType>>>> future = Futures.allAsList(
        groupers.stream()
                .map(grouper ->
                         grouperSorter.submit(
                             new AbstractPrioritizedCallable<Iterator<Entry<KeyType>>>(priority)
                             {
                               @Override
                               public Iterator<Entry<KeyType>> call() throws Exception
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

  private List<Iterator<Entry<KeyType>>> getGroupersIterator(boolean sorted)
  {
    return groupers.stream()
                   .map(grouper -> grouper.iterator(sorted))
                   .collect(Collectors.toList());
  }

  /**
   * Merge dictionaries of {@link Grouper.KeySerde}s of {@link Grouper}s.  The result dictionary contains unique string
   * string keys.
   *
   * @return merged dictionary if its size does not exceed max dictionary size.  Otherwise null.
   */
  @Nullable
  private List<String> mergeDictionary()
  {
    final long maxDictionarySize = mergingKeySerdeFactory.getMaxDictionarySize();
    final Set<String> mergedDictionary = new HashSet<>();
    long totalDictionarySize = 0L;

    for (SpillingGrouper<KeyType> grouper : groupers) {
      final List<String> dictionary = grouper.getDictionary();
      totalDictionarySize += dictionary.stream().mapToLong(RowBasedKeySerde::estimateStringKeySize).sum();
      if (totalDictionarySize > maxDictionarySize) {
        return null;
      }
      mergedDictionary.addAll(dictionary);
    }

    return ImmutableList.copyOf(mergedDictionary);
  }

  /**
   * Build a combining tree for the input iterators which combine input entries asynchronously.  This method is called
   * when data are spilled and thus streaming combine is preferred to avoid too many disk accesses.
   *
   * @param sortedIterators sorted iterators
   *
   * @return an iterator of the root grouper of the combining tree
   */
  private Iterator<Entry<KeyType>> parallelCombine(
      List<Iterator<Entry<KeyType>>> sortedIterators,
      List<String> mergedDictionary
  )
  {
    // CombineBuffer is initialized when this method is called
    final ResourceHolder<ByteBuffer> combineBufferHolder = combineBufferSupplier.get();
    final ByteBuffer combineBuffer = combineBufferHolder.get();
    final AggregatorFactory[] combiningFactories = new AggregatorFactory[aggregatorFactories.length];
    for (int i = 0; i < aggregatorFactories.length; i++) {
      combiningFactories[i] = aggregatorFactories[i].getCombiningFactory();
    }
    final int minimumRequiredBufferCapacity = StreamingMergeSortedGrouper.requiredBufferCapacity(
        mergingKeySerdeFactory.factorizeWithDictionary(mergedDictionary),
        combiningFactories
    );
    final Pair<Integer, Integer> degreeAndBufferNum = findCombineDegreeAndNumBuffers(
        combineBuffer,
        minimumRequiredBufferCapacity,
        concurrencyHint,
        sortedIterators.size()
    );

    if (degreeAndBufferNum == null) {
      throw new ISE("Cannot find a proper combine degree");
    }

    final int combineDegree = degreeAndBufferNum.lhs;
    final int bufferNum = degreeAndBufferNum.rhs;
    final int sliceSize = combineBuffer.capacity() / bufferNum;

    final List<ByteBuffer> slices = new ArrayList<>(bufferNum);
    for (int i = 0; i < bufferNum; i++) {
      slices.add(getSlice(combineBuffer, sliceSize, i));
    }

    final Supplier<ByteBuffer> bufferSupplier = new Supplier<ByteBuffer>()
    {
      private int i = 0;

      @Override
      public ByteBuffer get()
      {
        if (i < slices.size()) {
          return slices.get(i++);
        } else {
          throw new ISE("Requested number of buffer slices exceeds the planned one");
        }
      }
    };

    final List<Future> combineFutures = new ArrayList<>(bufferNum);
    final Iterator<Entry<KeyType>> combinedIterator = buildCombineTree(
        sortedIterators,
        bufferSupplier,
        combiningFactories,
        combineDegree,
        combineFutures,
        mergedDictionary
    );

    return new Iterator<Entry<KeyType>>()
    {
      private boolean closed;

      @Override
      public boolean hasNext()
      {
        if (!combinedIterator.hasNext()) {
          if (!closed) {
            combineBufferHolder.close();
            closed = true;

            for (Future future : combineFutures) {
              try {
                // futures should be done before reaching here and throw exceptions if they failed
                future.get();
              }
              catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            }
          }
          return false;
        }
        return true;
      }

      @Override
      public Entry<KeyType> next()
      {
        return combinedIterator.next();
      }
    };
  }

  /**
   * Find a minimum size of the buffer slice and corresponding combining degree and number of slices.  Note that each
   * node in the combining tree is executed by different threads.  This method assumes that higher degree of parallelism
   * can exploit better performance and find such a shape of the combining tree.
   *
   * @param combineBuffer                 entire buffer used for combining tree
   * @param requiredMinimumBufferCapacity minimum buffer capacity for {@link StreamingMergeSortedGrouper}
   * @param concurrencyHint               available degree of parallelism
   * @param maxDegree                     maximum degree
   *
   * @return a pair of degree and number of buffers if found. Otherwise null.
   */
  @Nullable
  private static Pair<Integer, Integer> findCombineDegreeAndNumBuffers(
      ByteBuffer combineBuffer,
      int requiredMinimumBufferCapacity,
      int concurrencyHint,
      int maxDegree
  )
  {
    for (int degree = MINIMUM_COMBINE_DEGREE; degree <= maxDegree; degree++) {
      // the number of available combine nodes is concurrencyHint because it's the max concurrency and we don't want to
      // parallelize behind that.
      final int requiredBufferNum = computeRequiredBufferNum(concurrencyHint, degree);
      final int expectedSliceSize = combineBuffer.capacity() / requiredBufferNum;
      if (expectedSliceSize > requiredMinimumBufferCapacity) {
        return Pair.of(degree, requiredBufferNum);
      }
    }
    return null;
  }

  /**
   * Recursively compute number of required buffers in a top-down manner.
   *
   * @param totalNumNodes total number of descendent nodes
   * @param degree        degree
   *
   * @return minimum number of buffers required for combining tree
   */
  private static int computeRequiredBufferNum(int totalNumNodes, int degree)
  {
    if (totalNumNodes > degree) {
      final int numNodesPerChild = (totalNumNodes + degree - 1) / degree; // ceiling
      int sum = 1; // count for the current node
      for (int i = 0; i < degree; i++) {
        // further compute for child nodes
        sum += computeRequiredBufferNum(Math.min(numNodesPerChild, totalNumNodes - i * numNodesPerChild), degree);
      }

      return sum;
    } else {
      return 1;
    }
  }

  /**
   * Recursively build a combining tree in a top-down manner.
   *
   * @param sortedIterators    sorted iterators
   * @param bufferSupplier     combining buffer supplier
   * @param combiningFactories array of combining aggregator factories
   * @param combineDegree      combining degree
   *
   * @return an iterator of the root of the combining tree
   */
  private Iterator<Entry<KeyType>> buildCombineTree(
      List<Iterator<Entry<KeyType>>> sortedIterators,
      Supplier<ByteBuffer> bufferSupplier,
      AggregatorFactory[] combiningFactories,
      int combineDegree,
      List<Future> combineFutures,
      List<String> dictionary
  )
  {
    final int numIterators = sortedIterators.size();
    if (numIterators > combineDegree) {
      final int iteratorsPerChild = (numIterators + combineDegree - 1) / combineDegree; // ceiling
      final List<Iterator<Entry<KeyType>>> childIterators = new ArrayList<>();
      for (int i = 0; i < combineDegree; i++) {
        childIterators.add(
            buildCombineTree(
                sortedIterators.subList(i * iteratorsPerChild, Math.min(numIterators, (i + 1) * iteratorsPerChild)),
                bufferSupplier,
                combiningFactories,
                combineDegree,
                combineFutures,
                dictionary
            )
        );
      }
      return runCombiner(childIterators, bufferSupplier.get(), combiningFactories, combineFutures, dictionary).iterator();
    } else {
      return runCombiner(sortedIterators, bufferSupplier.get(), combiningFactories, combineFutures, dictionary).iterator();
    }
  }

  private static ByteBuffer getSlice(ByteBuffer buffer, int sliceSize, int i)
  {
    final ByteBuffer slice = buffer.duplicate();
    slice.position(sliceSize * i);
    slice.limit(slice.position() + sliceSize);
    return slice.slice();
  }

  private StreamingMergeSortedGrouper<KeyType> runCombiner(
      List<Iterator<Entry<KeyType>>> iterators,
      ByteBuffer combineBuffer,
      AggregatorFactory[] combiningFactories,
      List<Future> combineFutures,
      List<String> dictionary
  )
  {
    final Iterator<Entry<KeyType>> mergedIterator = Groupers.mergeIterators(iterators, keyObjComparator);
    final SettableColumnSelectorFactory settableColumnSelectorFactory =
        new SettableColumnSelectorFactory(aggregatorFactories);
    final StreamingMergeSortedGrouper<KeyType> grouper = new StreamingMergeSortedGrouper<>(
        Suppliers.ofInstance(combineBuffer),
        mergingKeySerdeFactory.factorizeWithDictionary(dictionary),
        settableColumnSelectorFactory,
        combiningFactories
    );
    grouper.init();

    ListenableFuture future = grouperSorter.submit(() -> {
      while (mergedIterator.hasNext()) {
        final Entry<KeyType> next = mergedIterator.next();

        settableColumnSelectorFactory.set(next.values);
        final AggregateResult result = grouper.aggregate(next.key);
        if (!result.isOk()) {
          throw new ResourceLimitExceededException(result.getReason());
        }
        settableColumnSelectorFactory.set(null);
      }

      grouper.finish();
    });

    combineFutures.add(future);

    return grouper;
  }

  @Override
  public void close()
  {
    closed = true;
    for (Grouper<KeyType> grouper : groupers) {
      grouper.close();
    }
  }

  private int grouperNumberForKeyHash(int keyHash)
  {
    return keyHash % groupers.size();
  }

  private static class SettableColumnSelectorFactory implements ColumnSelectorFactory
  {
    private static final int UNKNOWN_COLUMN_INDEX = -1;
    private final Object2IntMap<String> columnIndexMap;

    private Object[] values;

    SettableColumnSelectorFactory(AggregatorFactory[] aggregatorFactories)
    {
      columnIndexMap = new Object2IntArrayMap<>(aggregatorFactories.length);
      columnIndexMap.defaultReturnValue(UNKNOWN_COLUMN_INDEX);
      for (int i = 0; i < aggregatorFactories.length; i++) {
        columnIndexMap.put(aggregatorFactories[i].getName(), i);
      }
    }

    public void set(Object[] values)
    {
      this.values = values;
    }

    private int checkAndGetColumnIndex(String columnName)
    {
      final int columnIndex = columnIndexMap.getInt(columnName);
      Preconditions.checkState(
          columnIndex != UNKNOWN_COLUMN_INDEX,
          "Cannot find a proper column index for column[%s]",
          columnName
      );
      return columnIndex;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float getFloat()
        {
          return ((Number) values[checkAndGetColumnIndex(columnName)]).floatValue();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // do nothing
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long getLong()
        {
          return ((Number) values[checkAndGetColumnIndex(columnName)]).longValue();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // do nothing
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double getDouble()
        {
          return ((Number) values[checkAndGetColumnIndex(columnName)]).doubleValue();
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // do nothing
        }
      };
    }

    @Nullable
    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          return values[checkAndGetColumnIndex(columnName)];
        }
      };
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      throw new UnsupportedOperationException();
    }
  }
}
