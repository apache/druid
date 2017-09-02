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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.CloseableIterators;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.AbstractPrioritizedCallable;
import io.druid.query.QueryInterruptedException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
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
  private final KeySerdeFactory<KeyType> combineKeySerdeFactory;
  private final DefaultLimitSpec limitSpec;
  private final boolean sortHasNonGroupingFields;
  private final Comparator<Grouper.Entry<KeyType>> keyObjComparator;
  private final ListeningExecutorService executor;
  private final int priority;
  private final boolean hasQueryTimeout;
  private final long queryTimeoutAt;

  private volatile boolean initialized = false;

  public ConcurrentGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final Supplier<ResourceHolder<ByteBuffer>> combineBufferSupplier,
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
      final long queryTimeoutAt
  )
  {
    Preconditions.checkArgument(concurrencyHint > 0, "concurrencyHint > 0");

    this.groupers = new ArrayList<>(concurrencyHint);
    this.threadLocalGrouper = ThreadLocal.withInitial(() -> groupers.get(threadNumber.getAndIncrement()));

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
    this.combineKeySerdeFactory = combineKeySerdeFactory;
    this.limitSpec = limitSpec;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;
    this.keyObjComparator = keySerdeFactory.objectComparator(sortHasNonGroupingFields);
    this.executor = Preconditions.checkNotNull(executor);
    this.priority = priority;
    this.hasQueryTimeout = hasQueryTimeout;
    this.queryTimeoutAt = queryTimeoutAt;
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
    if (sorted && spilling && isParallelizable()) {
      // First try to merge dictionaries generated by all underlying groupers. If it is merged successfully, the same
      // merged dictionary is used for all combining threads
      final List<String> dictionary = tryMergeDictionary();
      if (dictionary != null) {
        return parallelCombine(sortedIterators, dictionary);
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
                               public CloseableIterator<Entry<KeyType>> call() throws Exception
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
    final long maxDictionarySize = combineKeySerdeFactory.getMaxDictionarySize();
    final Set<String> mergedDictionary = new HashSet<>();
    long totalDictionarySize = 0L;

    for (SpillingGrouper<KeyType> grouper : groupers) {
      final List<String> dictionary = grouper.getDictionary();

      for (String key : dictionary) {
        if (!mergedDictionary.contains(key)) {
          totalDictionarySize += RowBasedGrouperHelper.estimateStringKeySize(key);
          if (totalDictionarySize > maxDictionarySize) {
            return null;
          }
          mergedDictionary.add(key);
        }
      }
    }

    return ImmutableList.copyOf(mergedDictionary);
  }

  /**
   * Build a combining tree for the input iterators which combine input entries asynchronously.  Each node in the tree
   * is a combining task which iterates through child iterators, aggregates the inputs from those iterators, and returns
   * an iterator for the result of aggregation.
   * <p>
   * This method is called when data is spilled and thus streaming combine is preferred to avoid too many disk accesses.
   *
   * @return an iterator of the root grouper of the combining tree
   */
  private CloseableIterator<Entry<KeyType>> parallelCombine(
      List<CloseableIterator<Entry<KeyType>>> sortedIterators,
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
        combineKeySerdeFactory.factorizeWithDictionary(mergedDictionary),
        combiningFactories
    );
    // We want to maximize the parallelism while the size of buffer slice is greater than the minimum buffer size
    // required by StreamingMergeSortedGrouper. Here, we find the degree of the cominbing tree and the required number
    // of buffers maximizing the degree of parallelism.
    final Pair<Integer, Integer> degreeAndNumBuffers = findCombineDegreeAndNumBuffers(
        combineBuffer,
        minimumRequiredBufferCapacity,
        concurrencyHint,
        sortedIterators.size()
    );

    final int combineDegree = degreeAndNumBuffers.lhs;
    final int numBuffers = degreeAndNumBuffers.rhs;
    final int sliceSize = combineBuffer.capacity() / numBuffers;

    final Supplier<ByteBuffer> bufferSupplier = createCombineBufferSupplier(combineBuffer, numBuffers, sliceSize);

    final Pair<CloseableIterator<Entry<KeyType>>, List<Future>> combineIteratorAndFutures = buildCombineTree(
        sortedIterators,
        bufferSupplier,
        combiningFactories,
        combineDegree,
        mergedDictionary
    );

    final CloseableIterator<Entry<KeyType>> combineIterator = combineIteratorAndFutures.lhs;
    final List<Future> combineFutures = combineIteratorAndFutures.rhs;

    final Closer closer = Closer.create();
    closer.register(combineBufferHolder);
    closer.register(() -> checkCombineFutures(combineFutures));

    return CloseableIterators.wrap(combineIterator, closer);
  }

  private static void checkCombineFutures(List<Future> combineFutures)
  {
    for (Future future : combineFutures) {
      try {
        // futures should be done before reaching here and throw exceptions if they failed
        future.get();
      }
      catch (InterruptedException e) {
        throw new QueryInterruptedException(e);
      }
      catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static Supplier<ByteBuffer> createCombineBufferSupplier(
      ByteBuffer combineBuffer,
      int numBuffers,
      int sliceSize
  )
  {
    return new Supplier<ByteBuffer>()
    {
      private int i = 0;

      @Override
      public ByteBuffer get()
      {
        if (i < numBuffers) {
          return getSlice(combineBuffer, sliceSize, i++);
        } else {
          throw new ISE("Requested number of buffer slices exceeds the planned one");
        }
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
   * @param numLeafNodes                  number of leaf nodes of combining tree
   *
   * @return a pair of degree and number of buffers if found.
   */
  private static Pair<Integer, Integer> findCombineDegreeAndNumBuffers(
      ByteBuffer combineBuffer,
      int requiredMinimumBufferCapacity,
      int concurrencyHint,
      int numLeafNodes
  )
  {
    for (int degree = MINIMUM_COMBINE_DEGREE; degree <= numLeafNodes; degree++) {
      final int requiredBufferNum = computeRequiredBufferNum(numLeafNodes, degree);
      if (requiredBufferNum <= concurrencyHint) {
        final int expectedSliceSize = combineBuffer.capacity() / requiredBufferNum;
        if (expectedSliceSize >= requiredMinimumBufferCapacity) {
          return Pair.of(degree, requiredBufferNum);
        }
      }
    }

    throw new ISE("Cannot find a proper combine degree");
  }

  /**
   * Recursively compute the number of required buffers for a combining tree in a top-down manner.  Since each node of
   * the combining tree represents a combining task and each combining task requires one buffer, the number of required
   * buffers is the number of nodes of the combining tree.
   *
   * @param numLeafNodes  number of leaf nodes
   * @param combineDegree combine degree
   *
   * @return minimum number of buffers required for combining tree
   *
   * @see {@link #buildCombineTree(List, Supplier, AggregatorFactory[], int, List)}
   */
  private static int computeRequiredBufferNum(int numLeafNodes, int combineDegree)
  {
    if (numLeafNodes > combineDegree) {
      final int numLeafNodesPerChild = (numLeafNodes + combineDegree - 1) / combineDegree; // ceiling
      int sum = 1; // count for the current node
      for (int i = 0; i < combineDegree; i++) {
        // further compute for child nodes
        sum += computeRequiredBufferNum(
            Math.min(numLeafNodesPerChild, numLeafNodes - i * numLeafNodesPerChild),
            combineDegree
        );
      }

      return sum;
    } else {
      return 1;
    }
  }

  /**
   * Recursively build a combining tree in a top-down manner.  Each node of the tree is a task that combines input
   * iterators asynchronously.
   *
   * @param sortedIterators    sorted iterators
   * @param bufferSupplier     combining buffer supplier
   * @param combiningFactories array of combining aggregator factories
   * @param combineDegree      combining degree
   * @param dictionary         merged dictionary
   *
   * @return a pair of an iterator of the root of the combining tree and a list of futures of all executed combining
   * tasks
   */
  private Pair<CloseableIterator<Entry<KeyType>>, List<Future>> buildCombineTree(
      List<CloseableIterator<Entry<KeyType>>> sortedIterators,
      Supplier<ByteBuffer> bufferSupplier,
      AggregatorFactory[] combiningFactories,
      int combineDegree,
      List<String> dictionary
  )
  {
    final int numIterators = sortedIterators.size();
    if (numIterators > combineDegree) {
      final List<CloseableIterator<Entry<KeyType>>> childIterators = new ArrayList<>(combineDegree);
      final List<Future> combineFutures = new ArrayList<>(combineDegree + 1);

      final int iteratorsPerChild = (numIterators + combineDegree - 1) / combineDegree; // ceiling
      for (int i = 0; i < combineDegree; i++) {
        final Pair<CloseableIterator<Entry<KeyType>>, List<Future>> childIteratorAndFutures = buildCombineTree(
            sortedIterators.subList(i * iteratorsPerChild, Math.min(numIterators, (i + 1) * iteratorsPerChild)),
            bufferSupplier,
            combiningFactories,
            combineDegree,
            dictionary
        );
        childIterators.add(childIteratorAndFutures.lhs);
        combineFutures.addAll(childIteratorAndFutures.rhs);
      }
      final Pair<CloseableIterator<Entry<KeyType>>, Future> iteratorAndFuture = runCombiner(
          childIterators,
          bufferSupplier.get(),
          combiningFactories,
          dictionary
      );
      combineFutures.add(iteratorAndFuture.rhs);
      return new Pair<>(iteratorAndFuture.lhs, combineFutures);
    } else {
      final Pair<CloseableIterator<Entry<KeyType>>, Future> iteratorAndFuture = runCombiner(
          sortedIterators,
          bufferSupplier.get(),
          combiningFactories,
          dictionary
      );
      return new Pair<>(iteratorAndFuture.lhs, Collections.singletonList(iteratorAndFuture.rhs));
    }
  }

  private static ByteBuffer getSlice(ByteBuffer buffer, int sliceSize, int i)
  {
    final ByteBuffer slice = buffer.duplicate();
    slice.position(sliceSize * i);
    slice.limit(slice.position() + sliceSize);
    return slice.slice();
  }

  private Pair<CloseableIterator<Entry<KeyType>>, Future> runCombiner(
      List<CloseableIterator<Entry<KeyType>>> iterators,
      ByteBuffer combineBuffer,
      AggregatorFactory[] combiningFactories,
      List<String> dictionary
  )
  {
    final SettableColumnSelectorFactory settableColumnSelectorFactory =
        new SettableColumnSelectorFactory(aggregatorFactories);
    final StreamingMergeSortedGrouper<KeyType> grouper = new StreamingMergeSortedGrouper<>(
        Suppliers.ofInstance(combineBuffer),
        combineKeySerdeFactory.factorizeWithDictionary(dictionary),
        settableColumnSelectorFactory,
        combiningFactories
    );

    final ListenableFuture future = executor.submit(() -> {
      grouper.init();

      try (
          CloseableIterator<Entry<KeyType>> mergedIterator = CloseableIterators.mergeSorted(
              iterators,
              keyObjComparator
          )
      ) {
        while (mergedIterator.hasNext()) {
          final Entry<KeyType> next = mergedIterator.next();

          settableColumnSelectorFactory.set(next.values);
          grouper.aggregate(next.key); // grouper always returns ok or throws an exception
          settableColumnSelectorFactory.set(null);
        }
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }

      grouper.finish();
    });

    return new Pair<>(grouper.iterator(), future);
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
