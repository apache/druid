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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
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
import io.druid.query.groupby.epinephelinae.Grouper.Entry;
import io.druid.query.groupby.epinephelinae.Grouper.KeySerdeFactory;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * ParallelCombiner builds a combining tree which asynchronously aggregates input entries.  Each node of the combining
 * tree is a combining task executed in parallel which aggregates inputs from the child nodes.
 */
public class ParallelCombiner<KeyType>
{
  // The combining tree created by this class can have two different degrees for intermediate nodes.
  // The "leaf combine degree (LCD)" is the number of leaf nodes combined together, while the "intermediate combine
  // degree (ICD)" is the number of non-leaf nodes combined together. The below picture shows an example where LCD = 2
  // and ICD = 4.
  //
  //        o         <- non-leaf node
  //     / / \ \      <- ICD = 4
  //  o   o   o   o   <- non-leaf nodes
  // / \ / \ / \ / \  <- LCD = 2
  // o o o o o o o o  <- leaf nodes
  //
  // The reason why we need two different degrees is to optimize the number of non-leaf nodes which are run by
  // different threads at the same time. Note that the leaf nodes are sorted iterators of SpillingGroupers which
  // generally returns multiple rows of the same grouping key which in turn should be combined, while the non-leaf nodes
  // are iterators of StreamingMergeSortedGroupers and always returns a single row per grouping key. Generally, the
  // performance will get better as LCD becomes low while ICD is some value larger than LCD because the amount of work
  // each thread has to do can be properly tuned. The optimal values for LCD and ICD may vary with query and data. Here,
  // we use a simple heuristic to avoid complex optimization. That is, ICD is fixed as a user-configurable value and the
  // minimum LCD satisfying the memory restriction is searched. See findLeafCombineDegreeAndNumBuffers() for more
  // details.
  private static final int MINIMUM_LEAF_COMBINE_DEGREE = 2;

  private final Supplier<ResourceHolder<ByteBuffer>> combineBufferSupplier;
  private final AggregatorFactory[] combiningFactories;
  private final KeySerdeFactory<KeyType> combineKeySerdeFactory;
  private final ListeningExecutorService executor;
  private final Comparator<Entry<KeyType>> keyObjComparator;
  private final int concurrencyHint;
  private final int priority;
  private final long queryTimeoutAt;

  // The default value is 8 which comes from an experiment. A non-leaf node will combine up to intermediateCombineDegree
  // rows for the same grouping key.
  private final int intermediateCombineDegree;

  public ParallelCombiner(
      Supplier<ResourceHolder<ByteBuffer>> combineBufferSupplier,
      AggregatorFactory[] combiningFactories,
      KeySerdeFactory<KeyType> combineKeySerdeFactory,
      ListeningExecutorService executor,
      boolean sortHasNonGroupingFields,
      int concurrencyHint,
      int priority,
      long queryTimeoutAt,
      int intermediateCombineDegree
  )
  {
    this.combineBufferSupplier = combineBufferSupplier;
    this.combiningFactories = combiningFactories;
    this.combineKeySerdeFactory = combineKeySerdeFactory;
    this.executor = executor;
    this.keyObjComparator = combineKeySerdeFactory.objectComparator(sortHasNonGroupingFields);
    this.concurrencyHint = concurrencyHint;
    this.priority = priority;
    this.intermediateCombineDegree = intermediateCombineDegree;

    this.queryTimeoutAt = queryTimeoutAt;
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
  public CloseableIterator<Entry<KeyType>> combine(
      List<? extends CloseableIterator<Entry<KeyType>>> sortedIterators,
      List<String> mergedDictionary
  )
  {
    // CombineBuffer is initialized when this method is called and closed after the result iterator is done
    final Closer closer = Closer.create();
    final ResourceHolder<ByteBuffer> combineBufferHolder = combineBufferSupplier.get();
    closer.register(combineBufferHolder);

    try {
      final ByteBuffer combineBuffer = combineBufferHolder.get();
      final int minimumRequiredBufferCapacity = StreamingMergeSortedGrouper.requiredBufferCapacity(
          combineKeySerdeFactory.factorizeWithDictionary(mergedDictionary),
          combiningFactories
      );
      // We want to maximize the parallelism while the size of buffer slice is greater than the minimum buffer size
      // required by StreamingMergeSortedGrouper. Here, we find the leafCombineDegree of the cominbing tree and the
      // required number of buffers maximizing the parallelism.
      final Pair<Integer, Integer> degreeAndNumBuffers = findLeafCombineDegreeAndNumBuffers(
          combineBuffer,
          minimumRequiredBufferCapacity,
          concurrencyHint,
          sortedIterators.size()
      );

      final int leafCombineDegree = degreeAndNumBuffers.lhs;
      final int numBuffers = degreeAndNumBuffers.rhs;
      final int sliceSize = combineBuffer.capacity() / numBuffers;

      final Supplier<ByteBuffer> bufferSupplier = createCombineBufferSupplier(combineBuffer, numBuffers, sliceSize);

      final Pair<List<CloseableIterator<Entry<KeyType>>>, List<Future>> combineIteratorAndFutures = buildCombineTree(
          sortedIterators,
          bufferSupplier,
          combiningFactories,
          leafCombineDegree,
          mergedDictionary
      );

      final CloseableIterator<Entry<KeyType>> combineIterator = Iterables.getOnlyElement(combineIteratorAndFutures.lhs);
      final List<Future> combineFutures = combineIteratorAndFutures.rhs;
      closer.register(() -> checkCombineFutures(combineFutures));

      return CloseableIterators.wrap(combineIterator, closer);
    }
    catch (Throwable t) {
      try {
        closer.close();
      }
      catch (Throwable t2) {
        t.addSuppressed(t2);
      }
      throw t;
    }
  }

  private static void checkCombineFutures(List<Future> combineFutures)
  {
    for (Future future : combineFutures) {
      try {
        if (!future.isDone()) {
          // Cancel futures if close() for the iterator is called early due to some reason (e.g., test failure)
          future.cancel(true);
        } else {
          future.get();
        }
      }
      catch (InterruptedException | CancellationException e) {
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
          return Groupers.getSlice(combineBuffer, sliceSize, i++);
        } else {
          throw new ISE("Requested number[%d] of buffer slices exceeds the planned one[%d]", i++, numBuffers);
        }
      }
    };
  }

  /**
   * Find a minimum size of the buffer slice and corresponding leafCombineDegree and number of slices.  Note that each
   * node in the combining tree is executed by different threads.  This method assumes that combining the leaf nodes
   * requires threads as many as possible, while combining intermediate nodes is not.  See the comment on
   * {@link #MINIMUM_LEAF_COMBINE_DEGREE} for more details.
   *
   * @param combineBuffer                 entire buffer used for combining tree
   * @param requiredMinimumBufferCapacity minimum buffer capacity for {@link StreamingMergeSortedGrouper}
   * @param numAvailableThreads           number of available threads
   * @param numLeafNodes                  number of leaf nodes of combining tree
   *
   * @return a pair of leafCombineDegree and number of buffers if found.
   */
  private Pair<Integer, Integer> findLeafCombineDegreeAndNumBuffers(
      ByteBuffer combineBuffer,
      int requiredMinimumBufferCapacity,
      int numAvailableThreads,
      int numLeafNodes
  )
  {
    for (int leafCombineDegree = MINIMUM_LEAF_COMBINE_DEGREE; leafCombineDegree <= numLeafNodes; leafCombineDegree++) {
      final int requiredBufferNum = computeRequiredBufferNum(numLeafNodes, leafCombineDegree);
      if (requiredBufferNum <= numAvailableThreads) {
        final int expectedSliceSize = combineBuffer.capacity() / requiredBufferNum;
        if (expectedSliceSize >= requiredMinimumBufferCapacity) {
          return Pair.of(leafCombineDegree, requiredBufferNum);
        }
      }
    }

    throw new ISE(
        "Cannot find a proper leaf combine degree for the combining tree. "
        + "Each node of the combining tree requires a buffer of [%d] bytes. "
        + "Try increasing druid.processing.buffer.sizeBytes for larger buffer or "
        + "druid.query.groupBy.intermediateCombineDegree for a smaller tree",
        requiredMinimumBufferCapacity
    );
  }

  /**
   * Recursively compute the number of required buffers for a combining tree in a bottom-up manner.  Since each node of
   * the combining tree represents a combining task and each combining task requires one buffer, the number of required
   * buffers is the number of nodes of the combining tree.
   *
   * @param numChildNodes number of child nodes
   * @param combineDegree combine degree for the current level
   *
   * @return minimum number of buffers required for combining tree
   *
   * @see #buildCombineTree(List, Supplier, AggregatorFactory[], int, List)
   */
  private int computeRequiredBufferNum(int numChildNodes, int combineDegree)
  {
    // numChildrenForLastNode used to determine that the last node is needed for the current level.
    // Please see buildCombineTree() for more details.
    final int numChildrenForLastNode = numChildNodes % combineDegree;
    final int numCurLevelNodes = numChildNodes / combineDegree + (numChildrenForLastNode > 1 ? 1 : 0);
    final int numChildOfParentNodes = numCurLevelNodes + (numChildrenForLastNode == 1 ? 1 : 0);

    if (numChildOfParentNodes == 1) {
      return numCurLevelNodes;
    } else {
      return numCurLevelNodes +
             computeRequiredBufferNum(numChildOfParentNodes, intermediateCombineDegree);
    }
  }

  /**
   * Recursively build a combining tree in a bottom-up manner.  Each node of the tree is a task that combines input
   * iterators asynchronously.
   *
   * @param childIterators     all iterators of the child level
   * @param bufferSupplier     combining buffer supplier
   * @param combiningFactories array of combining aggregator factories
   * @param combineDegree      combining degree for the current level
   * @param dictionary         merged dictionary
   *
   * @return a pair of a list of iterators of the current level in the combining tree and a list of futures of all
   * executed combining tasks
   */
  private Pair<List<CloseableIterator<Entry<KeyType>>>, List<Future>> buildCombineTree(
      List<? extends CloseableIterator<Entry<KeyType>>> childIterators,
      Supplier<ByteBuffer> bufferSupplier,
      AggregatorFactory[] combiningFactories,
      int combineDegree,
      List<String> dictionary
  )
  {
    final int numChildLevelIterators = childIterators.size();
    final List<CloseableIterator<Entry<KeyType>>> childIteratorsOfNextLevel = new ArrayList<>();
    final List<Future> combineFutures = new ArrayList<>();

    // The below algorithm creates the combining nodes of the current level. It first checks that the number of children
    // to be combined together is 1. If it is, the intermediate combining node for that child is not needed. Instead, it
    // can be directly connected to a node of the parent level. Here is an example of generated tree when
    // numLeafNodes = 6 and leafCombineDegree = intermediateCombineDegree = 2. See the description of
    // MINIMUM_LEAF_COMBINE_DEGREE for more details about leafCombineDegree and intermediateCombineDegree.
    //
    //      o
    //     / \
    //    o   \
    //   / \   \
    //  o   o   o
    // / \ / \ / \
    // o o o o o o
    //
    // We can expect that the aggregates can be combined as early as possible because the tree is built in a bottom-up
    // manner.

    for (int i = 0; i < numChildLevelIterators; i += combineDegree) {
      if (i < numChildLevelIterators - 1) {
        final List<? extends CloseableIterator<Entry<KeyType>>> subIterators = childIterators.subList(
            i,
            Math.min(i + combineDegree, numChildLevelIterators)
        );
        final Pair<CloseableIterator<Entry<KeyType>>, Future> iteratorAndFuture = runCombiner(
            subIterators,
            bufferSupplier.get(),
            combiningFactories,
            dictionary
        );

        childIteratorsOfNextLevel.add(iteratorAndFuture.lhs);
        combineFutures.add(iteratorAndFuture.rhs);
      } else {
        // If there remains one child, it can be directly connected to a node of the parent level.
        childIteratorsOfNextLevel.add(childIterators.get(i));
      }
    }

    if (childIteratorsOfNextLevel.size() == 1) {
      // This is the root
      return Pair.of(childIteratorsOfNextLevel, combineFutures);
    } else {
      // Build the parent level iterators
      final Pair<List<CloseableIterator<Entry<KeyType>>>, List<Future>> parentIteratorsAndFutures =
          buildCombineTree(
              childIteratorsOfNextLevel,
              bufferSupplier,
              combiningFactories,
              intermediateCombineDegree,
              dictionary
          );
      combineFutures.addAll(parentIteratorsAndFutures.rhs);
      return Pair.of(parentIteratorsAndFutures.lhs, combineFutures);
    }
  }

  private Pair<CloseableIterator<Entry<KeyType>>, Future> runCombiner(
      List<? extends CloseableIterator<Entry<KeyType>>> iterators,
      ByteBuffer combineBuffer,
      AggregatorFactory[] combiningFactories,
      List<String> dictionary
  )
  {
    final SettableColumnSelectorFactory settableColumnSelectorFactory =
        new SettableColumnSelectorFactory(combiningFactories);
    final StreamingMergeSortedGrouper<KeyType> grouper = new StreamingMergeSortedGrouper<>(
        Suppliers.ofInstance(combineBuffer),
        combineKeySerdeFactory.factorizeWithDictionary(dictionary),
        settableColumnSelectorFactory,
        combiningFactories,
        queryTimeoutAt
    );
    grouper.init(); // init() must be called before iterator(), so cannot be called inside the below callable.

    final ListenableFuture future = executor.submit(
        new AbstractPrioritizedCallable<Void>(priority)
        {
          @Override
          public Void call()
          {
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
            return null;
          }
        }
    );

    return new Pair<>(grouper.iterator(), future);
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
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          // do nothing
        }

        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object getObject()
        {
          return values[checkAndGetColumnIndex(columnName)];
        }
      };
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      throw new UnsupportedOperationException();
    }
  }
}
