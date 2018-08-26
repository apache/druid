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
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.epinephelinae.column.DictionaryBuildingStringGroupByColumnSelectorStrategy;
import io.druid.query.groupby.epinephelinae.column.DoubleGroupByColumnSelectorStrategy;
import io.druid.query.groupby.epinephelinae.column.FloatGroupByColumnSelectorStrategy;
import io.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import io.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import io.druid.query.groupby.epinephelinae.column.LongGroupByColumnSelectorStrategy;
import io.druid.query.groupby.epinephelinae.column.StringGroupByColumnSelectorStrategy;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class GroupByQueryEngineV2
{
  private static final GroupByStrategyFactory STRATEGY_FACTORY = new GroupByStrategyFactory();

  private static GroupByColumnSelectorPlus[] createGroupBySelectorPlus(ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] baseSelectorPlus)
  {
    GroupByColumnSelectorPlus[] retInfo = new GroupByColumnSelectorPlus[baseSelectorPlus.length];
    int curPos = 0;
    for (int i = 0; i < retInfo.length; i++) {
      retInfo[i] = new GroupByColumnSelectorPlus(baseSelectorPlus[i], curPos);
      curPos += retInfo[i].getColumnSelectorStrategy().getGroupingKeySize();
    }
    return retInfo;
  }

  private GroupByQueryEngineV2()
  {
    // No instantiation
  }

  public static Sequence<Row> process(
      final GroupByQuery query,
      final StorageAdapter storageAdapter,
      final NonBlockingPool<ByteBuffer> intermediateResultsBufferPool,
      final GroupByQueryConfig querySpecificConfig
  )
  {
    if (storageAdapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }

    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
        Filters.toFilter(query.getDimFilter()),
        intervals.get(0),
        query.getVirtualColumns(),
        query.getGranularity(),
        false,
        null
    );

    final boolean allSingleValueDims = query
        .getDimensions()
        .stream()
        .allMatch(dimension -> {
          final ColumnCapabilities columnCapabilities = storageAdapter.getColumnCapabilities(dimension.getDimension());
          return columnCapabilities != null && !columnCapabilities.hasMultipleValues();
        });

    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    final String fudgeTimestampString = Strings.emptyToNull(
        query.getContextValue(GroupByStrategyV2.CTX_KEY_FUDGE_TIMESTAMP, "")
    );

    final DateTime fudgeTimestamp = fudgeTimestampString == null
                                    ? null
                                    : DateTimes.utc(Long.parseLong(fudgeTimestampString));

    return cursors.flatMap(
        cursor -> new BaseSequence<>(
            new BaseSequence.IteratorMaker<Row, GroupByEngineIterator<?>>()
            {
              @Override
              public GroupByEngineIterator make()
              {
                ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] selectorPlus = DimensionHandlerUtils
                    .createColumnSelectorPluses(
                        STRATEGY_FACTORY,
                        query.getDimensions(),
                        cursor.getColumnSelectorFactory()
                    );
                GroupByColumnSelectorPlus[] dims = createGroupBySelectorPlus(selectorPlus);

                final ByteBuffer buffer = bufferHolder.get();

                // Check array-based aggregation is applicable
                if (isArrayAggregateApplicable(querySpecificConfig, query, dims, storageAdapter, buffer)) {
                  return new ArrayAggregateIterator(
                      query,
                      querySpecificConfig,
                      cursor,
                      buffer,
                      fudgeTimestamp,
                      dims,
                      allSingleValueDims,
                      // There must be 0 or 1 dimension if isArrayAggregateApplicable() is true
                      dims.length == 0 ? 1 : storageAdapter.getDimensionCardinality(dims[0].getName())
                  );
                } else {
                  return new HashAggregateIterator(
                      query,
                      querySpecificConfig,
                      cursor,
                      buffer,
                      fudgeTimestamp,
                      dims,
                      allSingleValueDims
                  );
                }
              }

              @Override
              public void cleanup(GroupByEngineIterator iterFromMake)
              {
                iterFromMake.close();
              }
            }
        )
    ).withBaggage(bufferHolder);
  }

  private static boolean isArrayAggregateApplicable(
      GroupByQueryConfig querySpecificConfig,
      GroupByQuery query,
      GroupByColumnSelectorPlus[] dims,
      StorageAdapter storageAdapter,
      ByteBuffer buffer
  )
  {
    if (querySpecificConfig.isForceHashAggregation()) {
      return false;
    }

    final ColumnCapabilities columnCapabilities;
    final int cardinality;

    // Find cardinality
    if (dims.length == 0) {
      columnCapabilities = null;
      cardinality = 1;
    } else if (dims.length == 1) {
      columnCapabilities = storageAdapter.getColumnCapabilities(dims[0].getName());
      cardinality = storageAdapter.getDimensionCardinality(dims[0].getName());
    } else {
      columnCapabilities = null;
      cardinality = -1; // ArrayAggregateIterator is not available
    }

    // Choose array-based aggregation if the grouping key is a single string dimension of a
    // known cardinality
    if ((columnCapabilities == null || columnCapabilities.getType().equals(ValueType.STRING))
        && cardinality > 0) {
      final AggregatorFactory[] aggregatorFactories = query
          .getAggregatorSpecs()
          .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]);
      final long requiredBufferCapacity = BufferArrayGrouper.requiredBufferCapacity(
          cardinality,
          aggregatorFactories
      );

      // Check that all keys and aggregated values can be contained the buffer
      if (requiredBufferCapacity <= buffer.capacity()) {
        return true;
      }
    }

    return false;
  }

  private static class GroupByStrategyFactory implements ColumnSelectorStrategyFactory<GroupByColumnSelectorStrategy>
  {
    @Override
    public GroupByColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities, ColumnValueSelector selector
    )
    {
      ValueType type = capabilities.getType();
      switch (type) {
        case STRING:
          DimensionSelector dimSelector = (DimensionSelector) selector;
          if (dimSelector.getValueCardinality() >= 0) {
            return new StringGroupByColumnSelectorStrategy();
          } else {
            return new DictionaryBuildingStringGroupByColumnSelectorStrategy();
          }
        case LONG:
          return new LongGroupByColumnSelectorStrategy();
        case FLOAT:
          return new FloatGroupByColumnSelectorStrategy();
        case DOUBLE:
          return new DoubleGroupByColumnSelectorStrategy();
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", type);
      }
    }
  }

  private abstract static class GroupByEngineIterator<KeyType> implements Iterator<Row>, Closeable
  {
    protected final GroupByQuery query;
    protected final GroupByQueryConfig querySpecificConfig;
    protected final Cursor cursor;
    protected final ByteBuffer buffer;
    protected final Grouper.KeySerde<ByteBuffer> keySerde;
    protected final GroupByColumnSelectorPlus[] dims;
    protected final DateTime timestamp;

    protected CloseableGrouperIterator<KeyType, Row> delegate = null;
    protected final boolean allSingleValueDims;

    public GroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig querySpecificConfig,
        final Cursor cursor,
        final ByteBuffer buffer,
        final DateTime fudgeTimestamp,
        final GroupByColumnSelectorPlus[] dims,
        final boolean allSingleValueDims
    )
    {
      this.query = query;
      this.querySpecificConfig = querySpecificConfig;
      this.cursor = cursor;
      this.buffer = buffer;
      this.keySerde = new GroupByEngineKeySerde(dims);
      this.dims = dims;

      // Time is the same for every row in the cursor
      this.timestamp = fudgeTimestamp != null ? fudgeTimestamp : cursor.getTime();
      this.allSingleValueDims = allSingleValueDims;
    }

    private CloseableGrouperIterator<KeyType, Row> initNewDelegate()
    {
      final Grouper<KeyType> grouper = newGrouper();
      grouper.init();

      if (allSingleValueDims) {
        aggregateSingleValueDims(grouper);
      } else {
        aggregateMultiValueDims(grouper);
      }

      return new CloseableGrouperIterator<>(
          grouper,
          false,
          entry -> {
            Map<String, Object> theMap = Maps.newLinkedHashMap();

            // Add dimensions.
            putToMap(entry.getKey(), theMap);

            convertRowTypesToOutputTypes(query.getDimensions(), theMap);

            // Add aggregations.
            for (int i = 0; i < entry.getValues().length; i++) {
              theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
            }

            return new MapBasedRow(timestamp, theMap);
          },
          grouper
      );
    }

    @Override
    public Row next()
    {
      if (delegate == null || !delegate.hasNext()) {
        throw new NoSuchElementException();
      }

      return delegate.next();
    }

    @Override
    public boolean hasNext()
    {
      if (delegate != null && delegate.hasNext()) {
        return true;
      } else {
        if (!cursor.isDone()) {
          if (delegate != null) {
            delegate.close();
          }
          delegate = initNewDelegate();
          return true;
        } else {
          return false;
        }
      }
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close()
    {
      if (delegate != null) {
        delegate.close();
      }
    }

    /**
     * Create a new grouper.
     */
    protected abstract Grouper<KeyType> newGrouper();

    /**
     * Grouping dimensions are all single-valued, and thus the given grouper don't have to worry about multi-valued
     * dimensions.
     */
    protected abstract void aggregateSingleValueDims(Grouper<KeyType> grouper);

    /**
     * Grouping dimensions can be multi-valued, and thus the given grouper should handle them properly during
     * aggregation.
     */
    protected abstract void aggregateMultiValueDims(Grouper<KeyType> grouper);

    /**
     * Add the key to the result map.  Some pre-processing like deserialization might be done for the key before
     * adding to the map.
     */
    protected abstract void putToMap(KeyType key, Map<String, Object> map);

    protected int getSingleValue(IndexedInts indexedInts)
    {
      Preconditions.checkArgument(indexedInts.size() < 2, "should be single value");
      return indexedInts.size() == 1 ? indexedInts.get(0) : GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE;
    }

  }

  private static class HashAggregateIterator extends GroupByEngineIterator<ByteBuffer>
  {
    private final int[] stack;
    private final Object[] valuess;
    private final ByteBuffer keyBuffer;

    private int stackPointer = Integer.MIN_VALUE;
    protected boolean currentRowWasPartiallyAggregated = false;

    public HashAggregateIterator(
        GroupByQuery query,
        GroupByQueryConfig querySpecificConfig,
        Cursor cursor,
        ByteBuffer buffer,
        DateTime fudgeTimestamp,
        GroupByColumnSelectorPlus[] dims,
        boolean allSingleValueDims
    )
    {
      super(query, querySpecificConfig, cursor, buffer, fudgeTimestamp, dims, allSingleValueDims);

      final int dimCount = query.getDimensions().size();
      stack = new int[dimCount];
      valuess = new Object[dimCount];
      keyBuffer = ByteBuffer.allocate(keySerde.keySize());
    }

    @Override
    protected Grouper<ByteBuffer> newGrouper()
    {
      return new BufferHashGrouper<>(
          Suppliers.ofInstance(buffer),
          keySerde,
          cursor.getColumnSelectorFactory(),
          query.getAggregatorSpecs()
               .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]),
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets(),
          true
      );
    }

    @Override
    protected void aggregateSingleValueDims(Grouper<ByteBuffer> grouper)
    {
      while (!cursor.isDone()) {
        for (GroupByColumnSelectorPlus dim : dims) {
          final GroupByColumnSelectorStrategy strategy = dim.getColumnSelectorStrategy();
          strategy.writeToKeyBuffer(
              dim.getKeyBufferPosition(),
              strategy.getOnlyValue(dim.getSelector()),
              keyBuffer
          );
        }
        keyBuffer.rewind();

        if (!grouper.aggregate(keyBuffer).isOk()) {
          return;
        }
        cursor.advance();
      }
    }

    @Override
    protected void aggregateMultiValueDims(Grouper<ByteBuffer> grouper)
    {
      while (!cursor.isDone()) {
        if (!currentRowWasPartiallyAggregated) {
          // Set up stack, valuess, and first grouping in keyBuffer for this row
          stackPointer = stack.length - 1;

          for (int i = 0; i < dims.length; i++) {
            GroupByColumnSelectorStrategy strategy = dims[i].getColumnSelectorStrategy();
            strategy.initColumnValues(
                dims[i].getSelector(),
                i,
                valuess
            );
            strategy.initGroupingKeyColumnValue(
                dims[i].getKeyBufferPosition(),
                i,
                valuess[i],
                keyBuffer,
                stack
            );
          }
        }

        // Aggregate groupings for this row
        boolean doAggregate = true;
        while (stackPointer >= -1) {
          // Aggregate additional grouping for this row
          if (doAggregate) {
            keyBuffer.rewind();
            if (!grouper.aggregate(keyBuffer).isOk()) {
              // Buffer full while aggregating; break out and resume later
              currentRowWasPartiallyAggregated = true;
              return;
            }
            doAggregate = false;
          }

          if (stackPointer >= 0) {
            doAggregate = dims[stackPointer].getColumnSelectorStrategy().checkRowIndexAndAddValueToGroupingKey(
                dims[stackPointer].getKeyBufferPosition(),
                valuess[stackPointer],
                stack[stackPointer],
                keyBuffer
            );

            if (doAggregate) {
              stack[stackPointer]++;
              for (int i = stackPointer + 1; i < stack.length; i++) {
                dims[i].getColumnSelectorStrategy().initGroupingKeyColumnValue(
                    dims[i].getKeyBufferPosition(),
                    i,
                    valuess[i],
                    keyBuffer,
                    stack
                );
              }
              stackPointer = stack.length - 1;
            } else {
              stackPointer--;
            }
          } else {
            stackPointer--;
          }
        }

        // Advance to next row
        cursor.advance();
        currentRowWasPartiallyAggregated = false;
      }
    }

    @Override
    protected void putToMap(ByteBuffer key, Map<String, Object> map)
    {
      for (GroupByColumnSelectorPlus selectorPlus : dims) {
        selectorPlus.getColumnSelectorStrategy().processValueFromGroupingKey(
            selectorPlus,
            key,
            map
        );
      }
    }
  }

  private static class ArrayAggregateIterator extends GroupByEngineIterator<Integer>
  {
    private final int cardinality;

    @Nullable
    private final GroupByColumnSelectorPlus dim;

    private IndexedInts multiValues;
    private int nextValIndex;

    public ArrayAggregateIterator(
        GroupByQuery query,
        GroupByQueryConfig querySpecificConfig,
        Cursor cursor,
        ByteBuffer buffer,
        DateTime fudgeTimestamp,
        GroupByColumnSelectorPlus[] dims,
        boolean allSingleValueDims,
        int cardinality
    )
    {
      super(query, querySpecificConfig, cursor, buffer, fudgeTimestamp, dims, allSingleValueDims);
      this.cardinality = cardinality;
      if (dims.length == 1) {
        this.dim = dims[0];
      } else if (dims.length == 0) {
        this.dim = null;
      } else {
        throw new IAE("Group key should be a single dimension");
      }
    }

    @Override
    protected IntGrouper newGrouper()
    {
      return new BufferArrayGrouper(
          Suppliers.ofInstance(buffer),
          cursor.getColumnSelectorFactory(),
          query.getAggregatorSpecs()
               .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]),
          cardinality
      );
    }

    @Override
    protected void aggregateSingleValueDims(Grouper<Integer> grouper)
    {
      aggregateSingleValueDims((IntGrouper) grouper);
    }

    @Override
    protected void aggregateMultiValueDims(Grouper<Integer> grouper)
    {
      aggregateMultiValueDims((IntGrouper) grouper);
    }

    private void aggregateSingleValueDims(IntGrouper grouper)
    {
      while (!cursor.isDone()) {
        final int key;
        if (dim != null) {
          // dim is always an indexed string dimension
          final IndexedInts indexedInts = ((DimensionSelector) dim.getSelector()).getRow();
          key = getSingleValue(indexedInts);
        } else {
          key = 0;
        }
        if (!grouper.aggregate(key).isOk()) {
          return;
        }
        cursor.advance();
      }
    }

    private void aggregateMultiValueDims(IntGrouper grouper)
    {
      if (dim == null) {
        throw new ISE("dim must exist");
      }

      if (multiValues == null) {
        // dim is always an indexed string dimension
        multiValues = ((DimensionSelector) dim.getSelector()).getRow();
        nextValIndex = 0;
      }

      while (!cursor.isDone()) {
        if (multiValues.size() == 0) {
          if (!grouper.aggregate(GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE).isOk()) {
            return;
          }
        } else {
          for (; nextValIndex < multiValues.size(); nextValIndex++) {
            if (!grouper.aggregate(multiValues.get(nextValIndex)).isOk()) {
              return;
            }
          }
        }

        cursor.advance();
        if (!cursor.isDone()) {
          // dim is always an indexed string dimension
          multiValues = ((DimensionSelector) dim.getSelector()).getRow();
          nextValIndex = multiValues.size() == 0 ? -1 : 0;
        }
      }
    }

    @Override
    protected void putToMap(Integer key, Map<String, Object> map)
    {
      if (dim != null) {
        if (key != -1) {
          map.put(
              dim.getOutputName(),
              ((DimensionSelector) dim.getSelector()).lookupName(key)
          );
        } else {
          map.put(dim.getOutputName(), "");
        }
      }
    }
  }

  private static void convertRowTypesToOutputTypes(List<DimensionSpec> dimensionSpecs, Map<String, Object> rowMap)
  {
    for (DimensionSpec dimSpec : dimensionSpecs) {
      final ValueType outputType = dimSpec.getOutputType();
      rowMap.compute(
          dimSpec.getOutputName(),
          (dimName, baseVal) -> DimensionHandlerUtils.convertObjectToTypeNonNull(baseVal, outputType)
      );
    }
  }

  private static class GroupByEngineKeySerde implements Grouper.KeySerde<ByteBuffer>
  {
    private final int keySize;

    public GroupByEngineKeySerde(final GroupByColumnSelectorPlus dims[])
    {
      int keySize = 0;
      for (GroupByColumnSelectorPlus selectorPlus : dims) {
        keySize += selectorPlus.getColumnSelectorStrategy().getGroupingKeySize();
      }
      this.keySize = keySize;
    }

    @Override
    public int keySize()
    {
      return keySize;
    }

    @Override
    public Class<ByteBuffer> keyClazz()
    {
      return ByteBuffer.class;
    }

    @Override
    public List<String> getDictionary()
    {
      return ImmutableList.of();
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer key)
    {
      return key;
    }

    @Override
    public ByteBuffer fromByteBuffer(ByteBuffer buffer, int position)
    {
      final ByteBuffer dup = buffer.duplicate();
      dup.position(position).limit(position + keySize);
      return dup.slice();
    }

    @Override
    public Grouper.BufferComparator bufferComparator()
    {
      // No sorting, let mergeRunners handle that
      throw new UnsupportedOperationException();
    }

    @Override
    public Grouper.BufferComparator bufferComparatorWithAggregators(
        AggregatorFactory[] aggregatorFactories, int[] aggregatorOffsets
    )
    {
      // not called on this
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset()
    {
      // No state, nothing to reset
    }
  }
}
