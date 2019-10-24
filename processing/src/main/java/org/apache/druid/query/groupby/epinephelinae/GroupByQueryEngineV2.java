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

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.QueryConfig;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.column.DictionaryBuildingStringGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.DoubleGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.FloatGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.LongGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.NullableValueGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.StringGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * Class that knows how to process a groupBy query on a single {@link StorageAdapter}. It returns a {@link Sequence}
 * of {@link ResultRow} objects that are not guaranteed to be in any particular order, and may not even be fully
 * grouped. It is expected that a downstream {@link GroupByMergingQueryRunnerV2} will finish grouping these results.
 *
 * This code runs on data servers, like Historicals.
 *
 * Used by
 * {@link GroupByStrategyV2#process(GroupByQuery, StorageAdapter)}.
 */
public class GroupByQueryEngineV2
{
  private static final GroupByStrategyFactory STRATEGY_FACTORY = new GroupByStrategyFactory();

  private static GroupByColumnSelectorPlus[] createGroupBySelectorPlus(
      ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] baseSelectorPlus,
      int dimensionStart
  )
  {
    GroupByColumnSelectorPlus[] retInfo = new GroupByColumnSelectorPlus[baseSelectorPlus.length];
    int curPos = 0;
    for (int i = 0; i < retInfo.length; i++) {
      retInfo[i] = new GroupByColumnSelectorPlus(baseSelectorPlus[i], curPos, dimensionStart + i);
      curPos += retInfo[i].getColumnSelectorStrategy().getGroupingKeySize();
    }
    return retInfo;
  }

  private GroupByQueryEngineV2()
  {
    // No instantiation
  }

  public static Sequence<ResultRow> process(
      final GroupByQuery query,
      @Nullable final StorageAdapter storageAdapter,
      final NonBlockingPool<ByteBuffer> intermediateResultsBufferPool,
      final GroupByQueryConfig querySpecificConfig,
      final QueryConfig queryConfig
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

    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    final String fudgeTimestampString = NullHandling.emptyToNullIfNeeded(
        query.getContextValue(GroupByStrategyV2.CTX_KEY_FUDGE_TIMESTAMP, null)
    );

    final DateTime fudgeTimestamp = fudgeTimestampString == null
                                    ? null
                                    : DateTimes.utc(Long.parseLong(fudgeTimestampString));

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
    final Interval interval = Iterables.getOnlyElement(query.getIntervals());

    final boolean doVectorize = queryConfig.getVectorize().shouldVectorize(
        VectorGroupByEngine.canVectorize(query, storageAdapter, filter)
    );

    final Sequence<ResultRow> result;

    if (doVectorize) {
      result = VectorGroupByEngine.process(
          query,
          storageAdapter,
          bufferHolder.get(),
          fudgeTimestamp,
          filter,
          interval,
          querySpecificConfig,
          queryConfig
      );
    } else {
      result = processNonVectorized(
          query,
          storageAdapter,
          bufferHolder.get(),
          fudgeTimestamp,
          querySpecificConfig,
          filter,
          interval
      );
    }

    return result.withBaggage(bufferHolder);
  }

  private static Sequence<ResultRow> processNonVectorized(
      final GroupByQuery query,
      final StorageAdapter storageAdapter,
      final ByteBuffer processingBuffer,
      @Nullable final DateTime fudgeTimestamp,
      final GroupByQueryConfig querySpecificConfig,
      @Nullable final Filter filter,
      final Interval interval
  )
  {
    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
        filter,
        interval,
        query.getVirtualColumns(),
        query.getGranularity(),
        false,
        null
    );

    return cursors.flatMap(
        cursor -> new BaseSequence<>(
            new BaseSequence.IteratorMaker<ResultRow, GroupByEngineIterator<?>>()
            {
              @Override
              public GroupByEngineIterator make()
              {
                final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
                final ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] selectorPlus = DimensionHandlerUtils
                    .createColumnSelectorPluses(
                        STRATEGY_FACTORY,
                        query.getDimensions(),
                        columnSelectorFactory
                    );
                final GroupByColumnSelectorPlus[] dims = createGroupBySelectorPlus(
                    selectorPlus,
                    query.getResultRowDimensionStart()
                );

                final int cardinalityForArrayAggregation = getCardinalityForArrayAggregation(
                    querySpecificConfig,
                    query,
                    storageAdapter,
                    processingBuffer
                );

                if (cardinalityForArrayAggregation >= 0) {
                  return new ArrayAggregateIterator(
                      query,
                      querySpecificConfig,
                      cursor,
                      processingBuffer,
                      fudgeTimestamp,
                      dims,
                      isAllSingleValueDims(columnSelectorFactory::getColumnCapabilities, query.getDimensions()),
                      cardinalityForArrayAggregation
                  );
                } else {
                  return new HashAggregateIterator(
                      query,
                      querySpecificConfig,
                      cursor,
                      processingBuffer,
                      fudgeTimestamp,
                      dims,
                      isAllSingleValueDims(columnSelectorFactory::getColumnCapabilities, query.getDimensions())
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
    );
  }

  /**
   * Returns the cardinality of array needed to do array-based aggregation, or -1 if array-based aggregation
   * is impossible.
   */
  public static int getCardinalityForArrayAggregation(
      GroupByQueryConfig querySpecificConfig,
      GroupByQuery query,
      StorageAdapter storageAdapter,
      ByteBuffer buffer
  )
  {
    if (querySpecificConfig.isForceHashAggregation()) {
      return -1;
    }

    final List<DimensionSpec> dimensions = query.getDimensions();
    final ColumnCapabilities columnCapabilities;
    final int cardinality;

    // Find cardinality
    if (dimensions.isEmpty()) {
      columnCapabilities = null;
      cardinality = 1;
    } else if (dimensions.size() == 1) {
      // Only real columns can use array-based aggregation, since virtual columns cannot currently report their
      // cardinality. We need to check if a virtual column exists with the same name, since virtual columns can shadow
      // real columns, and we might miss that since we're going directly to the StorageAdapter (which only knows about
      // real columns).
      if (query.getVirtualColumns().exists(Iterables.getOnlyElement(dimensions).getDimension())) {
        return -1;
      }

      final String columnName = Iterables.getOnlyElement(dimensions).getDimension();
      columnCapabilities = storageAdapter.getColumnCapabilities(columnName);
      cardinality = storageAdapter.getDimensionCardinality(columnName);
    } else {
      // Cannot use array-based aggregation with more than one dimension.
      return -1;
    }

    // Choose array-based aggregation if the grouping key is a single string dimension of a known cardinality
    if (columnCapabilities != null && columnCapabilities.getType().equals(ValueType.STRING) && cardinality > 0) {
      final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs().toArray(new AggregatorFactory[0]);
      final long requiredBufferCapacity = BufferArrayGrouper.requiredBufferCapacity(
          cardinality,
          aggregatorFactories
      );

      // Check that all keys and aggregated values can be contained in the buffer
      return requiredBufferCapacity <= buffer.capacity() ? cardinality : -1;
    } else {
      return -1;
    }
  }

  /**
   * Checks whether all "dimensions" are either single-valued or nonexistent (which is just as good as single-valued,
   * since their selectors will show up as full of nulls).
   */
  public static boolean isAllSingleValueDims(
      final Function<String, ColumnCapabilities> capabilitiesFunction,
      final List<DimensionSpec> dimensions
  )
  {
    return dimensions
        .stream()
        .allMatch(
            dimension -> {
              if (dimension.mustDecorate()) {
                // DimensionSpecs that decorate may turn singly-valued columns into multi-valued selectors.
                // To be safe, we must return false here.
                return false;
              }

              // Now check column capabilities.
              final ColumnCapabilities columnCapabilities = capabilitiesFunction.apply(dimension.getDimension());
              return columnCapabilities == null || !columnCapabilities.hasMultipleValues();
            });
  }

  private static class GroupByStrategyFactory
      implements ColumnSelectorStrategyFactory<GroupByColumnSelectorStrategy>
  {
    @Override
    public GroupByColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities,
        ColumnValueSelector selector
    )
    {
      ValueType type = capabilities.getType();
      switch (type) {
        case STRING:
          DimensionSelector dimSelector = (DimensionSelector) selector;
          if (dimSelector.getValueCardinality() >= 0) {
            return new StringGroupByColumnSelectorStrategy(dimSelector::lookupName);
          } else {
            return new DictionaryBuildingStringGroupByColumnSelectorStrategy();
          }
        case LONG:
          return makeNullableStrategy(new LongGroupByColumnSelectorStrategy());
        case FLOAT:
          return makeNullableStrategy(new FloatGroupByColumnSelectorStrategy());
        case DOUBLE:
          return makeNullableStrategy(new DoubleGroupByColumnSelectorStrategy());
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", type);
      }
    }

    private GroupByColumnSelectorStrategy makeNullableStrategy(GroupByColumnSelectorStrategy delegate)
    {
      if (NullHandling.sqlCompatible()) {
        return new NullableValueGroupByColumnSelectorStrategy(delegate);
      } else {
        return delegate;
      }
    }
  }

  private abstract static class GroupByEngineIterator<KeyType> implements Iterator<ResultRow>, Closeable
  {
    protected final GroupByQuery query;
    protected final GroupByQueryConfig querySpecificConfig;
    protected final Cursor cursor;
    protected final ByteBuffer buffer;
    protected final Grouper.KeySerde<ByteBuffer> keySerde;
    protected final GroupByColumnSelectorPlus[] dims;
    protected final DateTime timestamp;

    @Nullable
    protected CloseableGrouperIterator<KeyType, ResultRow> delegate = null;
    protected final boolean allSingleValueDims;

    public GroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig querySpecificConfig,
        final Cursor cursor,
        final ByteBuffer buffer,
        @Nullable final DateTime fudgeTimestamp,
        final GroupByColumnSelectorPlus[] dims,
        final boolean allSingleValueDims
    )
    {
      this.query = query;
      this.querySpecificConfig = querySpecificConfig;
      this.cursor = cursor;
      this.buffer = buffer;
      this.keySerde = new GroupByEngineKeySerde(dims, query);
      this.dims = dims;

      // Time is the same for every row in the cursor
      this.timestamp = fudgeTimestamp != null ? fudgeTimestamp : cursor.getTime();
      this.allSingleValueDims = allSingleValueDims;
    }

    private CloseableGrouperIterator<KeyType, ResultRow> initNewDelegate()
    {
      final Grouper<KeyType> grouper = newGrouper();
      grouper.init();

      if (allSingleValueDims) {
        aggregateSingleValueDims(grouper);
      } else {
        aggregateMultiValueDims(grouper);
      }

      final boolean resultRowHasTimestamp = query.getResultRowHasTimestamp();
      final int resultRowDimensionStart = query.getResultRowDimensionStart();
      final int resultRowAggregatorStart = query.getResultRowAggregatorStart();

      return new CloseableGrouperIterator<>(
          grouper.iterator(false),
          entry -> {
            final ResultRow resultRow = ResultRow.create(query.getResultRowSizeWithoutPostAggregators());

            // Add timestamp, if necessary.
            if (resultRowHasTimestamp) {
              resultRow.set(0, timestamp.getMillis());
            }

            // Add dimensions, and convert their types if necessary.
            putToRow(entry.getKey(), resultRow);
            convertRowTypesToOutputTypes(query.getDimensions(), resultRow, resultRowDimensionStart);

            // Add aggregations.
            for (int i = 0; i < entry.getValues().length; i++) {
              resultRow.set(resultRowAggregatorStart + i, entry.getValues()[i]);
            }

            return resultRow;
          },
          grouper
      );
    }

    @Override
    public ResultRow next()
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
          return delegate.hasNext();
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
     * Add the key to the result row.  Some pre-processing like deserialization might be done for the key before
     * adding to the map.
     */
    protected abstract void putToRow(KeyType key, ResultRow resultRow);

    protected int getSingleValue(IndexedInts indexedInts)
    {
      Preconditions.checkArgument(indexedInts.size() < 2, "should be single value");
      return indexedInts.size() == 1 ? indexedInts.get(0) : GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE;
    }

  }

  private static class HashAggregateIterator extends GroupByEngineIterator<ByteBuffer>
  {
    private static final Logger LOGGER = new Logger(HashAggregateIterator.class);

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
        @Nullable DateTime fudgeTimestamp,
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
      Grouper grouper = null;
      final DefaultLimitSpec limitSpec = query.isApplyLimitPushDown() &&
                                         querySpecificConfig.isApplyLimitPushDownToSegment() ?
                                         (DefaultLimitSpec) query.getLimitSpec() : null;
      if (limitSpec != null) {
        LimitedBufferHashGrouper limitGrouper = new LimitedBufferHashGrouper<>(
            Suppliers.ofInstance(buffer),
            keySerde,
            AggregatorAdapters.factorizeBuffered(
                cursor.getColumnSelectorFactory(),
                query.getAggregatorSpecs()
            ),
            querySpecificConfig.getBufferGrouperMaxSize(),
            querySpecificConfig.getBufferGrouperMaxLoadFactor(),
            querySpecificConfig.getBufferGrouperInitialBuckets(),
            limitSpec.getLimit(),
            DefaultLimitSpec.sortingOrderHasNonGroupingFields(
                limitSpec,
                query.getDimensions()
            )
        );

        if (limitGrouper.validateBufferCapacity(buffer.capacity())) {
          grouper = limitGrouper;
        } else {
          LOGGER.warn(
              "Limit is not applied in segment scan phase due to limited buffer capacity for query [%s].",
              query.getId()
          );
        }
      }

      if (grouper == null) {
        grouper = new BufferHashGrouper<>(
            Suppliers.ofInstance(buffer),
            keySerde,
            AggregatorAdapters.factorizeBuffered(
                cursor.getColumnSelectorFactory(),
                query.getAggregatorSpecs()
            ),
            querySpecificConfig.getBufferGrouperMaxSize(),
            querySpecificConfig.getBufferGrouperMaxLoadFactor(),
            querySpecificConfig.getBufferGrouperInitialBuckets(),
            true
        );
      }

      return grouper;
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
    protected void putToRow(ByteBuffer key, ResultRow resultRow)
    {
      for (GroupByColumnSelectorPlus selectorPlus : dims) {
        selectorPlus.getColumnSelectorStrategy().processValueFromGroupingKey(
            selectorPlus,
            key,
            resultRow,
            selectorPlus.getKeyBufferPosition()
        );
      }
    }
  }

  private static class ArrayAggregateIterator extends GroupByEngineIterator<Integer>
  {
    private final int cardinality;

    @Nullable
    private final GroupByColumnSelectorPlus dim;

    @Nullable
    private IndexedInts multiValues;
    private int nextValIndex;

    public ArrayAggregateIterator(
        GroupByQuery query,
        GroupByQueryConfig querySpecificConfig,
        Cursor cursor,
        ByteBuffer buffer,
        @Nullable DateTime fudgeTimestamp,
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
          AggregatorAdapters.factorizeBuffered(cursor.getColumnSelectorFactory(), query.getAggregatorSpecs()),
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
        int multiValuesSize = multiValues.size();
        if (multiValuesSize == 0) {
          if (!grouper.aggregate(GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE).isOk()) {
            return;
          }
        } else {
          for (; nextValIndex < multiValuesSize; nextValIndex++) {
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
    protected void putToRow(Integer key, ResultRow resultRow)
    {
      if (dim != null) {
        if (key != GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE) {
          resultRow.set(dim.getResultRowPosition(), ((DimensionSelector) dim.getSelector()).lookupName(key));
        } else {
          resultRow.set(dim.getResultRowPosition(), NullHandling.defaultStringValue());
        }
      }
    }
  }

  public static void convertRowTypesToOutputTypes(
      final List<DimensionSpec> dimensionSpecs,
      final ResultRow resultRow,
      final int resultRowDimensionStart
  )
  {
    for (int i = 0; i < dimensionSpecs.size(); i++) {
      DimensionSpec dimSpec = dimensionSpecs.get(i);
      final int resultRowIndex = resultRowDimensionStart + i;
      final ValueType outputType = dimSpec.getOutputType();

      resultRow.set(
          resultRowIndex,
          DimensionHandlerUtils.convertObjectToType(resultRow.get(resultRowIndex), outputType)
      );
    }
  }

  private static class GroupByEngineKeySerde implements Grouper.KeySerde<ByteBuffer>
  {
    private final int keySize;
    private final GroupByColumnSelectorPlus[] dims;
    private final GroupByQuery query;

    public GroupByEngineKeySerde(final GroupByColumnSelectorPlus[] dims, GroupByQuery query)
    {
      this.dims = dims;
      int keySize = 0;
      for (GroupByColumnSelectorPlus selectorPlus : dims) {
        keySize += selectorPlus.getColumnSelectorStrategy().getGroupingKeySize();
      }
      this.keySize = keySize;

      this.query = query;
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
      Preconditions.checkState(query.isApplyLimitPushDown(), "no limit push down");
      DefaultLimitSpec limitSpec = (DefaultLimitSpec) query.getLimitSpec();

      return GrouperBufferComparatorUtils.bufferComparator(
          query.getResultRowHasTimestamp(),
          query.getContextSortByDimsFirst(),
          query.getDimensions().size(),
          getDimensionComparators(limitSpec)
      );
    }

    @Override
    public Grouper.BufferComparator bufferComparatorWithAggregators(
        AggregatorFactory[] aggregatorFactories,
        int[] aggregatorOffsets
    )
    {
      Preconditions.checkState(query.isApplyLimitPushDown(), "no limit push down");
      DefaultLimitSpec limitSpec = (DefaultLimitSpec) query.getLimitSpec();

      return GrouperBufferComparatorUtils.bufferComparatorWithAggregators(
        query.getAggregatorSpecs().toArray(new AggregatorFactory[0]),
        aggregatorOffsets,
        limitSpec,
        query.getDimensions(),
        getDimensionComparators(limitSpec),
        query.getResultRowHasTimestamp(),
        query.getContextSortByDimsFirst()
      );
    }

    private Grouper.BufferComparator[] getDimensionComparators(DefaultLimitSpec limitSpec)
    {
      Grouper.BufferComparator[] dimComparators = new Grouper.BufferComparator[dims.length];

      for (int i = 0; i < dims.length; i++) {
        final String dimName = query.getDimensions().get(i).getOutputName();
        StringComparator stringComparator = DefaultLimitSpec.getComparatorForDimName(limitSpec, dimName);
        dimComparators[i] = dims[i].getColumnSelectorStrategy().bufferComparator(
            dims[i].getKeyBufferPosition(),
            stringComparator
        );
      }

      return dimComparators;
    }

    @Override
    public void reset()
    {
      // No state, nothing to reset
    }
  }
}
