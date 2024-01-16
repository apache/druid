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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.ColumnSelectorStrategyFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryMetrics;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.column.ArrayDoubleGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.ArrayLongGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.ArrayStringGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.DictionaryBuildingStringGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.DoubleGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.FloatGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorPlus;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.LongGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.NullableNumericGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.epinephelinae.column.StringGroupByColumnSelectorStrategy;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Contains logic to process a groupBy query on a single {@link StorageAdapter} in a non-vectorized manner.
 * Processing returns a {@link Sequence} of {@link ResultRow} objects that are not guaranteed to be in any particular
 * order, and may not even be fully grouped. It is expected that a downstream {@link GroupByMergingQueryRunner} will
 * finish grouping these results.
 * <p>
 * This code runs on anything that processes {@link StorageAdapter} directly, typically data servers like Historicals.
 * <p>
 * Used for non-vectorized processing by
 * {@link GroupingEngine#process(GroupByQuery, StorageAdapter, GroupByQueryMetrics)}.
 *
 * @see org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine for vectorized version of this class
 */
public class GroupByQueryEngine
{
  private static final GroupByStrategyFactory STRATEGY_FACTORY = new GroupByStrategyFactory();

  private GroupByQueryEngine()
  {
    // No instantiation
  }

  public static Sequence<ResultRow> process(
      final GroupByQuery query,
      final StorageAdapter storageAdapter,
      final ByteBuffer processingBuffer,
      @Nullable final DateTime fudgeTimestamp,
      final GroupByQueryConfig querySpecificConfig,
      final DruidProcessingConfig processingConfig,
      @Nullable final Filter filter,
      final Interval interval,
      @Nullable final GroupByQueryMetrics groupByQueryMetrics
  )
  {
    final Sequence<Cursor> cursors = storageAdapter.makeCursors(
        filter,
        interval,
        query.getVirtualColumns(),
        query.getGranularity(),
        false,
        groupByQueryMetrics
    );

    return cursors.flatMap(
        cursor -> new BaseSequence<>(
            new BaseSequence.IteratorMaker<ResultRow, GroupByEngineIterator<?>>()
            {
              @Override
              public GroupByEngineIterator<?> make()
              {
                final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
                final ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] selectorPlus = DimensionHandlerUtils
                    .createColumnSelectorPluses(
                        STRATEGY_FACTORY,
                        query.getDimensions(),
                        columnSelectorFactory
                    );
                GroupByColumnSelectorPlus[] dims = new GroupByColumnSelectorPlus[selectorPlus.length];
                int curPos = 0;
                for (int i = 0; i < dims.length; i++) {
                  dims[i] = new GroupByColumnSelectorPlus(
                      selectorPlus[i],
                      curPos,
                      query.getResultRowDimensionStart() + i
                  );
                  curPos += dims[i].getColumnSelectorStrategy().getGroupingKeySize();
                }

                final int cardinalityForArrayAggregation = GroupingEngine.getCardinalityForArrayAggregation(
                    querySpecificConfig,
                    query,
                    storageAdapter,
                    processingBuffer
                );

                if (cardinalityForArrayAggregation >= 0) {
                  return new ArrayAggregateIterator(
                      query,
                      querySpecificConfig,
                      processingConfig,
                      cursor,
                      processingBuffer,
                      fudgeTimestamp,
                      dims,
                      hasNoImplicitUnnestDimensions(columnSelectorFactory, query.getDimensions()),
                      cardinalityForArrayAggregation
                  );
                } else {
                  return new HashAggregateIterator(
                      query,
                      querySpecificConfig,
                      processingConfig,
                      cursor,
                      processingBuffer,
                      fudgeTimestamp,
                      dims,
                      hasNoImplicitUnnestDimensions(columnSelectorFactory, query.getDimensions())
                  );
                }
              }

              @Override
              public void cleanup(GroupByEngineIterator<?> iterFromMake)
              {
                iterFromMake.close();
              }
            }
        )
    );
  }

  /**
   * check if a column will operate correctly with {@link LimitedBufferHashGrouper} for query limit pushdown
   */
  @VisibleForTesting
  public static boolean canPushDownLimit(ColumnSelectorFactory columnSelectorFactory, String columnName)
  {
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(columnName);
    if (capabilities != null) {
      // strings can be pushed down if dictionaries are sorted and unique per id
      if (capabilities.is(ValueType.STRING)) {
        return capabilities.areDictionaryValuesSorted().and(capabilities.areDictionaryValuesUnique()).isTrue();
      }
      // party on
      return true;
    }
    // we don't know what we don't know, don't assume otherwise
    return false;
  }

  /**
   * Checks whether all "dimensions" are either single-valued, or if the input column or output dimension spec has
   * specified a type that {@link ColumnType#isArray()}. Both cases indicate we don't want to unnest the under-lying
   * multi value column. Since selectors on non-existent columns will show up as full of nulls, they are effectively
   * single valued, however capabilites on columns can also be null, for example during broker merge with an 'inline'
   * datasource subquery, so we only return true from this method when capabilities are fully known.
   */
  private static boolean hasNoImplicitUnnestDimensions(
      final ColumnInspector inspector,
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

              // Now check column capabilities, which must be present and explicitly not multi-valued and not arrays
              final ColumnCapabilities columnCapabilities = inspector.getColumnCapabilities(dimension.getDimension());
              return dimension.getOutputType().isArray()
                     || (columnCapabilities != null
                         && columnCapabilities.hasMultipleValues().isFalse()
                         && !columnCapabilities.isArray()
                     );
            });
  }

  private static class GroupByStrategyFactory implements ColumnSelectorStrategyFactory<GroupByColumnSelectorStrategy>
  {
    @Override
    public GroupByColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities,
        ColumnValueSelector selector
    )
    {
      switch (capabilities.getType()) {
        case STRING:
          DimensionSelector dimSelector = (DimensionSelector) selector;
          if (dimSelector.getValueCardinality() >= 0) {
            return new StringGroupByColumnSelectorStrategy(dimSelector::lookupName, capabilities);
          } else {
            return new DictionaryBuildingStringGroupByColumnSelectorStrategy();
          }
        case LONG:
          return makeNullableNumericStrategy(new LongGroupByColumnSelectorStrategy());
        case FLOAT:
          return makeNullableNumericStrategy(new FloatGroupByColumnSelectorStrategy());
        case DOUBLE:
          return makeNullableNumericStrategy(new DoubleGroupByColumnSelectorStrategy());
        case ARRAY:
          switch (capabilities.getElementType().getType()) {
            case LONG:
              return new ArrayLongGroupByColumnSelectorStrategy();
            case STRING:
              return new ArrayStringGroupByColumnSelectorStrategy();
            case DOUBLE:
              return new ArrayDoubleGroupByColumnSelectorStrategy();
            case FLOAT:
              // Array<Float> not supported in expressions, ingestion
            default:
              throw new IAE("Cannot create query type helper from invalid type [%s]", capabilities.asTypeString());

          }
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", capabilities.asTypeString());
      }
    }

    private GroupByColumnSelectorStrategy makeNullableNumericStrategy(GroupByColumnSelectorStrategy delegate)
    {
      if (NullHandling.sqlCompatible()) {
        return new NullableNumericGroupByColumnSelectorStrategy(delegate);
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
    protected final boolean allowMultiValueGrouping;
    protected final long maxSelectorFootprint;

    public GroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig querySpecificConfig,
        final DruidProcessingConfig processingConfig,
        final Cursor cursor,
        final ByteBuffer buffer,
        @Nullable final DateTime fudgeTimestamp,
        final GroupByColumnSelectorPlus[] dims,
        final boolean allSingleValueDims
    )
    {
      this.query = query;
      this.querySpecificConfig = querySpecificConfig;
      this.maxSelectorFootprint = querySpecificConfig.getActualMaxSelectorDictionarySize(processingConfig);
      this.cursor = cursor;
      this.buffer = buffer;
      this.keySerde = new GroupByEngineKeySerde(dims, query);
      this.dims = dims;

      // Time is the same for every row in the cursor
      this.timestamp = fudgeTimestamp != null ? fudgeTimestamp : cursor.getTime();
      this.allSingleValueDims = allSingleValueDims;
      this.allowMultiValueGrouping = query.context().getBoolean(
          GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING,
          true
      );
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
            GroupingEngine.convertRowTypesToOutputTypes(query.getDimensions(), resultRow, resultRowDimensionStart);

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

    /**
     * Throws {@link UnexpectedMultiValueDimensionException} if "allowMultiValueGrouping" is false.
     */
    protected void checkIfMultiValueGroupingIsAllowed(String dimName)
    {
      if (!allowMultiValueGrouping) {
        throw new UnexpectedMultiValueDimensionException(dimName);
      }
    }
  }

  private static class HashAggregateIterator extends GroupByEngineIterator<ByteBuffer>
  {
    private static final Logger LOGGER = new Logger(HashAggregateIterator.class);

    private final int[] stack;
    private final Object[] valuess;
    protected final ByteBuffer keyBuffer;

    private int stackPointer = Integer.MIN_VALUE;
    private boolean currentRowWasPartiallyAggregated = false;

    // Sum of internal state footprint across all "dims".
    private long selectorInternalFootprint = 0;

    private HashAggregateIterator(
        GroupByQuery query,
        GroupByQueryConfig querySpecificConfig,
        DruidProcessingConfig processingConfig,
        Cursor cursor,
        ByteBuffer buffer,
        @Nullable DateTime fudgeTimestamp,
        GroupByColumnSelectorPlus[] dims,
        boolean allSingleValueDims
    )
    {
      super(query, querySpecificConfig, processingConfig, cursor, buffer, fudgeTimestamp, dims, allSingleValueDims);

      final int dimCount = query.getDimensions().size();
      stack = new int[dimCount];
      valuess = new Object[dimCount];
      keyBuffer = ByteBuffer.allocate(keySerde.keySize());
    }

    @Override
    protected Grouper<ByteBuffer> newGrouper()
    {
      Grouper<ByteBuffer> grouper = null;
      final ColumnSelectorFactory selectorFactory = cursor.getColumnSelectorFactory();
      final DefaultLimitSpec limitSpec = query.isApplyLimitPushDown() &&
                                         querySpecificConfig.isApplyLimitPushDownToSegment() ?
                                         (DefaultLimitSpec) query.getLimitSpec() : null;

      final boolean canDoLimitPushdown;
      if (limitSpec != null) {
        // there is perhaps a more graceful way this could be handled a bit more selectively, but for now just avoid
        // pushdown if it will prove problematic by checking grouping and ordering columns

        canDoLimitPushdown = Stream.concat(
            query.getDimensions().stream().map(DimensionSpec::getDimension),
            limitSpec.getColumns().stream().map(OrderByColumnSpec::getDimension)
        ).allMatch(col -> canPushDownLimit(selectorFactory, col));
      } else {
        canDoLimitPushdown = false;
      }

      if (canDoLimitPushdown) {
        // Sanity check; must not have "offset" at this point.
        Preconditions.checkState(!limitSpec.isOffset(), "Cannot push down offsets");

        LimitedBufferHashGrouper<ByteBuffer> limitGrouper = new LimitedBufferHashGrouper<>(
            Suppliers.ofInstance(buffer),
            keySerde,
            AggregatorAdapters.factorizeBuffered(
                selectorFactory,
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
                selectorFactory,
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
      if (!currentRowWasPartiallyAggregated) {
        for (GroupByColumnSelectorPlus dim : dims) {
          dim.getColumnSelectorStrategy().reset();
        }
        selectorInternalFootprint = 0;
      }

      while (!cursor.isDone()) {
        for (GroupByColumnSelectorPlus dim : dims) {
          final GroupByColumnSelectorStrategy strategy = dim.getColumnSelectorStrategy();
          selectorInternalFootprint += strategy.writeToKeyBuffer(
              dim.getKeyBufferPosition(),
              dim.getSelector(),
              keyBuffer
          );
        }
        keyBuffer.rewind();

        if (!grouper.aggregate(keyBuffer).isOk()) {
          return;
        }

        cursor.advance();

        // Check selectorInternalFootprint after advancing the cursor. (We reset after the first row that causes
        // us to go past the limit.)
        if (selectorInternalFootprint > maxSelectorFootprint) {
          return;
        }
      }
    }

    @Override
    protected void aggregateMultiValueDims(Grouper<ByteBuffer> grouper)
    {
      if (!currentRowWasPartiallyAggregated) {
        for (GroupByColumnSelectorPlus dim : dims) {
          dim.getColumnSelectorStrategy().reset();
        }
        selectorInternalFootprint = 0;
      }

      while (!cursor.isDone()) {
        if (!currentRowWasPartiallyAggregated) {
          // Set up stack, valuess, and first grouping in keyBuffer for this row
          stackPointer = stack.length - 1;

          for (int i = 0; i < dims.length; i++) {
            GroupByColumnSelectorStrategy strategy = dims[i].getColumnSelectorStrategy();
            selectorInternalFootprint += strategy.initColumnValues(
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
              // this check is done during the row aggregation as a dimension can become multi-value col if column
              // capabilities is unknown.
              checkIfMultiValueGroupingIsAllowed(dims[stackPointer].getName());
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

        // Check selectorInternalFootprint after advancing the cursor. (We reset after the first row that causes
        // us to go past the limit.)
        if (selectorInternalFootprint > maxSelectorFootprint) {
          return;
        }
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

  private static class ArrayAggregateIterator extends GroupByEngineIterator<IntKey>
  {
    private final int cardinality;

    @Nullable
    private final GroupByColumnSelectorPlus dim;

    @Nullable
    private IndexedInts multiValues;
    private int nextValIndex;

    private ArrayAggregateIterator(
        GroupByQuery query,
        GroupByQueryConfig querySpecificConfig,
        DruidProcessingConfig processingConfig,
        Cursor cursor,
        ByteBuffer buffer,
        @Nullable DateTime fudgeTimestamp,
        GroupByColumnSelectorPlus[] dims,
        boolean allSingleValueDims,
        int cardinality
    )
    {
      super(query, querySpecificConfig, processingConfig, cursor, buffer, fudgeTimestamp, dims, allSingleValueDims);
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
    protected void aggregateSingleValueDims(Grouper<IntKey> grouper)
    {
      aggregateSingleValueDims((IntGrouper) grouper);
    }

    @Override
    protected void aggregateMultiValueDims(Grouper<IntKey> grouper)
    {
      aggregateMultiValueDims((IntGrouper) grouper);
    }

    private void aggregateSingleValueDims(IntGrouper grouper)
    {
      // No need to track strategy internal state footprint, because array-based grouping does not use strategies.
      // It accesses dimension selectors directly and only works on truly dictionary-coded columns.

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
      // No need to track strategy internal state footprint, because array-based grouping does not use strategies.
      // It accesses dimension selectors directly and only works on truly dictionary-coded columns.

      if (dim == null) {
        throw new ISE("dim must exist");
      }

      if (multiValues == null) {
        // dim is always an indexed string dimension
        multiValues = ((DimensionSelector) dim.getSelector()).getRow();
        nextValIndex = 0;
      }

      while (!cursor.isDone()) {
        final int multiValuesSize = multiValues.size();
        if (multiValuesSize == 0) {
          if (!grouper.aggregate(GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE).isOk()) {
            return;
          }
        } else {
          if (multiValuesSize > 1) {
            // this check is done during the row aggregation as a dimension can become multi-value col if column
            // capabilities is unknown.
            checkIfMultiValueGroupingIsAllowed(dim.getName());
          }
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
    protected void putToRow(IntKey key, ResultRow resultRow)
    {
      final int intKey = key.intValue();
      if (dim != null) {
        if (intKey != GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE) {
          resultRow.set(dim.getResultRowPosition(), ((DimensionSelector) dim.getSelector()).lookupName(intKey));
        } else {
          resultRow.set(dim.getResultRowPosition(), NullHandling.defaultStringValue());
        }
      }
    }
  }

  private static class GroupByEngineKeySerde implements Grouper.KeySerde<ByteBuffer>
  {
    private final int keySize;
    private final GroupByColumnSelectorPlus[] dims;
    private final GroupByQuery query;

    private GroupByEngineKeySerde(final GroupByColumnSelectorPlus[] dims, GroupByQuery query)
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
    public ByteBuffer createKey()
    {
      return ByteBuffer.allocate(keySize);
    }

    @Override
    public ByteBuffer toByteBuffer(ByteBuffer key)
    {
      return key;
    }

    @Override
    public void readFromByteBuffer(ByteBuffer dstBuffer, ByteBuffer srcBuffer, int position)
    {
      dstBuffer.limit(keySize);
      dstBuffer.position(0);

      for (int i = 0; i < keySize; i++) {
        dstBuffer.put(i, srcBuffer.get(position + i));
      }
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
          query.getContextSortByDimsFirst(),
          keySize
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
