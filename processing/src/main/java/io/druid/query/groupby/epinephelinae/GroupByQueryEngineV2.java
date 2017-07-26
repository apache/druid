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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
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
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
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
      final GroupByQueryConfig config
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
        .noneMatch(dimension -> {
          final ColumnCapabilities columnCapabilities = storageAdapter.getColumnCapabilities(dimension.getDimension());
          return columnCapabilities == null || columnCapabilities.hasMultipleValues();
        });

    final ResourceHolder<ByteBuffer> bufferHolder = intermediateResultsBufferPool.take();

    final String fudgeTimestampString = Strings.emptyToNull(
        query.getContextValue(GroupByStrategyV2.CTX_KEY_FUDGE_TIMESTAMP, "")
    );

    final DateTime fudgeTimestamp = fudgeTimestampString == null
                                    ? null
                                    : new DateTime(Long.parseLong(fudgeTimestampString));

    return Sequences.concat(
        Sequences.withBaggage(
            Sequences.map(
                cursors,
                new Function<Cursor, Sequence<Row>>()
                {
                  @Override
                  public Sequence<Row> apply(final Cursor cursor)
                  {
                    return new BaseSequence<>(
                        new BaseSequence.IteratorMaker<Row, GroupByEngineIterator>()
                        {
                          @Override
                          public GroupByEngineIterator make()
                          {
                            ColumnSelectorPlus<GroupByColumnSelectorStrategy>[] selectorPlus = DimensionHandlerUtils.createColumnSelectorPluses(
                                STRATEGY_FACTORY,
                                query.getDimensions(),
                                cursor
                            );
                            return new GroupByEngineIterator(
                                query,
                                config,
                                cursor,
                                bufferHolder.get(),
                                fudgeTimestamp,
                                createGroupBySelectorPlus(selectorPlus),
                                storageAdapter::getDimensionCardinality,
                                allSingleValueDims
                            );
                          }

                          @Override
                          public void cleanup(GroupByEngineIterator iterFromMake)
                          {
                            iterFromMake.close();
                          }
                        }
                    );
                  }
                }
            ),
            new Closeable()
            {
              @Override
              public void close() throws IOException
              {
                CloseQuietly.close(bufferHolder);
              }
            }
        )
    );
  }

  private static class GroupByStrategyFactory implements ColumnSelectorStrategyFactory<GroupByColumnSelectorStrategy>
  {
    @Override
    public GroupByColumnSelectorStrategy makeColumnSelectorStrategy(
        ColumnCapabilities capabilities, ColumnValueSelector selector
    )
    {
      ValueType type = capabilities.getType();
      switch(type) {
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

  private static class GroupByEngineIterator implements Iterator<Row>, Closeable
  {
    private final GroupByQuery query;
    private final GroupByQueryConfig querySpecificConfig;
    private final Cursor cursor;
    private final ByteBuffer buffer;
    private final Grouper.KeySerde<ByteBuffer> keySerde;
    private final DateTime timestamp;
    private final ByteBuffer keyBuffer;
    private final int[] stack;
    private final Object[] valuess;
    private final GroupByColumnSelectorPlus[] dims;

    private int stackp = Integer.MIN_VALUE;
    private boolean currentRowWasPartiallyAggregated = false;
    private CloseableGrouperIterator<ByteBuffer, Row> delegate = null;
    private final Function<String, Integer> cardinalityFunction; // dimension name -> cardinality
    private final boolean allSingleValueDims;

    public GroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig config,
        final Cursor cursor,
        final ByteBuffer buffer,
        final DateTime fudgeTimestamp,
        final GroupByColumnSelectorPlus[] dims,
        final Function<String, Integer> cardinalityFunction,
        final boolean allSingleValueDims
    )
    {
      final int dimCount = query.getDimensions().size();

      this.query = query;
      this.querySpecificConfig = config.withOverrides(query);
      this.cursor = cursor;
      this.buffer = buffer;
      this.keySerde = new GroupByEngineKeySerde(dims);
      this.keyBuffer = ByteBuffer.allocate(keySerde.keySize());
      this.dims = dims;
      this.stack = new int[dimCount];
      this.valuess = new Object[dimCount];

      // Time is the same for every row in the cursor
      this.timestamp = fudgeTimestamp != null ? fudgeTimestamp : cursor.getTime();
      this.cardinalityFunction = cardinalityFunction;
      this.allSingleValueDims = allSingleValueDims;
    }

    private CloseableGrouperIterator<ByteBuffer, Row> initNewDelegate()
    {
      final Grouper<ByteBuffer> grouper = newGrouper();
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
            for (GroupByColumnSelectorPlus selectorPlus : dims) {
              selectorPlus.getColumnSelectorStrategy().processValueFromGroupingKey(
                  selectorPlus,
                  entry.getKey(),
                  theMap
              );
            }

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

    private Grouper<ByteBuffer> newGrouper()
    {
      final AggregatorFactory[] aggregatorFactories = query
          .getAggregatorSpecs()
          .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]);

      if (!querySpecificConfig.isForceHashAggregation()) {
        if (dims.length == 1) {
          final ColumnCapabilities columnCapabilities = cursor.getColumnCapabilities(dims[0].getName());
          final int cardinality = computeCardinality(cardinalityFunction, dims[0]);

          // Choose array-based aggregation if the grouping key is a single string dimension of a known cardinality
          if ((columnCapabilities == null || columnCapabilities.getType().equals(ValueType.STRING)) &&
              cardinality > 0) {
            final int requiredBufferCapacity = BufferArrayGrouper.requiredBufferCapacity(keySerde, cardinality, aggregatorFactories);

            // Check that all keys and aggregated values can be contained the buffer
            if (requiredBufferCapacity <= buffer.capacity()) {
              return new BufferArrayGrouper<>(
                  Suppliers.ofInstance(buffer),
                  keySerde,
                  cursor,
                  aggregatorFactories,
                  cardinality
              );
            }
          }
        }
      }

      return new BufferHashGrouper<>(
          Suppliers.ofInstance(buffer),
          keySerde,
          cursor,
          aggregatorFactories,
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets()
      );
    }

    @SuppressWarnings("ConstantConditions")
    private static int computeCardinality(Function<String, Integer> cardinalityFunction, GroupByColumnSelectorPlus dim)
    {
      return cardinalityFunction.apply(dim.getName());
    }

    private void aggregateSingleValueDims(Grouper<ByteBuffer> grouper)
    {
      while (!cursor.isDone()) {
        for (int i = 0; i < dims.length; i++) {
          final GroupByColumnSelectorStrategy strategy = dims[i].getColumnSelectorStrategy();
          strategy.writeToKeyBuffer(
              dims[i].getKeyBufferPosition(),
              strategy.getOnlyValue(dims[i].getSelector()),
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

    private void aggregateMultiValueDims(Grouper<ByteBuffer> grouper)
    {
      while (!cursor.isDone()) {
        if (!currentRowWasPartiallyAggregated) {
          // Set up stack, valuess, and first grouping in keyBuffer for this row
          stackp = stack.length - 1;

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
        while (stackp >= -1) {
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

          if (stackp >= 0) {
            doAggregate = dims[stackp].getColumnSelectorStrategy().checkRowIndexAndAddValueToGroupingKey(
                dims[stackp].getKeyBufferPosition(),
                valuess[stackp],
                stack[stackp],
                keyBuffer
            );

            if (doAggregate) {
              stack[stackp]++;
              for (int i = stackp + 1; i < stack.length; i++) {
                dims[i].getColumnSelectorStrategy().initGroupingKeyColumnValue(
                    dims[i].getKeyBufferPosition(),
                    i,
                    valuess[i],
                    keyBuffer,
                    stack
                );
              }
              stackp = stack.length - 1;
            } else {
              stackp--;
            }
          } else {
            stackp--;
          }
        }

        // Advance to next row
        cursor.advance();
        currentRowWasPartiallyAggregated = false;
      }
    }
  }

  private static void convertRowTypesToOutputTypes(List<DimensionSpec> dimensionSpecs, Map<String, Object> rowMap)
  {
    for (DimensionSpec dimSpec : dimensionSpecs) {
      final ValueType outputType = dimSpec.getOutputType();
      rowMap.compute(
          dimSpec.getOutputName(),
          (dimName, baseVal) -> {
            switch (outputType) {
              case STRING:
                baseVal = baseVal == null ? "" : baseVal.toString();
                break;
              case LONG:
                baseVal = DimensionHandlerUtils.convertObjectToLong(baseVal);
                baseVal = baseVal == null ? 0L : baseVal;
                break;
              case FLOAT:
                baseVal = DimensionHandlerUtils.convertObjectToFloat(baseVal);
                baseVal = baseVal == null ? 0.f : baseVal;
                break;
              default:
                throw new IAE("Unsupported type: " + outputType);
            }
            return baseVal;
          }
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
