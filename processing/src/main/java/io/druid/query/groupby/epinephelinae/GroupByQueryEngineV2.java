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
import com.google.common.primitives.Ints;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
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
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.VirtualColumns;
import io.druid.segment.data.IndexedInts;
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
      final StupidPool<ByteBuffer> intermediateResultsBufferPool,
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
         VirtualColumns.EMPTY,
        query.getGranularity(),
        false
    );


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
                                createGroupBySelectorPlus(selectorPlus)
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
        ColumnCapabilities capabilities
    )
    {
      ValueType type = capabilities.getType();
      switch(type) {
        case STRING:
          return new StringGroupByColumnSelectorStrategy();
        default:
          throw new IAE("Cannot create query type helper from invalid type [%s]", type);
      }
    }
  }

  /**
   * Contains a collection of query processing methods for type-specific operations used exclusively by
   * GroupByQueryEngineV2.
   *
   * Each GroupByColumnSelectorStrategy is associated with a single dimension.
   */
  private interface GroupByColumnSelectorStrategy extends ColumnSelectorStrategy
  {
    /**
     * Return the size, in bytes, of this dimension's values in the grouping key.
     *
     * For example, a String implementation would return 4, the size of an int.
     *
     * @return size, in bytes, of this dimension's values in the grouping key.
     */
    int getGroupingKeySize();

    /**
     * Read a value from a grouping key and add it to the group by query result map, using the output name specified
     * in a DimensionSpec.
     *
     * An implementation may choose to not add anything to the result map
     * (e.g., as the String implementation does for empty rows)
     *
     * selectorPlus provides access to:
     * - the keyBufferPosition offset from which to read the value
     * - the dimension value selector
     * - the DimensionSpec for this dimension from the query
     *
     * @param selectorPlus dimension info containing the key offset, value selector, and dimension spec
     * @param resultMap result map for the group by query being served
     * @param key grouping key
     */
    void processValueFromGroupingKey(
        GroupByColumnSelectorPlus selectorPlus,
        ByteBuffer key,
        Map<String, Object> resultMap
    );

    /**
     * Retrieve a row object from the ColumnSelectorPlus and put it in valuess at columnIndex.
     *
     * @param selector Value selector for a column.
     * @param columnIndex Index of the column within the row values array
     * @param valuess Row values array, one index per column
     */
    void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess);

    /**
     * Read the first value within a row values object (IndexedInts, IndexedLongs, etc.) and write that value
     * to the keyBuffer at keyBufferPosition. If rowSize is 0, write GROUP_BY_MISSING_VALUE instead.
     *
     * If the size of the row is > 0, write 1 to stack[] at columnIndex, otherwise write 0.
     *
     * @param keyBufferPosition Starting offset for this column's value within the grouping key.
     * @param columnIndex Index of the column within the row values array
     * @param rowObj Row value object for this column (e.g., IndexedInts)
     * @param keyBuffer grouping key
     * @param stack array containing the current within-row value index for each column
     */
    void initGroupingKeyColumnValue(int keyBufferPosition, int columnIndex, Object rowObj, ByteBuffer keyBuffer, int[] stack);

    /**
     * If rowValIdx is less than the size of rowObj (haven't handled all of the row values):
     * First, read the value at rowValIdx from a rowObj and write that value to the keyBuffer at keyBufferPosition.
     * Then return true
     *
     * Otherwise, return false.
     *
     * @param keyBufferPosition Starting offset for this column's value within the grouping key.
     * @param rowObj Row value object for this column (e.g., IndexedInts)
     * @param rowValIdx Index of the current value being grouped on within the row
     * @param keyBuffer grouping key
     * @return true if rowValIdx < size of rowObj, false otherwise
     */
    boolean checkRowIndexAndAddValueToGroupingKey(int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer);
  }

  private static class StringGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
  {
    private static final int GROUP_BY_MISSING_VALUE = -1;

    @Override
    public int getGroupingKeySize()
    {
      return Ints.BYTES;
    }

    @Override
    public void processValueFromGroupingKey(GroupByColumnSelectorPlus selectorPlus, ByteBuffer key, Map<String, Object> resultMap)
    {
      final int id = key.getInt(selectorPlus.getKeyBufferPosition());

      // GROUP_BY_MISSING_VALUE is used to indicate empty rows, which are omitted from the result map.
      if (id != GROUP_BY_MISSING_VALUE) {
        resultMap.put(
            selectorPlus.getOutputName(),
            ((DimensionSelector) selectorPlus.getSelector()).lookupName(id)
        );
      } else {
        resultMap.put(selectorPlus.getOutputName(), "");
      }
    }

    @Override
    public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
    {
      DimensionSelector dimSelector = (DimensionSelector) selector;
      IndexedInts row = dimSelector.getRow();
      valuess[columnIndex] = row;
    }

    @Override
    public void initGroupingKeyColumnValue(int keyBufferPosition, int columnIndex, Object rowObj, ByteBuffer keyBuffer, int[] stack)
    {
      IndexedInts row = (IndexedInts) rowObj;
      int rowSize = row.size();

      initializeGroupingKeyV2Dimension(row, rowSize, keyBuffer, keyBufferPosition);
      stack[columnIndex] = rowSize == 0 ? 0 : 1;
    }

    @Override
    public boolean checkRowIndexAndAddValueToGroupingKey(int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer)
    {
      IndexedInts row = (IndexedInts) rowObj;
      int rowSize = row.size();

      if (rowValIdx < rowSize) {
        keyBuffer.putInt(
            keyBufferPosition,
            row.get(rowValIdx)
        );
        return true;
      } else {
        return false;
      }
    }

    private void initializeGroupingKeyV2Dimension(
        final IndexedInts values,
        final int rowSize,
        final ByteBuffer keyBuffer,
        final int keyBufferPosition
    )
    {
      if (rowSize == 0) {
        keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
      } else {
        keyBuffer.putInt(keyBufferPosition, values.get(0));
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

    public GroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig config,
        final Cursor cursor,
        final ByteBuffer buffer,
        final DateTime fudgeTimestamp,
        final GroupByColumnSelectorPlus[] dims
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
    }

    @Override
    public Row next()
    {
      if (delegate != null && delegate.hasNext()) {
        return delegate.next();
      }

      if (cursor.isDone()) {
        throw new NoSuchElementException();
      }

      // Make a new delegate iterator
      if (delegate != null) {
        delegate.close();
        delegate = null;
      }

      final Grouper<ByteBuffer> grouper = new BufferGrouper<>(
          Suppliers.ofInstance(buffer),
          keySerde,
          cursor,
          query.getAggregatorSpecs()
               .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]),
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets()
      );
      grouper.init();

outer:
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
            if (!grouper.aggregate(keyBuffer)) {
              // Buffer full while aggregating; break out and resume later
              currentRowWasPartiallyAggregated = true;
              break outer;
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

      delegate = new CloseableGrouperIterator<>(
          grouper,
          false,
          new Function<Grouper.Entry<ByteBuffer>, Row>()
          {
            @Override
            public Row apply(final Grouper.Entry<ByteBuffer> entry)
            {
              Map<String, Object> theMap = Maps.newLinkedHashMap();

              // Add dimensions.
              for (GroupByColumnSelectorPlus selectorPlus : dims) {
                selectorPlus.getColumnSelectorStrategy().processValueFromGroupingKey(
                    selectorPlus,
                    entry.getKey(),
                    theMap
                );
              }

              // Add aggregations.
              for (int i = 0; i < entry.getValues().length; i++) {
                theMap.put(query.getAggregatorSpecs().get(i).getName(), entry.getValues()[i]);
              }

              return new MapBasedRow(timestamp, theMap);
            }
          },
          new Closeable()
          {
            @Override
            public void close() throws IOException
            {
              grouper.close();
            }
          }
      );

      return delegate.next();
    }

    @Override
    public boolean hasNext()
    {
      return (delegate != null && delegate.hasNext()) || !cursor.isDone();
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
    public Grouper.KeyComparator bufferComparator()
    {
      // No sorting, let mergeRunners handle that
      throw new UnsupportedOperationException();
    }

    @Override
    public void reset()
    {
      // No state, nothing to reset
    }
  }

  private static class GroupByColumnSelectorPlus extends ColumnSelectorPlus<GroupByColumnSelectorStrategy>
  {
    /**
     * Indicates the offset of this dimension's value within the grouping key.
     */
    private int keyBufferPosition;

    public GroupByColumnSelectorPlus(ColumnSelectorPlus<GroupByColumnSelectorStrategy> baseInfo, int keyBufferPosition)
    {
      super(
          baseInfo.getName(),
          baseInfo.getOutputName(),
          baseInfo.getColumnSelectorStrategy(),
          baseInfo.getSelector()
      );
      this.keyBufferPosition = keyBufferPosition;
    }

    public int getKeyBufferPosition()
    {
      return keyBufferPosition;
    }
  }
}
