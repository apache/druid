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
import io.druid.java.util.common.guava.ResourceClosingSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.QueryDimensionInfo;
import io.druid.query.dimension.QueryTypeHelper;
import io.druid.query.dimension.QueryTypeHelperFactory;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionQueryHelper;
import io.druid.segment.DimensionSelector;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
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
  private static final GroupByTypeHelperFactory TYPE_HELPER_FACTORY = new GroupByTypeHelperFactory();

  private static GroupByDimensionInfo[] getGroupByDimInfo(QueryDimensionInfo<GroupByTypeHelper>[] baseDimInfo)
  {
    GroupByDimensionInfo[] retInfo = new GroupByDimensionInfo[baseDimInfo.length];
    int curPos = 0;
    for (int i = 0; i < retInfo.length; i++) {
      retInfo[i] = new GroupByDimensionInfo(baseDimInfo[i], curPos);
      curPos += retInfo[i].getQueryTypeHelper().getGroupingKeySize();
    }
    return retInfo;
  }

  private static class GroupByDimensionInfo extends QueryDimensionInfo<GroupByTypeHelper>
  {
    /**
     * Indicates the offset of this dimension's value within the grouping key.
     */
    private int keyBufferPosition;

    public GroupByDimensionInfo(QueryDimensionInfo<GroupByTypeHelper> baseInfo, int keyBufferPosition)
    {
      super(
          baseInfo.getSpec(),
          baseInfo.getQueryHelper(),
          baseInfo.getQueryTypeHelper(),
          baseInfo.getSelector()
      );
      this.keyBufferPosition = keyBufferPosition;
    }

    public int getKeyBufferPosition()
    {
      return keyBufferPosition;
    }
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
        new ResourceClosingSequence<>(
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
                            QueryDimensionInfo<GroupByTypeHelper>[] dimInfo = DimensionHandlerUtils.getDimensionInfo(
                                TYPE_HELPER_FACTORY,
                                query.getDimensions(),
                                storageAdapter,
                                cursor
                            );
                            return new GroupByEngineIterator(
                                query,
                                config,
                                cursor,
                                bufferHolder.get(),
                                fudgeTimestamp,
                                getGroupByDimInfo(dimInfo)
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

  private static class GroupByTypeHelperFactory implements QueryTypeHelperFactory<GroupByTypeHelper>
  {
    @Override
    public GroupByTypeHelper makeQueryTypeHelper(
        String dimName, ColumnCapabilities capabilities
    )
    {
      ValueType type = capabilities.getType();
      switch(type) {
        case STRING:
          return new StringGroupByTypeHelper();
        default:
          return null;
      }
    }
  }

  /**
   * Contains a collection of query processing methods for type-specific operations used exclusively by
   * GroupByQueryEngineV2.
   *
   * Each GroupByTypeHelper is associated with a single dimension.
   *
   * @param <RowValuesType> The type of the row values object for this dimension
   */
  private interface GroupByTypeHelper<RowValuesType> extends QueryTypeHelper
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
     * Read the first value within a row values object (IndexedInts, IndexedLongs, etc.) and write that value
     * to the keyBuffer at keyBufferPosition. If rowSize is 0, write GROUP_BY_MISSING_VALUE instead.
     *
     * @param valuesObj row values object
     * @param keyBuffer grouping key
     * @param keyBufferPosition offset within grouping key
     */
    void initializeGroupingKeyV2Dimension(
        final RowValuesType valuesObj,
        final ByteBuffer keyBuffer,
        final int keyBufferPosition
    );


    /**
     * Read the value at rowValueIdx from a row values object and write that value to the keyBuffer at keyBufferPosition.
     *
     * @param values row values object
     * @param rowValueIdx index of the value to read
     * @param keyBuffer grouping key
     * @param keyBufferPosition offset within grouping key
     */
    void addValueToGroupingKeyV2(
        RowValuesType values,
        int rowValueIdx,
        ByteBuffer keyBuffer,
        final int keyBufferPosition
    );


    /**
     * Read a value from a grouping key and add it to the group by query result map, using the output name specified
     * in a DimensionSpec.
     *
     * An implementation may choose to not add anything to the result map
     * (e.g., as the String implementation does for empty rows)
     *
     * dimInfo provides access to:
     * - the keyBufferPosition offset from which to read the value
     * - the dimension value selector
     * - the DimensionSpec for this dimension from the query
     *
     * @param dimInfo dimension info containing the key offset, value selector, and dimension spec
     * @param resultMap result map for the group by query being served
     * @param key grouping key
     */
    void processValueFromGroupingKeyV2(
        GroupByDimensionInfo dimInfo,
        ByteBuffer key,
        Map<String, Object> resultMap
    );
  }

  private static class StringGroupByTypeHelper implements GroupByTypeHelper<IndexedInts>
  {
    private static final int GROUP_BY_MISSING_VALUE = -1;

    @Override
    public int getGroupingKeySize()
    {
      return Ints.BYTES;
    }

    @Override
    public void initializeGroupingKeyV2Dimension(
        final IndexedInts values,
        final ByteBuffer keyBuffer,
        final int keyBufferPosition
    )
    {
      int rowSize = values.size();
      if (rowSize == 0) {
        keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
      } else {
        keyBuffer.putInt(keyBufferPosition, values.get(0));
      }
    }

    @Override
    public void addValueToGroupingKeyV2(
        final IndexedInts values,
        final int rowValueIdx,
        final ByteBuffer keyBuffer,
        final int keyBufferPosition
    )
    {
      keyBuffer.putInt(
          keyBufferPosition,
          values.get(rowValueIdx)
      );
    }

    @Override
    public void processValueFromGroupingKeyV2(GroupByDimensionInfo dimInfo, ByteBuffer key, Map<String, Object> resultMap)
    {
      final int id = key.getInt(dimInfo.getKeyBufferPosition());

      // GROUP_BY_MISSING_VALUE is used to indicate empty rows, which are omitted from the result map.
      if (id != GROUP_BY_MISSING_VALUE) {
        resultMap.put(
            dimInfo.getOutputName(),
            ((DimensionSelector) dimInfo.getSelector()).lookupName(id)
        );
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
    private final GroupByDimensionInfo[] dims;

    private int stackp = Integer.MIN_VALUE;
    private boolean currentRowWasPartiallyAggregated = false;
    private CloseableGrouperIterator<ByteBuffer, Row> delegate = null;

    public GroupByEngineIterator(
        final GroupByQuery query,
        final GroupByQueryConfig config,
        final Cursor cursor,
        final ByteBuffer buffer,
        final DateTime fudgeTimestamp,
        final GroupByDimensionInfo[] dims
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
          buffer,
          keySerde,
          cursor,
          query.getAggregatorSpecs()
               .toArray(new AggregatorFactory[query.getAggregatorSpecs().size()]),
          querySpecificConfig.getBufferGrouperMaxSize(),
          querySpecificConfig.getBufferGrouperMaxLoadFactor(),
          querySpecificConfig.getBufferGrouperInitialBuckets()
      );

outer:
      while (!cursor.isDone()) {
        if (!currentRowWasPartiallyAggregated) {
          // Set up stack, valuess, and first grouping in keyBuffer for this row
          stackp = stack.length - 1;

          for (int i = 0; i < dims.length; i++) {
            final DimensionQueryHelper queryHelper = dims[i].getQueryHelper();
            valuess[i] = queryHelper.getRowFromDimSelector(dims[i].getSelector());
            int rowSize = queryHelper.getRowSize(valuess[i]);
            dims[i].getQueryTypeHelper().initializeGroupingKeyV2Dimension(valuess[i], keyBuffer, dims[i].getKeyBufferPosition());
            stack[i] = rowSize == 0 ? 0 : 1;
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

          if (stackp >= 0 && stack[stackp] < dims[stackp].getQueryHelper().getRowSize(valuess[stackp])) {
            // Load next value for current slot
            dims[stackp].getQueryTypeHelper().addValueToGroupingKeyV2(valuess[stackp], stack[stackp], keyBuffer, dims[stackp].getKeyBufferPosition());
            stack[stackp]++;
            for (int i = stackp + 1; i < stack.length; i++) {
              int rowSize = dims[i].getQueryHelper().getRowSize(valuess[i]);
              dims[i].getQueryTypeHelper().initializeGroupingKeyV2Dimension(valuess[i], keyBuffer, dims[i].getKeyBufferPosition());
              stack[i] = rowSize == 0 ? 0 : 1;
            }
            stackp = stack.length - 1;
            doAggregate = true;
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
              for (GroupByDimensionInfo dimInfo : dims) {
                dimInfo.getQueryTypeHelper().processValueFromGroupingKeyV2(
                    dimInfo,
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

    public GroupByEngineKeySerde(final GroupByDimensionInfo dims[])
    {
      int keySize = 0;
      for (GroupByDimensionInfo dimInfo : dims) {
        keySize += dimInfo.getQueryTypeHelper().getGroupingKeySize();
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
    public Grouper.KeyComparator comparator()
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
}
