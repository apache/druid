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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionIndexer;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import oak.OakMapOffHeapImpl;
import oak.WritableOakBufferImpl;
import oak.OakMap;
import oak.CloseableIterator;

import javax.annotation.Nullable;


/**
 */
public class OffheapOakIncrementalIndex extends
    io.druid.segment.incremental.InternalDataIncrementalIndex<BufferAggregator>
{

  private static final Logger log = new Logger(OffheapOakIncrementalIndex.class);

  // When serializing an object from TimeAndDims.dims, we use:
  // 1. 4 bytes for representing its type (Double, Float, Long or String)
  // 2. 8 bytes for saving its value or the array position and length (in the case of String)
  static final Integer ALLOC_PER_DIM = 12;
  static final Integer NO_DIM = -1;
  static final Integer TIME_STAMP_INDEX = 0;
  static final Integer DIMS_LENGTH_INDEX = TIME_STAMP_INDEX + Long.BYTES;
  static final Integer DIMS_INDEX = DIMS_LENGTH_INDEX + Integer.BYTES;

  OakMapOffHeapImpl oak;
  private volatile Map<String, ColumnSelectorFactory> selectors;
  private volatile int[] aggOffsetInBuffer;
  private volatile int aggsTotalSize;
  private final int maxRowCount;
  private String outOfRowsReason = null;

  OffheapOakIncrementalIndex(
      io.druid.segment.incremental.IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics, boolean reportParseExceptions,
      boolean concurrentEventAdd,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions,
        concurrentEventAdd);
    oak = new OakMapOffHeapImpl(new TimeAndDimsByteBuffersComp(dimensionDescsList), getMinTimeAndDimsByteBuffer());
    this.maxRowCount = maxRowCount;
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(List<PostAggregator> postAggs, boolean descending)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        OakMap oakMap = descending ? oak.descendingMap() : oak;
        Function<Map.Entry<ByteBuffer, ByteBuffer>, Row> transformer = new Transformer(postAggs);
        return oakMap.entriesTransformIterator(transformer);
      }
    };
  }

  // for oak's transform iterator
  private class Transformer implements Function<Map.Entry<ByteBuffer, ByteBuffer>, Row>
  {
    List<PostAggregator> postAggs;
    final List<DimensionDesc> dimensions;

    public Transformer(List<PostAggregator> postAggs)
    {
      this.postAggs = postAggs;
      this.dimensions = getDimensions();
    }

    @Nullable
    @Override
    public Row apply(@Nullable Map.Entry<ByteBuffer, ByteBuffer> entry)
    {
      TimeAndDims key = timeAndDimsDeserialization(entry.getKey());
      ByteBuffer value = entry.getValue();
      Object[] dims = key.getDims();

      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < dims.length; ++i) {
        Object dim = dims[i];
        DimensionDesc dimensionDesc = dimensions.get(i);
        if (dimensionDesc == null) {
          continue;
        }
        String dimensionName = dimensionDesc.getName();
        DimensionHandler handler = dimensionDesc.getHandler();
        if (dim == null || handler.getLengthOfEncodedKeyComponent(dim) == 0) {
          theVals.put(dimensionName, null);
          continue;
        }
        final DimensionIndexer indexer = dimensionDesc.getIndexer();
        Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualArrayOrList(dim, DimensionIndexer.LIST);
        theVals.put(dimensionName, rowVals);
      }

      BufferAggregator[] aggs = getAggs();
      for (int i = 0; i < aggs.length; ++i) {
        theVals.put(metrics[i].getName(), aggs[i].get(value, aggOffsetInBuffer[i]));
      }

      if (postAggs != null) {
        for (PostAggregator postAgg : postAggs) {
          theVals.put(postAgg.getName(), postAgg.compute(theVals));
        }
      }

      return new MapBasedRow(key.getTimestamp(), theVals);
    }
  };

  @Override
  protected long getMinTimeMillis()
  {
    ByteBuffer minKey = oak.getMinKey();
    return getTimestamp(minKey);
  }

  @Override
  protected long getMaxTimeMillis()
  {
    ByteBuffer maxKey = oak.getMaxKey();
    return getTimestamp(maxKey);
  }

  @Override
  public int getLastRowIndex()
  {
    return 0; // Oak doesn't use the row indexes
  }

  @Override
  protected BufferAggregator[] getAggsForRow(TimeAndDims timeAndDims)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, TimeAndDims timeAndDims, int aggPosition)
  {
    ByteBuffer serializedKey = timeAndDimsSerialization(timeAndDims);
    WritableOakBufferImpl oakValue = (WritableOakBufferImpl) oak.get(serializedKey);
    ByteBuffer aggBuffer = oakValue.getByteBuffer();
    return agg.get(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[aggPosition]);
  }

  @Override
  protected float getMetricFloatValue(TimeAndDims timeAndDims, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    ByteBuffer serializedKey = timeAndDimsSerialization(timeAndDims);
    WritableOakBufferImpl oakValue = (WritableOakBufferImpl) oak.get(serializedKey);
    ByteBuffer aggBuffer = oakValue.getByteBuffer();
    return agg.getFloat(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected long getMetricLongValue(TimeAndDims timeAndDims, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    ByteBuffer serializedKey = timeAndDimsSerialization(timeAndDims);
    WritableOakBufferImpl oakValue = (WritableOakBufferImpl) oak.get(serializedKey);
    ByteBuffer aggBuffer = oakValue.getByteBuffer();
    return agg.getLong(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected Object getMetricObjectValue(TimeAndDims timeAndDims, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    ByteBuffer serializedKey = timeAndDimsSerialization(timeAndDims);
    WritableOakBufferImpl oakValue = (WritableOakBufferImpl) oak.get(serializedKey);
    ByteBuffer aggBuffer = oakValue.getByteBuffer();
    return agg.get(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected double getMetricDoubleValue(TimeAndDims timeAndDims, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    ByteBuffer serializedKey = timeAndDimsSerialization(timeAndDims);
    WritableOakBufferImpl oakValue = (WritableOakBufferImpl) oak.get(serializedKey);
    ByteBuffer aggBuffer = oakValue.getByteBuffer();
    return agg.getDouble(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected boolean isNull(TimeAndDims timeAndDims, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    ByteBuffer serializedKey = timeAndDimsSerialization(timeAndDims);
    WritableOakBufferImpl oakValue = (WritableOakBufferImpl) oak.get(serializedKey);
    ByteBuffer aggBuffer = oakValue.getByteBuffer();
    return agg.isNull(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public Iterable<TimeAndDims> timeRangeIterable(
      boolean descending, long timeStart, long timeEnd)
  {
    if (timeStart > timeEnd) {
      return null;
    }
    ByteBuffer from = timeAndDimsSerialization(new TimeAndDims(timeStart, null, dimensionDescsList));
    ByteBuffer to = timeAndDimsSerialization(new TimeAndDims(timeEnd + 1, null, dimensionDescsList));
    OakMap subMap = oak.subMap(from, true, to, false);
    if (descending == true) {
      subMap = subMap.descendingMap();
    }

    CloseableIterator<ByteBuffer> keysIterator = subMap.keysIterator();

    return new Iterable<TimeAndDims>() {
      @Override
      public Iterator<TimeAndDims> iterator()
      {
        return Iterators.transform(
            keysIterator,
            byteBuffer -> timeAndDimsDeserialization(byteBuffer)
        );
      }
    };
  }

  @Override
  public Iterable<IncrementalIndex.TimeAndDims> keySet()
  {
    CloseableIterator<ByteBuffer> keysIterator = oak.keysIterator();

    return new Iterable<TimeAndDims>() {
      @Override
      public Iterator<TimeAndDims> iterator()
      {
        return Iterators.transform(
            keysIterator,
            byteBuffer -> timeAndDimsDeserialization(byteBuffer)
        );
      }
    };
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  private Integer addToOak(
      AggregatorFactory[] metrics,
      boolean reportParseExceptions,
      InputRow row,
      AtomicInteger numEntries,
      IncrementalIndex.TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException
  {
    ByteBuffer serializedKey = timeAndDimsSerialization(key);
    OffheapOakCreateValueConsumer valueCreator = new OffheapOakCreateValueConsumer(metrics, reportParseExceptions,
            row, rowContainer, getAggs(), selectors, aggOffsetInBuffer);
    OffheapOakComputeConsumer func = new OffheapOakComputeConsumer(metrics, reportParseExceptions, row, rowContainer,
            aggOffsetInBuffer, getAggs());
    if (numEntries.get() < maxRowCount || skipMaxRowsInMemoryCheck) {
      oak.putIfAbsentComputeIfPresent(serializedKey, valueCreator, aggsTotalSize, func);
      if (func.executed() == false) { // a put operation was executed
        numEntries.incrementAndGet();
      }
    } else {
      if (oak.computeIfPresent(serializedKey, func) == false) { // the key wasn't in oak
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
    }
    return numEntries.get();
  }

  @Override
  protected BufferAggregator[] initAggs(
      AggregatorFactory[] metrics,
      Supplier<InputRow> rowSupplier,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd
  )
  {
    selectors = Maps.newHashMap();
    aggOffsetInBuffer = new int[metrics.length];

    for (int i = 0; i < metrics.length; i++) {
      AggregatorFactory agg = metrics[i];

      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
              agg,
              rowSupplier,
              deserializeComplexMetrics
      );

      selectors.put(
              agg.getName(),
              new OnheapIncrementalIndex.ObjectCachingColumnSelectorFactory(columnSelectorFactory, concurrentEventAdd)
      );

      if (i == 0) {
        aggOffsetInBuffer[i] = metrics[i].getMaxIntermediateSize();
      } else {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i - 1] + metrics[i - 1].getMaxIntermediateSize();
      }
    }
    aggsTotalSize += aggOffsetInBuffer[metrics.length - 1] + metrics[metrics.length - 1].getMaxIntermediateSize();

    return new BufferAggregator[metrics.length];
  }

  public static void aggregate(
          AggregatorFactory[] metrics,
          boolean reportParseExceptions,
          InputRow row,
          ThreadLocal<InputRow> rowContainer,
          ByteBuffer aggBuffer,
          int[] aggOffsetInBuffer,
          BufferAggregator[] aggs
  )
  {
    rowContainer.set(row);

    for (int i = 0; i < metrics.length; i++) {
      final BufferAggregator agg = aggs[i];

      synchronized (agg) {
        try {
          agg.aggregate(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[i]);
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", metrics[i].getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          }
        }
      }
    }
    rowContainer.set(null);
  }

  @Override
  public int add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    TimeAndDims key = toTimeAndDims(row);
    final int rv = addToOak(
            metrics,
            reportParseExceptions,
            row,
            numEntries,
            key,
            in,
            skipMaxRowsInMemoryCheck
    );
    updateMaxIngestedTime(row.getTimestamp());
    return rv;
  }

  ByteBuffer timeAndDimsSerialization(TimeAndDims timeAndDims)
  {
    int allocSize;
    ByteBuffer buf;
    Object[] dims = timeAndDims.getDims();

    if (dims == null) {
      allocSize = Long.BYTES + Integer.BYTES;
      buf = ByteBuffer.allocate(allocSize);
      buf.putLong(timeAndDims.getTimestamp());
      buf.putInt(0);
      buf.position(0);
      return buf;
    }

    // When the dimensionDesc's capabilities are of type ValueType.STRING,
    // the object in timeAndDims.dims is of type int[].
    // In this case, we need to know the array size before allocating the ByteBuffer.
    int sumOfArrayLengths = 0;
    for (int i = 0; i < dims.length; i++) {
      Object dim = dims[i];
      DimensionDesc dimensionDesc = getDimensions().get(i);
      if (dimensionDesc != null) {
      }
      if (dim == null) {
        continue;
      }
      if (getDimValueType(i) == ValueType.STRING) {
        sumOfArrayLengths += ((int[]) dim).length;
      }
    }

    // The ByteBuffer will contain:
    // 1. the timeStamp
    // 2. dims.length
    // 3. the serialization of each dim
    // 4. the array (for dims with capabilities of a String ValueType)
    allocSize = Long.BYTES + Integer.BYTES + ALLOC_PER_DIM * dims.length + Integer.BYTES * sumOfArrayLengths;
    buf = ByteBuffer.allocate(allocSize);
    buf.putLong(timeAndDims.getTimestamp());
    buf.putInt(dims.length);
    int currDimsIndex = DIMS_INDEX;
    int currArrayIndex = DIMS_INDEX + ALLOC_PER_DIM * dims.length;
    for (int dimIndex = 0; dimIndex < dims.length; dimIndex++) {
      ValueType valueType = getDimValueType(dimIndex);
      if (valueType == null || dims[dimIndex] == null) {
        buf.putInt(currDimsIndex, NO_DIM);
        currDimsIndex += ALLOC_PER_DIM;
        continue;
      }
      switch (valueType) {
        case LONG:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putLong(currDimsIndex, (Long) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case FLOAT:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putFloat(currDimsIndex, (Float) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case DOUBLE:
          buf.putInt(currDimsIndex, valueType.ordinal());
          currDimsIndex += Integer.BYTES;
          buf.putDouble(currDimsIndex, (Double) dims[dimIndex]);
          currDimsIndex += Long.BYTES;
          break;
        case STRING:
          buf.putInt(currDimsIndex, valueType.ordinal()); // writing the value type
          currDimsIndex += Integer.BYTES;
          buf.putInt(currDimsIndex, currArrayIndex); // writing the array position
          currDimsIndex += Integer.BYTES;
          if (dims[dimIndex] == null) {
            buf.putInt(currDimsIndex, 0);
            currDimsIndex += Integer.BYTES;
            break;
          }
          int[] array = (int[]) dims[dimIndex];
          buf.putInt(currDimsIndex, array.length); // writing the array length
          currDimsIndex += Integer.BYTES;
          for (int j = 0; j < array.length; j++) {
            buf.putInt(currArrayIndex, array[j]);
            currArrayIndex += Integer.BYTES;
          }
          break;
        default:
          buf.putInt(currDimsIndex, NO_DIM);
          currDimsIndex += ALLOC_PER_DIM;
      }
    }
    buf.position(0);
    return buf;
  }

  TimeAndDims timeAndDimsDeserialization(ByteBuffer buff)
  {
    long timeStamp = getTimestamp(buff);
    int dimsLength = getDimsLength(buff);
    Object[] dims = new Object[dimsLength];
    for (int dimIndex = 0; dimIndex < dimsLength; dimIndex++) {
      Object dim = getDimValue(buff, dimIndex);
      dims[dimIndex] = dim;
    }
    return new TimeAndDims(timeStamp, dims, dimensionDescsList, TimeAndDims.EMPTY_ROW_INDEX);
  }

  private ValueType getDimValueType(int dimIndex)
  {
    DimensionDesc dimensionDesc = getDimensions().get(dimIndex);
    if (dimensionDesc == null) {
      return null;
    }
    ColumnCapabilitiesImpl capabilities = dimensionDesc.getCapabilities();
    if (capabilities == null) {
      return null;
    }
    return capabilities.getType();
  }

  static long getTimestamp(ByteBuffer buff)
  {
    return buff.getLong(buff.position() + TIME_STAMP_INDEX);
  }

  static int getDimsLength(ByteBuffer buff)
  {
    return buff.getInt(buff.position() + DIMS_LENGTH_INDEX);
  }

  static Object getDimValue(ByteBuffer buff, int dimIndex)
  {
    Object dimObject = null;
    int dimsLength = getDimsLength(buff);
    if (dimIndex >= dimsLength) {
      return null;
    }
    int dimType = buff.getInt(getDimIndexInBuffer(buff, dimIndex));
    if (dimType == NO_DIM) {
      return null;
    } else if (dimType == ValueType.DOUBLE.ordinal()) {
      dimObject = buff.getDouble(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.FLOAT.ordinal()) {
      dimObject = buff.getFloat(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.LONG.ordinal()) {
      dimObject = buff.getLong(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
    } else if (dimType == ValueType.STRING.ordinal()) {
      int arrayIndex = buff.position() + buff.getInt(getDimIndexInBuffer(buff, dimIndex) + Integer.BYTES);
      int arraySize = buff.getInt(getDimIndexInBuffer(buff, dimIndex) + 2 * Integer.BYTES);
      int[] array = new int[arraySize];
      for (int i = 0; i < arraySize; i++) {
        array[i] = buff.getInt(arrayIndex);
        arrayIndex += Integer.BYTES;
      }
      dimObject = array;
    }

    return dimObject;
  }

  static boolean checkDimsAllNull(ByteBuffer buff)
  {
    int dimsLength = getDimsLength(buff);
    for (int index = 0; index < dimsLength; index++) {
      if (buff.getInt(getDimIndexInBuffer(buff, index)) != NO_DIM) {
        return false;
      }
    }
    return true;
  }

  static int getDimIndexInBuffer(ByteBuffer buff, int dimIndex)
  {
    int dimsLength = getDimsLength(buff);
    if (dimIndex >= dimsLength) {
      return NO_DIM;
    }
    return buff.position() + DIMS_INDEX + dimIndex * ALLOC_PER_DIM;
  }

  private ByteBuffer getMinTimeAndDimsByteBuffer()
  {
    Object[] dims = new Object[dimensionDescsList.size()];
    for (int i = 0; i < dims.length; i++) {
      dims[i] = null;
    }
    TimeAndDims minTimeAndDims = new TimeAndDims(this.minTimestamp, dims, dimensionDescsList);
    ByteBuffer minTimeAndDimsByteBuffer = timeAndDimsSerialization(minTimeAndDims);
    return minTimeAndDimsByteBuffer;
  }

  public final Comparator<ByteBuffer> dimsByteBufferComparator()
  {
    return new TimeAndDimsByteBuffersComp(dimensionDescsList);
  }

  static final class TimeAndDimsByteBuffersComp implements Comparator<ByteBuffer>
  {
    private List<DimensionDesc> dimensionDescsList;

    public TimeAndDimsByteBuffersComp(List<DimensionDesc> dimensionDescsList)
    {
      this.dimensionDescsList = dimensionDescsList;
    }

    @Override
    public int compare(ByteBuffer lhs, ByteBuffer rhs)
    {
      int retVal = Longs.compare(getTimestamp(lhs), getTimestamp(rhs));
      int numComparisons = Math.min(getDimsLength(lhs), getDimsLength(rhs));

      int dimIndex = 0;
      while (retVal == 0 && dimIndex < numComparisons) {
        int lhsType = lhs.getInt(getDimIndexInBuffer(lhs, dimIndex));
        int rhsType = rhs.getInt(getDimIndexInBuffer(rhs, dimIndex));

        if (lhsType == NO_DIM) {
          if (rhsType == NO_DIM) {
            ++dimIndex;
            continue;
          }
          return -1;
        }

        if (rhsType == NO_DIM) {
          return 1;
        }

        final DimensionIndexer indexer = dimensionDescsList.get(dimIndex).getIndexer();
        Object lhsObject = getDimValue(lhs, dimIndex);
        Object rhsObject = getDimValue(rhs, dimIndex);
        retVal = indexer.compareUnsortedEncodedKeyComponents(lhsObject, rhsObject);
        ++dimIndex;
      }

      if (retVal == 0) {
        int lengthDiff = Ints.compare(getDimsLength(lhs), getDimsLength(rhs));
        if (lengthDiff == 0) {
          return 0;
        }
        ByteBuffer largerDims = lengthDiff > 0 ? lhs : rhs;
        return checkDimsAllNull(largerDims) ? 0 : lengthDiff;
      }
      return retVal;
    }
  }
}
