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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * A buffered aggregator to compute a min CompressedBigDecimal
 */
public class CompressedBigDecimalMinBufferAggregator implements BufferAggregator
{

  private static final int HEADER_OFFSET_BYTES =
      CompressedBigDecimalMinAggregatorFactory.BUFFER_AGGREGATOR_HEADER_SIZE_BYTES;

  //Cache will hold the aggregated value.
  //We are using ByteBuffer to hold the key to the aggregated value.
  private final ColumnValueSelector<CompressedBigDecimal> selector;
  private final int size;
  private final int scale;
  private final boolean strictNumberParsing;

  /**
   * Constructor.
   *
   * @param size                the size to allocate
   * @param scale               the scale
   * @param selector            a ColumnSelector to retrieve incoming values
   * @param strictNumberParsing true => NumberFormatExceptions thrown; false => NumberFormatException returns 0
   */
  public CompressedBigDecimalMinBufferAggregator(
      int size,
      int scale,
      ColumnValueSelector<CompressedBigDecimal> selector,
      boolean strictNumberParsing
  )
  {
    this.selector = selector;
    this.size = size;
    this.scale = scale;
    this.strictNumberParsing = strictNumberParsing;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBufferCompressedBigDecimal.initMax(buf, position + HEADER_OFFSET_BYTES, size);
    setEmpty(true, buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    CompressedBigDecimal addend = Utils.objToCompressedBigDecimalWithScale(
        selector.getObject(),
        scale,
        strictNumberParsing
    );

    if (addend != null) {
      setEmpty(false, buf, position);

      CompressedBigDecimal existing = new ByteBufferCompressedBigDecimal(
          buf,
          position + HEADER_OFFSET_BYTES,
          size,
          scale
      );

      existing.accumulateMin(addend);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    if (isEmpty(buf, position)) {
      return null;
    }

    ByteBufferCompressedBigDecimal byteBufferCompressedBigDecimal = new ByteBufferCompressedBigDecimal(
        buf,
        position + HEADER_OFFSET_BYTES,
        size,
        scale
    );

    CompressedBigDecimal heapCompressedBigDecimal = byteBufferCompressedBigDecimal.toHeap();

    return heapCompressedBigDecimal;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException(getClass().getSimpleName() + " does not support getLong()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  private static void setEmpty(boolean value, ByteBuffer byteBuffer, int position)
  {
    byteBuffer.put(position, (byte) (value ? 1 : 0));
  }

  private static boolean isEmpty(ByteBuffer byteBuffer, int position)
  {
    return byteBuffer.get(position) != 0;
  }
}
