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

public abstract class CompressedBigDecimalBufferAggregatorBase implements BufferAggregator
{
  protected final ColumnValueSelector<CompressedBigDecimal> selector;
  protected final int size;
  protected final int scale;
  protected final boolean strictNumberParsing;

  private final int headerSizeBytes;

  /**
   * Constructor.
   *
   * @param size                the size to allocate
   * @param scale               the scale
   * @param selector            a ColumnSelector to retrieve incoming values
   * @param strictNumberParsing true => NumberFormatExceptions thrown; false => NumberFormatException returns 0
   * @param headerSizeBytes     size of a header, if it exists
   */
  public CompressedBigDecimalBufferAggregatorBase(
      int size,
      int scale,
      ColumnValueSelector<CompressedBigDecimal> selector,
      boolean strictNumberParsing,
      int headerSizeBytes
  )
  {
    this.selector = selector;
    this.size = size;
    this.scale = scale;
    this.strictNumberParsing = strictNumberParsing;
    this.headerSizeBytes = headerSizeBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBufferCompressedBigDecimal.initMin(buf, position + headerSizeBytes, size);
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
          position + headerSizeBytes,
          size,
          scale
      );

      existing.accumulateMax(addend);
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
        position + headerSizeBytes,
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

  //do not allow overriding to avoid megamorphic callsites in this class and subclasses
  protected final void setEmpty(boolean value, ByteBuffer byteBuffer, int position)
  {
    // no header means the aggregator is considered non-empty
    if (headerSizeBytes > 0) {
      byteBuffer.put(position, (byte) (value ? 1 : 0));
    }
  }

  //do not allow overriding to avoid megamorphic callsites in this class and subclasses
  protected final boolean isEmpty(ByteBuffer byteBuffer, int position)
  {
    // no header means the aggregator is considered non-empty
    if (headerSizeBytes > 0) {
      return byteBuffer.get(position) != 0;
    } else {
      return false;
    }
  }
}
