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
 * A buffered aggregator to aggregate big decimal value.
 */
public class CompressedBigDecimalBufferAggregator implements BufferAggregator
{

  //Cache will hold the aggregated value.
  //We are using ByteBuffer to hold the key to the aggregated value.
  private final ColumnValueSelector<CompressedBigDecimal<?>> selector;
  private final int size;
  private final int scale;

  /**
   * Constructor.
   *
   * @param size     the size to allocate
   * @param scale    the scale
   * @param selector a ColumnSelector to retrieve incoming values
   */
  public CompressedBigDecimalBufferAggregator(
      int size,
      int scale,
      ColumnValueSelector<CompressedBigDecimal<?>> selector
  )
  {
    this.selector = selector;
    this.size = size;
    this.scale = scale;
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.BufferAggregator#init(java.nio.ByteBuffer, int)
   */
  @Override
  public void init(ByteBuffer buf, int position)
  {
    for (int ii = 0; ii < size; ++ii) {
      buf.putInt(position + (ii * Integer.BYTES), 0);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.BufferAggregator#aggregate(java.nio.ByteBuffer, int)
   */
  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    CompressedBigDecimal<?> addend = selector.getObject();
    if (addend != null) {
      Utils.accumulate(buf, position, size, scale, addend);
    }
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.BufferAggregator#get(java.nio.ByteBuffer, int)
   */
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return new ByteBufferCompressedBigDecimal(buf, position, size, scale);
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.BufferAggregator#getFloat(java.nio.ByteBuffer, int)
   */
  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CompressedBigDecimalBufferAggregator does not support getFloat()");
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.BufferAggregator#getLong(java.nio.ByteBuffer, int)
   */
  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CompressedBigDecimalBufferAggregator does not support getLong()");
  }

  /* (non-Javadoc)
   * @see org.apache.druid.query.aggregation.BufferAggregator#close()
   */
  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
