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

import org.apache.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * A buffered aggregator to aggregate big decimal value.
 */
public class CompressedBigDecimalSumBufferAggregator extends CompressedBigDecimalBufferAggregatorBase
{
  /**
   * Constructor.
   *
   * @param size                the size to allocate
   * @param scale               the scale
   * @param selector            a ColumnSelector to retrieve incoming values
   * @param strictNumberParsing true => NumberFormatExceptions thrown; false => NumberFormatException returns 0
   */
  public CompressedBigDecimalSumBufferAggregator(
      int size,
      int scale,
      ColumnValueSelector<CompressedBigDecimal> selector,
      boolean strictNumberParsing
  )
  {
    super(size, scale, selector, strictNumberParsing, 0);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBufferCompressedBigDecimal.initZero(buf, position, size);
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
      CompressedBigDecimal existing = new ByteBufferCompressedBigDecimal(buf, position, size, scale);

      existing.accumulateSum(addend);
    }
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
}
