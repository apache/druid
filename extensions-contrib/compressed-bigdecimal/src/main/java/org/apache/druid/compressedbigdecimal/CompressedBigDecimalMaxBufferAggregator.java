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
 * A buffered aggregator to compute a max CompressedBigDecimal
 */
public class CompressedBigDecimalMaxBufferAggregator extends CompressedBigDecimalBufferAggregatorBase
{
  private static final int HEADER_SIZE_BYTES =
        CompressedBigDecimalAggregatorFactoryBase.BUFFER_AGGREGATOR_HEADER_SIZE_BYTES;

  /**
   * Constructor.
   *
   * @param size                the size to allocate
   * @param scale               the scale
   * @param selector            a ColumnSelector to retrieve incoming values
   * @param strictNumberParsing true => NumberFormatExceptions thrown; false => NumberFormatException returns 0
   */
  public CompressedBigDecimalMaxBufferAggregator(
      int size,
      int scale,
      ColumnValueSelector<CompressedBigDecimal> selector,
      boolean strictNumberParsing
  )
  {
    super(size, scale, selector, strictNumberParsing, HEADER_SIZE_BYTES);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBufferCompressedBigDecimal.initMin(buf, position + HEADER_SIZE_BYTES, size);
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
          position + HEADER_SIZE_BYTES,
          size,
          scale
      );

      existing.accumulateMax(addend);
    }
  }
}
