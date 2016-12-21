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

package io.druid.query.aggregation.cardinality;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;

import java.nio.ByteBuffer;
import java.util.List;

public class CardinalityBufferAggregator implements BufferAggregator
{
  private final List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList;
  private final boolean byRow;

  private static final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();

  public CardinalityBufferAggregator(
      List<ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>> selectorPlusList,
      boolean byRow
  )
  {
    this.selectorPlusList = selectorPlusList;
    this.byRow = byRow;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.put(EMPTY_BYTES);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    // Save position, limit and restore later instead of allocating a new ByteBuffer object
    final int oldPosition = buf.position();
    final int oldLimit = buf.limit();
    buf.limit(position + HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    buf.position(position);

    try {
      final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(buf);
      if (byRow) {
        CardinalityAggregator.hashRow(selectorPlusList, collector);
      } else {
        CardinalityAggregator.hashValues(selectorPlusList, collector);
      }
    }
    finally {
      buf.limit(oldLimit);
      buf.position(oldPosition);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer dataCopyBuffer = ByteBuffer.allocate(HyperLogLogCollector.getLatestNumBytesForDenseStorage());
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    mutationBuffer.get(dataCopyBuffer.array());
    return HyperLogLogCollector.makeCollector(dataCopyBuffer);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CardinalityBufferAggregator does not support getFloat()");
  }


  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CardinalityBufferAggregator does not support getLong()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
