/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.cardinality;

import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class CardinalityBufferAggregator implements BufferAggregator
{
  private final List<DimensionSelector> selectorList;
  private final boolean byRow;

  private static final byte[] EMPTY_BYTES = HyperLogLogCollector.makeEmptyVersionedByteArray();

  public CardinalityBufferAggregator(
      List<DimensionSelector> selectorList,
      boolean byRow
  )
  {
    this.selectorList = selectorList;
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
    final HyperLogLogCollector collector = HyperLogLogCollector.makeCollector(
        (ByteBuffer) buf.duplicate().position(position).limit(
            position
            + HyperLogLogCollector.getLatestNumBytesForDenseStorage()
        )
    );
    if (byRow) {
      CardinalityAggregator.hashRow(selectorList, collector);
    } else {
      CardinalityAggregator.hashValues(selectorList, collector);
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
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
