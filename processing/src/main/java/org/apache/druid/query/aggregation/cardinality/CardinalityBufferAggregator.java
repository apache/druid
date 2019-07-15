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

package org.apache.druid.query.aggregation.cardinality;

import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesBufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import java.nio.ByteBuffer;

public class CardinalityBufferAggregator implements BufferAggregator
{
  private final ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses;
  private final boolean byRow;

  CardinalityBufferAggregator(
      ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] selectorPluses,
      boolean byRow
  )
  {
    this.selectorPluses = selectorPluses;
    this.byRow = byRow;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    HyperUniquesBufferAggregator.doInit(buf, position);
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
        CardinalityAggregator.hashRow(selectorPluses, collector);
      } else {
        CardinalityAggregator.hashValues(selectorPluses, collector);
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
    return HyperUniquesBufferAggregator.doGet(buf, position);
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
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("CardinalityBufferAggregators does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    for (int i = 0; i < selectorPluses.length; i++) {
      inspector.visit(StringUtils.format("selector-%d", i), selectorPluses[i].getSelector());
    }
  }
}
