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

package org.apache.druid.query.aggregation;

import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class LongSumVectorAggregator implements VectorAggregator
{
  private final VectorValueSelector selector;

  public LongSumVectorAggregator(final VectorValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position, 0);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final long[] vector = selector.getLongVector();

    long sum = 0;
    for (int i = startRow; i < endRow; i++) {
      sum += vector[i];
    }

    buf.putLong(position, buf.getLong(position) + sum);
  }

  @Override
  public void aggregate(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows,
      final int positionOffset
  )
  {
    final long[] vector = selector.getLongVector();

    for (int i = 0; i < numRows; i++) {
      final int position = positions[i] + positionOffset;
      buf.putLong(position, buf.getLong(position) + vector[rows != null ? rows[i] : i]);
    }
  }

  @Override
  public Object get(final ByteBuffer buf, final int position)
  {
    return buf.getLong(position);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
