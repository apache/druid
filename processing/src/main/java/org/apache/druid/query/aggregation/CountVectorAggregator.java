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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class CountVectorAggregator implements VectorAggregator
{
  @Override
  public void init(final ByteBuffer buf, final int position)
  {
    buf.putLong(position, 0);
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    final int delta = endRow - startRow;
    buf.putLong(position, buf.getLong(position) + delta);
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
    for (int i = 0; i < numRows; i++) {
      final int position = positions[i] + positionOffset;
      buf.putLong(position, buf.getLong(position) + 1);
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
