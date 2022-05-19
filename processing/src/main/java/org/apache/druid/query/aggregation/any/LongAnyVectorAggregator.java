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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.segment.vector.VectorValueSelector;

import java.nio.ByteBuffer;

/**
 * Vectorized implementation of the {@link LongAnyBufferAggregator}
 */
public class LongAnyVectorAggregator extends NumericAnyVectorAggregator
{

  public LongAnyVectorAggregator(VectorValueSelector vectorValueSelector)
  {
    super(vectorValueSelector);
  }

  @Override
  void initValue(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0L);
  }

  @Override
  boolean putAnyValueFromRow(ByteBuffer buf, int position, int startRow, int endRow)
  {
    long[] values = vectorValueSelector.getLongVector();
    boolean isRowsWithinIndex = startRow < endRow && startRow < values.length;
    if (isRowsWithinIndex) {
      buf.putLong(position, values[startRow]);
    }
    return isRowsWithinIndex;
  }

  @Override
  Object getNonNullObject(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }
}
