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

package org.apache.druid.query.aggregation.last;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public abstract class NumericLastVectorAggregator implements VectorAggregator
{
  final VectorValueSelector valueSelector;
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final VectorValueSelector timeSelector;
  long lastTime;
  boolean rhsNull;

  public NumericLastVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector)
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    lastTime = Long.MIN_VALUE;
    rhsNull = !useDefault;
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final long[] vector = timeSelector.getLongVector();
    final boolean[] nulls = valueSelector.getNullVector();
    boolean foundNull = false;

    //check if nulls is null or not
    if (nulls == null) {
      foundNull = true;
    }

    //the time vector is already sorted so the last element would be the latest
    //traverse the value vector from the back (for latest)

    int index = 0;
    if (!useDefault && foundNull == false) {
      for (int i = endRow - 1; i >= startRow; i--) {
        if (nulls[i] == false) {
          index = i;
          break;
        }
      }
    } else {
      index = endRow - 1;
    }

    //find the first non-null value
    final long latestTime = vector[index];

    if (latestTime > lastTime) {
      lastTime = latestTime;
      if (useDefault || (index >= startRow && index < endRow)) {
        putValue(buf, position, index);
        rhsNull = false;
      } else {
        rhsNull = true;
      }
    }
  }

  @Override
  public void aggregate(
      ByteBuffer buf,
      int numRows,
      int[] positions,
      @Nullable int[] rows,
      int positionOffset
  )
  {

  }

  abstract void putValue(ByteBuffer buf, int position, int index);

  @Override
  public void close()
  {

  }
}
