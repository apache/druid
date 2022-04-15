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

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;


public class DoubleLastVectorAggregator implements VectorAggregator
{
  private final boolean useDefault = NullHandling.replaceWithDefault();
  private final VectorValueSelector timeSelector;
  private final VectorValueSelector valueSelector;
  double lastValue;
  long lastTime;
  boolean rhsNull;

  public DoubleLastVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector)
  {

    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    lastTime = Long.MIN_VALUE;
    rhsNull = !useDefault;
    lastValue = 0;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, Double.NEGATIVE_INFINITY);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    final long[] vector = timeSelector.getLongVector();
    final boolean[] nulls = valueSelector.getNullVector();
    boolean status = true;
    if (nulls == null) {
      status = true;
    } else {
      status = !nulls[endRow - 1];
    }
    //the time vector is already sorted so the last element would be the latest
    long latestTime = vector[endRow - 1];
    if (latestTime > lastTime) {
      lastTime = latestTime;
      if (useDefault || status) {
        lastValue = valueSelector.getLongVector()[endRow - 1];
        rhsNull = false;
      } else {
        rhsNull = true;
      }
    }
    buf.putDouble(position, lastValue);
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    final double[] vector = valueSelector.getDoubleVector();
    final int pos = positions[numRows - 1] + positionOffset;
    buf.putDouble(pos, vector[rows != null ? rows[numRows - 1] : numRows - 1]);

  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return new SerializablePair<>(lastTime, rhsNull ? null : lastValue);
  }

  @Override
  public void close()
  {
    //nothing to close
  }
}

