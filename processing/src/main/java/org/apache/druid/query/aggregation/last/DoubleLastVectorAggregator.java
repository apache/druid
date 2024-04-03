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

import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.query.aggregation.first.FirstLastUtils;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Vectorized version of on heap 'last' aggregator for column selectors with type DOUBLE..
 */
public class DoubleLastVectorAggregator extends NumericLastVectorAggregator<Double, SerializablePairLongDouble>
{
  double lastValue;

  public DoubleLastVectorAggregator(VectorValueSelector timeSelector, VectorObjectSelector objectSelector)
  {
    super(timeSelector, null, objectSelector);
    lastValue = 0;
  }

  public DoubleLastVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector)
  {
    super(timeSelector, valueSelector, null);
    lastValue = 0;
  }

  @Override
  void initValue(ByteBuffer buf, int position)
  {
    buf.putDouble(position, 0.0d);
  }

  @Override
  void putValue(ByteBuffer buf, int position, Double value)
  {
    buf.putDouble(position, value);
  }

  @Override
  void putValue(ByteBuffer buf, int position, VectorValueSelector valueSelector, int index)
  {
    buf.putDouble(position, valueSelector.getDoubleVector()[index]);
  }

  @Override
  void putDefaultValue(ByteBuffer buf, int position)
  {
    buf.putDouble(position, 0.0d);
  }

  @Override
  Double getValue(ByteBuffer buf, int position)
  {
    return buf.getDouble(position);
  }

  @Override
  SerializablePairLongDouble readPairFromVectorSelectors(
      boolean[] timeNullityVector,
      long[] timeVector,
      Object[] maybeFoldedObjects,
      int index
  )
  {
    return FirstLastUtils.readDoublePairFromVectorSelectors(timeNullityVector, timeVector, maybeFoldedObjects, index);
  }

  @Override
  SerializablePairLongDouble createPair(long time, @Nullable Double value)
  {
    return new SerializablePairLongDouble(time, value);
  }
}

