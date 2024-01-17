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

import org.apache.druid.query.aggregation.SerializablePairLongFloat;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Vectorized version of on heap 'last' aggregator for column selectors with type FLOAT..
 */
public class FloatLastVectorAggregator extends NumericLastVectorAggregator
{
  float lastValue;

  public FloatLastVectorAggregator(VectorValueSelector timeSelector, VectorObjectSelector objectSelector)
  {
    super(timeSelector, null, objectSelector);
    lastValue = 0;
  }

  public FloatLastVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector)
  {
    super(timeSelector, valueSelector, null);
    lastValue = 0;
  }

  @Override
  void putValue(ByteBuffer buf, int position, int index)
  {
    lastValue = valueSelector != null ? valueSelector.getFloatVector()[index] : ((SerializablePairLongFloat) objectSelector.getObjectVector()[index]).getRhs();
    buf.putFloat(position, lastValue);
  }


  @Override
  public void initValue(ByteBuffer buf, int position)
  {
    buf.putFloat(position, 0);
  }


  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final boolean rhsNull = isValueNull(buf, position);
    return new SerializablePairLongFloat(buf.getLong(position), rhsNull ? null : buf.getFloat(position + VALUE_OFFSET));
  }
}

