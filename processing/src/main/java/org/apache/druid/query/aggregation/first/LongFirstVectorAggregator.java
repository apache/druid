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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class LongFirstVectorAggregator extends NumericFirstVectorAggregator
{
  public LongFirstVectorAggregator(VectorValueSelector timeSelector, VectorObjectSelector objectSelector)
  {
    super(timeSelector, null, objectSelector);
  }

  public LongFirstVectorAggregator(VectorValueSelector timeSelector, VectorValueSelector valueSelector)
  {
    super(timeSelector, valueSelector, null);
  }

  @Override
  public void initValue(ByteBuffer buf, int position)
  {
    buf.putLong(position, 0);
  }

  @Override
  void putValue(ByteBuffer buf, int position, int index)
  {
    long firstValue = valueSelector != null ? valueSelector.getLongVector()[index] : ((SerializablePairLongLong) objectSelector.getObjectVector()[index]).getRhs();
    buf.putLong(position, firstValue);
  }

  /**
   * @return The object as a pair with the position and the value stored at the position in the buffer.
   */
  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    final boolean rhsNull = isValueNull(buf, position);
    return new SerializablePairLongLong(buf.getLong(position), rhsNull ? null : buf.getLong(position + VALUE_OFFSET));
  }
}
