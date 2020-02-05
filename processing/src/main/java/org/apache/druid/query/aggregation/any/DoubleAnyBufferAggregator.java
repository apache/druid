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

import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * This Aggregator is created by the {@link DoubleAnyAggregatorFactory} which has no special null handling logic.
 * Hence, null can be pass into this aggregator from the valueSelector and null can be return from this aggregator.
 */
public class DoubleAnyBufferAggregator extends NumericAnyBufferAggregator<BaseDoubleColumnValueSelector>
{
  public DoubleAnyBufferAggregator(
      BaseDoubleColumnValueSelector valueSelector
  )
  {
    super(valueSelector);
  }

  @Override
  void initValue(ByteBuffer buf, int position)
  {
    buf.putDouble(getFoundValueStoredPosition(position), 0);
  }

  @Override
  void putValue(ByteBuffer buf, int position)
  {
    buf.putDouble(getFoundValueStoredPosition(position), valueSelector.getDouble());
  }

  @Override
  @Nullable
  public Object get(ByteBuffer buf, int position)
  {
    final boolean isNull = isValueNull(buf, position);
    return isNull ? null : buf.getDouble(getFoundValueStoredPosition(position));
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getDouble(getFoundValueStoredPosition(position));
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return (long) buf.getDouble(getFoundValueStoredPosition(position));
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return buf.getDouble(getFoundValueStoredPosition(position));
  }
}
