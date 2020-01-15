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

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.nio.ByteBuffer;

public class LongAnyBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector valueSelector;

  private boolean isValueFound;

  public LongAnyBufferAggregator(BaseLongColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    isValueFound = false;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (!isValueFound && !valueSelector.isNull()) {
      buf.putLong(position, valueSelector.getLong());
      isValueFound = true;
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) buf.getLong(position);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    return (double) buf.getLong(position);
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return buf.getLong(position);
  }

  @Override
  public void close()
  {

  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("valueSelector", valueSelector);
  }
}
