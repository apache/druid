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

package org.apache.druid.query.aggregation.constant;

import org.apache.druid.query.aggregation.BufferAggregator;

import java.nio.ByteBuffer;

/**
 * {@link BufferAggregator} variant of {@link LongConstantAggregator}
 */
public class LongConstantBufferAggregator implements BufferAggregator
{
  private final long value;

  public LongConstantBufferAggregator(long value)
  {
    this.value = value;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    // Since we always return a constant value despite what is in the buffer, there is no need to
    // update the buffer at all
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {

  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return value;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return (float) value;
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return value;
  }

  @Override
  public void close()
  {

  }
}
