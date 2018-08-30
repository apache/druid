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

import org.apache.druid.segment.BaseDoubleColumnValueSelector;

import java.nio.ByteBuffer;

/**
 */
public class DoubleMaxBufferAggregator extends SimpleDoubleBufferAggregator
{

  DoubleMaxBufferAggregator(BaseDoubleColumnValueSelector selector)
  {
    super(selector);
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putDouble(position, Double.NEGATIVE_INFINITY);
  }

  @Override
  public void putFirst(ByteBuffer buf, int position, double value)
  {
    if (!Double.isNaN(value)) {
      buf.putDouble(position, value);
    } else {
      init(buf, position);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, double value)
  {
    buf.putDouble(position, Math.max(buf.getDouble(position), value));
  }
}
