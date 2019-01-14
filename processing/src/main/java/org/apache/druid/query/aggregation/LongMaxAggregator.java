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

import org.apache.druid.segment.BaseLongColumnValueSelector;

/**
 */
public class LongMaxAggregator implements Aggregator
{
  static long combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).longValue(), ((Number) rhs).longValue());
  }

  private final BaseLongColumnValueSelector selector;

  private long max;

  public LongMaxAggregator(BaseLongColumnValueSelector selector)
  {
    this.selector = selector;
    this.max = Long.MIN_VALUE;
  }

  @Override
  public void aggregate()
  {
    max = Math.max(max, selector.getLong());
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    return (float) max;
  }

  @Override
  public long getLong()
  {
    return max;
  }

  @Override
  public double getDouble()
  {
    return (double) max;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
