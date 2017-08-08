/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.segment.LongColumnSelector;

import java.util.Comparator;

/**
 */
public class LongMinAggregator implements Aggregator
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  static long combineValues(Object lhs, Object rhs)
  {
    return Math.min(((Number) lhs).longValue(), ((Number) rhs).longValue());
  }

  private final LongColumnSelector selector;

  private long min;

  public LongMinAggregator(LongColumnSelector selector)
  {
    this.selector = selector;

    reset();
  }

  @Override
  public void aggregate()
  {
    min = Math.min(min, selector.get());
  }

  @Override
  public void reset()
  {
    min = Long.MAX_VALUE;
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    return (float) min;
  }

  @Override
  public long getLong()
  {
    return min;
  }

  @Override
  public double getDouble()
  {
    return (double) min;
  }

  @Override
  public Aggregator clone()
  {
    return new LongMinAggregator(selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
