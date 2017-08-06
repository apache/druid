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

import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public class FloatMaxAggregator implements Aggregator
{
  static final Comparator COMPARATOR = FloatSumAggregator.COMPARATOR;

  static double combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number) lhs).floatValue(), ((Number) rhs).floatValue());
  }

  private final FloatColumnSelector selector;

  private float max;

  public FloatMaxAggregator(FloatColumnSelector selector)
  {
    this.selector = selector;

    reset();
  }

  @Override
  public void aggregate()
  {
    max = Math.max(max, selector.get());
  }

  @Override
  public void reset()
  {
    max = Float.NEGATIVE_INFINITY;
  }

  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    return max;
  }

  @Override
  public long getLong()
  {
    return (long) max;
  }

  @Override
  public double getDouble()
  {
    return (double) max;
  }

  @Override
  public Aggregator clone()
  {
    return new FloatMaxAggregator(selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
