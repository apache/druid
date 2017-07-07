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

import com.google.common.collect.Ordering;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public class FloatSumAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Float.compare(((Number) o).floatValue(), ((Number) o1).floatValue());
    }
  }.nullsFirst();

  static double combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).floatValue() + ((Number) rhs).floatValue();
  }

  private final FloatColumnSelector selector;

  private float sum;

  public FloatSumAggregator(FloatColumnSelector selector)
  {
    this.selector = selector;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += selector.get();
  }

  @Override
  public void reset()
  {
    sum = 0;
  }

  @Override
  public Object get()
  {
    return sum;
  }

  @Override
  public float getFloat()
  {
    return sum;
  }

  @Override
  public long getLong()
  {
    return (long) sum;
  }

  @Override
  public double getDouble()
  {
    return (double) sum;
  }

  @Override
  public Aggregator clone()
  {
    return new FloatSumAggregator(selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
