/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.aggregation;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import io.druid.segment.FloatColumnSelector;

import java.util.Comparator;

/**
 */
public class DoubleSumAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Doubles.compare(((Number) o).doubleValue(), ((Number) o1).doubleValue());
    }
  }.nullsFirst();

  static double combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).doubleValue() + ((Number) rhs).doubleValue();
  }

  private final FloatColumnSelector selector;
  private final String name;
  private final int exponent;

  private double sum;

  public DoubleSumAggregator(String name, FloatColumnSelector selector, int exponent)
  {
    this.name = name;
    this.selector = selector;
    this.exponent = exponent;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += (exponent == 1 ? selector.get() : Math.pow(selector.get(), exponent));
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
    return (float) sum;
  }

  @Override
  public long getLong()
  {
    return (long) sum;
  }

  @Override
  public String getName()
  {
    return this.name;
  }

  @Override
  public Aggregator clone()
  {
    return new DoubleSumAggregator(name, selector, exponent);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
