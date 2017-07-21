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

import io.druid.data.input.impl.TimestampSpec;
import io.druid.segment.ObjectColumnSelector;

import java.util.Comparator;

public class TimestampAggregator implements Aggregator
{
  static final Comparator COMPARATOR = LongMaxAggregator.COMPARATOR;

  static long combineValues(Object lhs, Object rhs)
  {
    return Math.max(((Number)lhs).longValue(), ((Number)rhs).longValue());
  }

  private final ObjectColumnSelector selector;
  private final String name;
  private final TimestampSpec timestampSpec;
  private final Comparator<Long> comparator;
  private final Long initValue;

  private long most;

  public TimestampAggregator(
      String name,
      ObjectColumnSelector selector,
      TimestampSpec timestampSpec,
      Comparator<Long> comparator,
      Long initValue
  )
  {
    this.name = name;
    this.selector = selector;
    this.timestampSpec = timestampSpec;
    this.comparator = comparator;
    this.initValue = initValue;

    reset();
  }

  @Override
  public void aggregate()
  {
    Long value = TimestampAggregatorFactory.convertLong(timestampSpec, selector.get());

    if (value != null) {
      most = comparator.compare(most, value) > 0 ? most : value;
    }
  }

  @Override
  public void reset()
  {
    most = initValue;
  }

  @Override
  public Object get()
  {
    return most;
  }

  @Override
  public float getFloat()
  {
    return (float) most;
  }

  @Override
  public double getDouble()
  {
    return (double) most;
  }

  @Override
  public long getLong()
  {
    return most;
  }

  @Override
  public void close()
  {
    // no resource to cleanup
  }

  @Override
  public Aggregator clone()
  {
    return new TimestampAggregator(name, selector, timestampSpec, comparator, initValue);
  }
}
