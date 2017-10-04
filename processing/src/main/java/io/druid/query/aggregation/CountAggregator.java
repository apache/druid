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

import java.util.Comparator;

/**
 */
public class CountAggregator implements Aggregator
{
  static final Comparator COMPARATOR = LongSumAggregator.COMPARATOR;

  static Object combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  private long count = 0;

  public CountAggregator()
  {
  }

  @Override
  public void aggregate()
  {
    ++count;
  }

  @Override
  public void reset()
  {
    count = 0;
  }

  @Override
  public Object get()
  {
    return count;
  }

  @Override
  public float getFloat()
  {
    return (float) count;
  }

  @Override
  public long getLong()
  {
    return count;
  }

  @Override
  public double getDouble()
  {
    return (double) count;
  }

  @Override
  public Aggregator clone()
  {
    return new CountAggregator();
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
