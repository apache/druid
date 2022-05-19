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

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import org.apache.druid.segment.BaseLongColumnValueSelector;

import java.util.Comparator;

/**
 */
public class LongSumAggregator implements Aggregator
{
  static final Comparator COMPARATOR = new Ordering()
  {
    @Override
    public int compare(Object o, Object o1)
    {
      return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
    }
  }.nullsFirst();

  static long combineValues(Object lhs, Object rhs)
  {
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  private final BaseLongColumnValueSelector selector;

  private long sum;

  public LongSumAggregator(BaseLongColumnValueSelector selector)
  {
    this.selector = selector;

    this.sum = 0;
  }

  @Override
  public void aggregate()
  {
    sum += selector.getLong();
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
    return sum;
  }

  @Override
  public double getDouble()
  {
    return (double) sum;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
