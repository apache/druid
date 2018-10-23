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

import org.apache.druid.segment.BaseFloatColumnValueSelector;

/**
 */
public class FloatMinAggregator implements Aggregator
{
  static double combineValues(Object lhs, Object rhs)
  {
    return Math.min(((Number) lhs).floatValue(), ((Number) rhs).floatValue());
  }

  private final BaseFloatColumnValueSelector selector;

  private float min;

  public FloatMinAggregator(BaseFloatColumnValueSelector selector)
  {
    this.selector = selector;
    this.min = Float.POSITIVE_INFINITY;
  }

  @Override
  public void aggregate()
  {
    min = Math.min(min, selector.getFloat());
  }

  @Override
  public Object get()
  {
    return min;
  }

  @Override
  public float getFloat()
  {
    return min;
  }

  @Override
  public long getLong()
  {
    return (long) min;
  }

  @Override
  public double getDouble()
  {
    return (double) min;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
