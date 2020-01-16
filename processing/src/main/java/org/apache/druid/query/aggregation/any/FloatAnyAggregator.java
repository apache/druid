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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.NullableNumericAggregator;
import org.apache.druid.query.aggregation.NullableNumericAggregatorFactory;
import org.apache.druid.segment.BaseFloatColumnValueSelector;

/**
 * This Aggregator is created by the {@link FloatAnyAggregatorFactory} which extends from
 * {@link NullableNumericAggregatorFactory}. If null needs to be handle, then {@link NullableNumericAggregatorFactory}
 * will wrap this aggregator in {@link NullableNumericAggregator} and can handle all null in that class.
 * Hence, no null will ever be pass into this aggregator from the valueSelector.
 */
public class FloatAnyAggregator implements Aggregator
{
  private final BaseFloatColumnValueSelector valueSelector;

  private float foundValue;
  private boolean isFound;

  public FloatAnyAggregator(BaseFloatColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    this.foundValue = 0;
    this.isFound = false;
  }

  @Override
  public void aggregate()
  {
    if (!isFound) {
      foundValue = valueSelector.getFloat();
      isFound = true;
    }
  }

  @Override
  public Object get()
  {
    return foundValue;
  }

  @Override
  public float getFloat()
  {
    return foundValue;
  }

  @Override
  public long getLong()
  {
    return (long) foundValue;
  }

  @Override
  public double getDouble()
  {
    return (double) foundValue;
  }

  @Override
  public void close()
  {
    // no-op
  }
}
