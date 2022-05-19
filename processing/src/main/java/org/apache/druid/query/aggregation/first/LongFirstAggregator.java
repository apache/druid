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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.segment.BaseLongColumnValueSelector;

public class LongFirstAggregator extends NumericFirstAggregator<BaseLongColumnValueSelector>
{
  long firstValue;

  public LongFirstAggregator(BaseLongColumnValueSelector timeSelector, BaseLongColumnValueSelector valueSelector)
  {
    super(timeSelector, valueSelector);
    firstValue = 0;
  }

  @Override
  void setCurrentValue()
  {
    firstValue = valueSelector.getLong();
  }

  @Override
  public Object get()
  {
    return new SerializablePair<>(firstTime, rhsNull ? null : firstValue);
  }

  @Override
  public float getFloat()
  {
    return (float) firstValue;
  }

  @Override
  public double getDouble()
  {
    return (double) firstValue;
  }

  @Override
  public long getLong()
  {
    return firstValue;
  }
}
