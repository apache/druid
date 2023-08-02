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

package org.apache.druid.query.aggregation.last;

import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;

public class DoubleLastAggregator extends NumericLastAggregator
{
  double lastValue;

  public DoubleLastAggregator(BaseLongColumnValueSelector timeSelector, ColumnValueSelector valueSelector, boolean needsFoldCheck)
  {
    super(timeSelector, valueSelector, needsFoldCheck);
    lastValue = 0;
  }

  @Override
  void setLastValue()
  {
    lastValue = valueSelector.getDouble();
  }

  @Override
  void setLastValue(Number lastValue)
  {
    this.lastValue = lastValue.doubleValue();
  }

  @Override
  public Object get()
  {
    return new SerializablePairLongDouble(lastTime, rhsNull ? null : lastValue);
  }

  @Override
  public float getFloat()
  {
    return (float) lastValue;
  }

  @Override
  public long getLong()
  {
    return (long) lastValue;
  }

  @Override
  public double getDouble()
  {
    return lastValue;
  }
}
