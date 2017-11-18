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

package io.druid.query.aggregation.first;

import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.Aggregator;
import io.druid.segment.BaseLongColumnValueSelector;

public class LongFirstAggregator implements Aggregator
{

  private final BaseLongColumnValueSelector valueSelector;
  private final BaseLongColumnValueSelector timeSelector;

  protected long firstTime;
  protected long firstValue;

  public LongFirstAggregator(BaseLongColumnValueSelector timeSelector, BaseLongColumnValueSelector valueSelector)
  {
    this.valueSelector = valueSelector;
    this.timeSelector = timeSelector;

    reset();
  }

  @Override
  public void aggregate()
  {
    long time = timeSelector.getLong();
    if (time < firstTime) {
      firstTime = time;
      firstValue = valueSelector.getLong();
    }
  }

  @Override
  public void reset()
  {
    firstTime = Long.MAX_VALUE;
    firstValue = 0;
  }

  @Override
  public Object get()
  {
    return new SerializablePair<>(firstTime, firstValue);
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

  @Override
  public void close()
  {

  }
}
