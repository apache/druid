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

package io.druid.query.aggregation.last;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.SerializablePairLongString;
import io.druid.segment.BaseLongColumnValueSelector;
import io.druid.segment.BaseObjectColumnValueSelector;

public class StringLastAggregator implements Aggregator
{

  private final BaseObjectColumnValueSelector valueSelector;
  private final BaseLongColumnValueSelector timeSelector;
  private final Integer maxStringBytes;

  protected long lastTime;
  protected String lastValue;

  public StringLastAggregator(
      BaseLongColumnValueSelector timeSelector,
      BaseObjectColumnValueSelector valueSelector,
      Integer maxStringBytes
  )
  {
    this.valueSelector = valueSelector;
    this.timeSelector = timeSelector;
    this.maxStringBytes = maxStringBytes;

    lastTime = Long.MIN_VALUE;
    lastValue = null;
  }

  @Override
  public void aggregate()
  {
    long time = timeSelector.getLong();
    if (time >= lastTime) {
      lastTime = time;
      Object value = valueSelector.getObject();

      if (value instanceof String) {
        lastValue = (String) value;
      } else if (value instanceof SerializablePairLongString) {
        lastValue = ((SerializablePairLongString) value).rhs;
      } else if (value != null) {
        throw new IllegalStateException(
            "Try to aggregate unsuported class type ["
            + value.getClass().getName() +
            "]. Supported class types: String or SerializablePairLongString"
        );
      }

      if (lastValue != null && lastValue.length() > maxStringBytes) {
        lastValue = lastValue.substring(0, maxStringBytes);
      }
    }
  }

  @Override
  public Object get()
  {
    return new SerializablePairLongString(lastTime, lastValue);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
