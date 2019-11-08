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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;

public class StringFirstAggregator implements Aggregator
{
  @Nullable
  private final BaseLongColumnValueSelector timeSelector;
  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;

  protected long firstTime;
  protected String firstValue;

  public StringFirstAggregator(
      @Nullable BaseLongColumnValueSelector timeSelector,
      BaseObjectColumnValueSelector valueSelector,
      int maxStringBytes
  )
  {
    this.valueSelector = valueSelector;
    this.timeSelector = timeSelector;
    this.maxStringBytes = maxStringBytes;

    firstTime = DateTimes.MAX.getMillis();
    firstValue = null;
  }

  @Override
  public void aggregate()
  {
    final SerializablePairLongString inPair = StringFirstLastUtils.readPairFromSelectors(
        timeSelector,
        valueSelector
    );

    if (inPair != null && inPair.rhs != null && inPair.lhs < firstTime) {
      firstTime = inPair.lhs;
      firstValue = inPair.rhs;

      if (firstValue.length() > maxStringBytes) {
        firstValue = firstValue.substring(0, maxStringBytes);
      }
    }
  }

  @Override
  public Object get()
  {
    return new SerializablePairLongString(firstTime, StringFirstLastUtils.chop(firstValue, maxStringBytes));
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
