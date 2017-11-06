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

import io.druid.segment.BaseNullableColumnValueSelector;

import javax.annotation.Nullable;

/**
 * The result of a NullableAggregator will be null if all the values to be aggregated are null values or no values are aggregated at all.
 * If any of the value is non-null, the result would be the aggregated value of the delegate aggregator.
 * Note that the delegate aggregator is not required to perform check for isNull on the columnValueSelector as only non-null values
 * will be passed to the delegate aggregator.
 */
public class NullableAggregator implements Aggregator
{
  private final Aggregator delegate;
  private final BaseNullableColumnValueSelector selector;
  private boolean isNullResult = true;

  public NullableAggregator(Aggregator delegate, BaseNullableColumnValueSelector selector)
  {
    this.delegate = delegate;
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    boolean isNotNull = !selector.isNull();
    if (isNotNull) {
      if (isNullResult) {
        isNullResult = false;
      }
      delegate.aggregate();
    }
  }

  @Override
  public void reset()
  {
    isNullResult = true;
    delegate.reset();
  }

  @Override
  @Nullable
  public Object get()
  {
    if (isNullResult) {
      return null;
    }
    return delegate.get();
  }

  @Override
  public float getFloat()
  {
    return delegate.getFloat();
  }

  @Override
  public long getLong()
  {
    return delegate.getLong();
  }

  @Override
  public double getDouble()
  {
    return delegate.getDouble();
  }

  @Override
  public boolean isNull()
  {
    return isNullResult;
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
