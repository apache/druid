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

import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.segment.BaseNullableColumnValueSelector;

import javax.annotation.Nullable;

/**
 * The result of a NullableAggregator will be null if all the values to be aggregated are null values
 * or no values are aggregated at all. If any of the value is non-null, the result would be the aggregated
 * value of the delegate aggregator. Note that the delegate aggregator is not required to perform check for
 * {@link BaseNullableColumnValueSelector#isNull()} on the selector as only non-null values will be passed
 * to the delegate aggregator. This class is only used when SQL compatible null handling is enabled.
 */
@PublicApi
public final class NullableAggregator implements Aggregator
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
    if (isNullResult) {
      throw new IllegalStateException("Cannot return float for Null Value");
    }
    return delegate.getFloat();
  }

  @Override
  public long getLong()
  {
    if (isNullResult) {
      throw new IllegalStateException("Cannot return long for Null Value");
    }
    return delegate.getLong();
  }

  @Override
  public double getDouble()
  {
    if (isNullResult) {
      throw new IllegalStateException("Cannot return double for Null Value");
    }
    return delegate.getDouble();
  }

  @Override
  public boolean isNull()
  {
    return isNullResult || delegate.isNull();
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
