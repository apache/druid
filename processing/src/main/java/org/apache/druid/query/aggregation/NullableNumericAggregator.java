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
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;

/**
 * Null-aware numeric {@link Aggregator}.
 *
 * The result of this aggregator will be null if all the values to be aggregated are null values or no values are
 * aggregated at all. If any of the values are non-null, the result will be the aggregated value of the delegate
 * aggregator.
 *
 * When wrapped by this class, the underlying aggregator's required storage space is increased by one byte. The extra
 * byte is a boolean that stores whether or not any non-null values have been seen. The extra byte is placed before
 * the underlying aggregator's normal state. (Buffer layout = [nullability byte] [delegate storage bytes])
 *
 * Used by {@link NullableNumericAggregatorFactory#factorize(ColumnSelectorFactory)} to wrap non-null aware
 * aggregators. This class is only used when SQL compatible null handling is enabled.
 *
 * @see NullableNumericAggregatorFactory#factorize(ColumnSelectorFactory)
 */
@PublicApi
public final class NullableNumericAggregator implements Aggregator
{
  private final Aggregator delegate;
  private final BaseNullableColumnValueSelector selector;
  private boolean isNullResult = true;

  public NullableNumericAggregator(Aggregator delegate, BaseNullableColumnValueSelector selector)
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
