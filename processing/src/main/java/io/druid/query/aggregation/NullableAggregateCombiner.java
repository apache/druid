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
import io.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

/**
 * The result of a NullableAggregateCombiner will be null if all the values to be combined are null values or no values are combined at all.
 * If any of the value is non-null, the result would be the value of the delegate combiner.
 * Note that the delegate combiner is not required to perform check for {@link BaseNullableColumnValueSelector#isNull()} on the selector as only non-null values
 * will be passed to the delegate combiner.
 */
public class NullableAggregateCombiner implements AggregateCombiner
{
  private boolean isNullResult = true;

  private final AggregateCombiner delegate;

  public NullableAggregateCombiner(AggregateCombiner delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public void reset(ColumnValueSelector selector)
  {
    if (selector.isNull()) {
      isNullResult = true;
    } else {
      isNullResult = false;
      delegate.reset(selector);
    }
  }

  @Override
  public void fold(ColumnValueSelector selector)
  {
    boolean isNotNull = !selector.isNull();
    if (isNotNull) {
      if (isNullResult) {
        isNullResult = false;
        delegate.reset(selector);
      } else {
        delegate.fold(selector);
      }
    }
  }

  @Override
  public float getFloat()
  {
    if (isNullResult) {
      throw new IllegalStateException("Cannot return primitive float for Null Value");
    }
    return delegate.getFloat();
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
  public long getLong()
  {
    if (isNullResult) {
      throw new IllegalStateException("Cannot return long for Null Value");
    }
    return delegate.getLong();
  }

  @Override
  public boolean isNull()
  {
    return isNullResult || delegate.isNull();
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return isNullResult ? null : delegate.getObject();
  }

  @Override
  public Class classOfObject()
  {
    return delegate.classOfObject();
  }
}
