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

import org.apache.druid.query.filter.ValueMatcher;

import javax.annotation.Nullable;

public class FilteredAggregator implements Aggregator
{
  private final ValueMatcher matcher;
  private final Aggregator delegate;
  @Nullable
  private final Number elseValue;
  private boolean hasUnmatchedRow;

  public FilteredAggregator(ValueMatcher matcher, Aggregator delegate, @Nullable Number elseValue)
  {
    this.matcher = matcher;
    this.delegate = delegate;
    this.elseValue = elseValue;
  }

  @Override
  public void aggregate()
  {
    if (matcher.matches(false)) {
      delegate.aggregate();
    } else if (elseValue != null) {
      hasUnmatchedRow = true;
    }
  }

  @Override
  public boolean isNull()
  {
    return delegate.isNull() && (elseValue == null || !hasUnmatchedRow);
  }

  @Override
  @Nullable
  public Object get()
  {
    if (elseValue != null && hasUnmatchedRow && delegate.isNull()) {
      return elseValue;
    } else {
      return delegate.get();
    }
  }

  @Override
  public float getFloat()
  {
    if (elseValue != null && hasUnmatchedRow && delegate.isNull()) {
      return elseValue.floatValue();
    } else {
      return delegate.getFloat();
    }
  }

  @Override
  public long getLong()
  {
    if (elseValue != null && hasUnmatchedRow && delegate.isNull()) {
      return elseValue.longValue();
    } else {
      return delegate.getLong();
    }
  }

  @Override
  public double getDouble()
  {
    if (elseValue != null && hasUnmatchedRow && delegate.isNull()) {
      return elseValue.doubleValue();
    } else {
      return delegate.getDouble();
    }
  }

  @Override
  public void close()
  {
    delegate.close();
  }
}
