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

import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

/**
 *
 */
public class SingleValueAggregator implements Aggregator
{
  final ColumnValueSelector selector;

  @Nullable
  Object value;

  private boolean isNullResult = true;

  private boolean isAggregateInvoked = false;

  public SingleValueAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
    isNullResult = selector.isNull();
    if (!isNullResult) {
      this.value = selector.getObject();
    }
  }

  @Override
  public void aggregate()
  {
    if (isAggregateInvoked) {
      throw InvalidInput.exception("Single Value Aggregator would not be applied to more than one row..");
    }
    boolean isNotNull = !selector.isNull();
    if (isNotNull && isNullResult) {
      isNullResult = false;
    }
    isAggregateInvoked = true;
  }

  @Override
  public Object get()
  {
    if (isNullResult) {
      return null;
    }
    return value;
  }

  @Override
  public float getFloat()
  {
    if (isNullResult) {
      throw new IllegalStateException("Cannot return float for Null Value");
    }
    return (float) value;
  }

  @Override
  public long getLong()
  {
    if (isNullResult) {
      throw new IllegalStateException("Cannot return long for Null Value");
    }
    return (long) value;
  }

  @Override
  public double getDouble()
  {
    if (isNullResult) {
      throw new IllegalStateException("Cannot return double for Null Value");
    }
    return (double) value;
  }

  @Override
  public boolean isNull()
  {
    return isNullResult;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public String toString()
  {
    return "SingleValueAggregator{" +
           "valueSelector=" + selector +
           ", value=" + value +
           '}';
  }
}
