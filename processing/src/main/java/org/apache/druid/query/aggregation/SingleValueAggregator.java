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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class SingleValueAggregator implements Aggregator
{
  final ColumnValueSelector selector;
  @Nullable
  private Object value;
  private boolean isAggregateInvoked = false;

  public SingleValueAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    if (isAggregateInvoked) {
      throw InvalidInput.exception("Subquery expression returned more than one row");
    }
    value = selector.getObject();
    isAggregateInvoked = true;
  }

  @Override
  public Object get()
  {
    return value;
  }

  @Override
  public float getFloat()
  {
    assert validObjectValue();
    return (value == null) ? NullHandling.ZERO_FLOAT : ((Number) value).floatValue();
  }

  @Override
  public long getLong()
  {
    assert validObjectValue();
    return (value == null) ? NullHandling.ZERO_LONG : ((Number) value).longValue();
  }

  @Override
  public double getDouble()
  {
    assert validObjectValue();
    return (value == null) ? NullHandling.ZERO_DOUBLE : ((Number) value).doubleValue();
  }

  @Override
  public boolean isNull()
  {
    return NullHandling.sqlCompatible() && value == null;
  }

  private boolean validObjectValue()
  {
    return NullHandling.replaceWithDefault() || !isNull();
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
           "selector=" + selector +
           ", value=" + value +
           ", isAggregateInvoked=" + isAggregateInvoked +
           '}';
  }
}
