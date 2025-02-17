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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;

public class StringMaxAggregator implements Aggregator
{
  @Nullable
  public static String combineValues(@Nullable String lhs, @Nullable String rhs)
  {
    if (lhs == null) {
      return rhs;
    }
    if (rhs == null) {
      return lhs;
    }
    return lhs.compareTo(rhs) > 0 ? lhs : rhs;
  }

  @Nullable
  private String max;
  private final int maxStringBytes;
  private final BaseObjectColumnValueSelector<String> selector;

  public StringMaxAggregator(BaseObjectColumnValueSelector<String> selector, int maxStringBytes)
  {
    this.selector = selector;
    this.maxStringBytes = maxStringBytes;
    this.max = null; // Initialize with null, meaning no value yet
  }

  @Override
  public void aggregate()
  {
    String currentValue = selector.getObject();

    if (currentValue != null && currentValue.length() > maxStringBytes) {
      currentValue = StringUtils.fastLooseChop(currentValue, maxStringBytes);
    }

    max = combineValues(max, currentValue);
  }

  @Nullable
  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("StringMaxAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("StringMaxAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("StringMaxAggregator does not support getDouble()");
  }

  @Override
  public boolean isNull()
  {
    return max == null;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
