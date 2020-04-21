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

package org.apache.druid.query.aggregation.any;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

public class StringAnyAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;
  private boolean isFound;
  private String foundValue;

  public StringAnyAggregator(BaseObjectColumnValueSelector valueSelector, int maxStringBytes)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.foundValue = null;
    this.isFound = false;
  }

  @Override
  public void aggregate()
  {
    if (!isFound) {
      final Object object = valueSelector.getObject();
      foundValue = DimensionHandlerUtils.convertObjectToString(object);
      if (foundValue != null && foundValue.length() > maxStringBytes) {
        foundValue = foundValue.substring(0, maxStringBytes);
      }
      isFound = true;
    }
  }

  @Override
  public Object get()
  {
    return StringUtils.chop(foundValue, maxStringBytes);
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException("StringAnyAggregator does not support getFloat()");
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException("StringAnyAggregator does not support getLong()");
  }

  @Override
  public double getDouble()
  {
    throw new UnsupportedOperationException("StringAnyAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no-op
  }
}
