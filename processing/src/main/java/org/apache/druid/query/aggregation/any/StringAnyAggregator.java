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

import java.util.List;

public class StringAnyAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;
  private boolean isFound;
  private String foundValue;
  private final boolean aggregateMultipleValues;

  public StringAnyAggregator(BaseObjectColumnValueSelector valueSelector, int maxStringBytes, boolean aggregateMultipleValues)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.foundValue = null;
    this.isFound = false;
    this.aggregateMultipleValues = aggregateMultipleValues;
  }

  @Override
  public void aggregate()
  {
    if (!isFound) {
      final Object object = valueSelector.getObject();
      foundValue = StringUtils.fastLooseChop(readValue(object), maxStringBytes);
      isFound = true;
    }
  }

  private String readValue(final Object object)
  {
    if (object == null) {
      return null;
    }
    if (object instanceof List) {
      List<Object> objectList = (List) object;
      if (objectList.size() == 0) {
        return null;
      }
      if (objectList.size() == 1) {
        return DimensionHandlerUtils.convertObjectToString(objectList.get(0));
      }
      if (aggregateMultipleValues) {
        return DimensionHandlerUtils.convertObjectToString(objectList);
      }
      return DimensionHandlerUtils.convertObjectToString(objectList.get(0));
    }
    return DimensionHandlerUtils.convertObjectToString(object);
  }

  @Override
  public Object get()
  {
    return foundValue;
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
