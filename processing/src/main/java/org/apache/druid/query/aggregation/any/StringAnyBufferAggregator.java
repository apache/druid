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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

import java.nio.ByteBuffer;
import java.util.List;

public class StringAnyBufferAggregator implements BufferAggregator
{
  private static final int FOUND_AND_NULL_FLAG_VALUE = -1;
  private static final int NOT_FOUND_FLAG_VALUE = -2;
  private static final int FOUND_VALUE_OFFSET = Integer.BYTES;

  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;
  private final boolean aggregateMultipleValues;

  public StringAnyBufferAggregator(BaseObjectColumnValueSelector valueSelector, int maxStringBytes, boolean aggregateMultipleValues)
  {
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
    this.aggregateMultipleValues = aggregateMultipleValues;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putInt(position, NOT_FOUND_FLAG_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (buf.getInt(position) == NOT_FOUND_FLAG_VALUE) {
      String foundValue = readValue(valueSelector.getObject());
      if (foundValue != null) {
        ByteBuffer mutationBuffer = buf.duplicate();
        mutationBuffer.position(position + FOUND_VALUE_OFFSET);
        mutationBuffer.limit(position + FOUND_VALUE_OFFSET + maxStringBytes);
        final int len = StringUtils.toUtf8WithLimit(foundValue, mutationBuffer);
        mutationBuffer.putInt(position, len);
      } else {
        buf.putInt(position, FOUND_AND_NULL_FLAG_VALUE);
      }
    }
  }

  private String readValue(Object object)
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
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer copyBuffer = buf.duplicate();
    copyBuffer.position(position);
    int stringSizeBytes = copyBuffer.getInt();
    if (stringSizeBytes >= 0) {
      byte[] valueBytes = new byte[stringSizeBytes];
      copyBuffer.get(valueBytes, 0, stringSizeBytes);
      return StringUtils.fromUtf8(valueBytes);
    } else {
      return null;
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringAnyBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no-op
  }
}
