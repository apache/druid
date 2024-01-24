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
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *
 */
public class SingleValueBufferAggregator implements BufferAggregator
{
  final ColumnValueSelector selector;

  final ColumnType columnType;

  private int stringByteArrayLength = 0;

  private boolean isAggregateInvoked = false;

  SingleValueBufferAggregator(ColumnValueSelector selector, ColumnType columnType)
  {
    this.selector = selector;
    this.columnType = columnType;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.put(position, NullHandling.IS_NULL_BYTE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    if (isAggregateInvoked) {
      throw InvalidInput.exception("Single Value Aggregator would not be applied to more than one row");
    }
    boolean isNotNull;
    if (columnType.is(ValueType.STRING)) {
      isNotNull = (selector.getObject() != null);
    } else {
      isNotNull = !selector.isNull();
    }
    if (isNotNull) {
      if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
        buf.put(position, NullHandling.IS_NOT_NULL_BYTE);
      }
      updatevalue(buf, position + Byte.BYTES);
    }
    isAggregateInvoked = true;
  }

  private void updatevalue(ByteBuffer buf, int position)
  {
    if (columnType.is(ValueType.LONG)) {
      buf.putLong(position, selector.getLong());
    } else if (columnType.is(ValueType.FLOAT)) {
      buf.putFloat(position, selector.getFloat());
    } else if (columnType.is(ValueType.DOUBLE)) {
      buf.putDouble(position, selector.getDouble());
    } else if (columnType.is(ValueType.STRING)) {
      byte[] bytes = DimensionHandlerUtils.convertObjectToString(selector.getObject()).getBytes(StandardCharsets.UTF_8);
      int size = bytes.length;
      for (int ii = 0; ii < size; ++ii) {
        buf.put(position + ii * Integer.BYTES, bytes[ii]);
      }
      stringByteArrayLength = size;
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    if (columnType.is(ValueType.LONG)) {
      return getLong(buf, position);
    } else if (columnType.is(ValueType.FLOAT)) {
      return getFloat(buf, position);
    } else if (columnType.is(ValueType.DOUBLE)) {
      return getDouble(buf, position);
    } else if (columnType.is(ValueType.STRING)) {
      if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
        return null;
      }
      byte[] bytes = new byte[stringByteArrayLength];
      for (int ii = 0; ii < stringByteArrayLength; ++ii) {
        bytes[ii] = buf.get(position + Byte.BYTES + ii * Integer.BYTES);
      }
      return new String(bytes, StandardCharsets.UTF_8);
    }

    return null;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return float for Null Value");
    }
    return buf.getFloat(position + Byte.BYTES);
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return double for Null Value");
    }
    return buf.getDouble(position + Byte.BYTES);
  }


  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    if (buf.get(position) == NullHandling.IS_NULL_BYTE) {
      throw new IllegalStateException("Cannot return long for Null Value");
    }
    return buf.getLong(position + Byte.BYTES);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

}
