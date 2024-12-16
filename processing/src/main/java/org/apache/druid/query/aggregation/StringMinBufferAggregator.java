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
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringMinBufferAggregator implements BufferAggregator
{
  private final BaseObjectColumnValueSelector<String> selector;
  private final int maxStringBytes;

  public StringMinBufferAggregator(BaseObjectColumnValueSelector<String> selector, int maxStringBytes)
  {
    this.selector = selector;
    this.maxStringBytes = maxStringBytes;
  }

  private static final int FOUND_AND_NULL_FLAG_VALUE = -1;
  private static final int NOT_FOUND_FLAG_VALUE = -2;

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putInt(position, NOT_FOUND_FLAG_VALUE);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    String currentValue = selector.getObject();

    if (buf.getInt(position) == NOT_FOUND_FLAG_VALUE) {
      if (currentValue == null) {
        buf.putInt(position, FOUND_AND_NULL_FLAG_VALUE);
      } else {
        setMinStringInBuffer(buf, position, currentValue);
      }
      return;
    }

    // Retrieve the current minimum from the buffer
    String currentMinValue = getMinStringInBuffer(buf, position);

    // Update the buffer with the new minimum if the current value is smaller
    if (currentValue != null && (currentMinValue == null || currentValue.compareTo(currentMinValue) < 0)) {
      setMinStringInBuffer(buf, position, currentValue);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return getMinStringInBuffer(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringMinBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringMinBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringMinBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // No resources to clean up
  }

  @Nullable
  private String getMinStringInBuffer(ByteBuffer buf, int position)
  {
    int length = buf.getInt(position);
    if (length == FOUND_AND_NULL_FLAG_VALUE) {
      return null;
    }

    byte[] stringBytes = new byte[length];
    buf.position(position + Integer.BYTES);
    buf.get(stringBytes);
    buf.position(position); // Reset buffer position to avoid side effects.

    return StringUtils.fromUtf8(stringBytes);
  }

  private void setMinStringInBuffer(ByteBuffer buf, int position, String value)
  {
    byte[] stringBytes = value.getBytes(StandardCharsets.UTF_8);
    int length = Math.min(stringBytes.length, maxStringBytes);

    buf.putInt(position, length);
    buf.position(position + Integer.BYTES);
    buf.put(stringBytes, 0, length);
    buf.position(position); // Reset buffer position to avoid side effects.
  }

  @Override
  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    int length = oldBuffer.getInt(oldPosition);
    newBuffer.putInt(newPosition, length);

    if (length == FOUND_AND_NULL_FLAG_VALUE || length == NOT_FOUND_FLAG_VALUE) {
      return;
    }

    byte[] stringBytes = new byte[length];

    oldBuffer.position(oldPosition + Integer.BYTES);
    oldBuffer.get(stringBytes);

    newBuffer.position(newPosition + Integer.BYTES);
    newBuffer.put(stringBytes);

    // Reset buffer positions to avoid side effects
    oldBuffer.position(oldPosition);
    newBuffer.position(newPosition);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
