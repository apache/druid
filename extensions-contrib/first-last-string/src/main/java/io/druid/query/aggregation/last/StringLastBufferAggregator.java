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

package io.druid.query.aggregation.last;

import io.druid.collections.SerializablePair;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.BaseLongColumnValueSelector;
import io.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class StringLastBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector timeSelector;
  private final BaseObjectColumnValueSelector valueSelector;
  private final Integer maxStringBytes;

  public StringLastBufferAggregator(
      BaseLongColumnValueSelector timeSelector,
      BaseObjectColumnValueSelector valueSelector,
      Integer maxStringBytes
  )
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    mutationBuffer.putLong(position, Long.MIN_VALUE);
    mutationBuffer.putInt(position + Long.BYTES, 0);
    for (int i = 0; i < maxStringBytes - 1; i++) {
      mutationBuffer.putChar(position + Long.BYTES + Integer.BYTES + i, '\0');
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    Object value = valueSelector.getObject();

    long time;
    String lastString;

    if (value instanceof SerializablePair) {
      SerializablePair<Long, String> serializablePair = (SerializablePair) value;
      time = serializablePair.lhs;
      lastString = serializablePair.rhs;
    } else {
      time = timeSelector.getLong();
      lastString = (String) value;
    }

    long lastTime = mutationBuffer.getLong(position);
    if (time >= lastTime) {
      byte[] valueBytes = lastString.getBytes(StandardCharsets.UTF_8);

      mutationBuffer.putLong(position, time);
      mutationBuffer.putInt(position + Long.BYTES, valueBytes.length);

      mutationBuffer.position(position + Long.BYTES + Integer.BYTES);
      mutationBuffer.put(valueBytes);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    Long timeValue = mutationBuffer.getLong(position);
    Integer stringSizeBytes = mutationBuffer.getInt(position + Long.BYTES);

    SerializablePair<Long, String> serializablePair;

    if (stringSizeBytes > 0) {
      byte[] valueBytes = new byte[stringSizeBytes];
      mutationBuffer.position(position + Long.BYTES + Integer.BYTES);
      mutationBuffer.get(valueBytes, 0, stringSizeBytes);
      serializablePair = new SerializablePair<>(timeValue, new String(valueBytes, StandardCharsets.UTF_8));
    } else {
      serializablePair = new SerializablePair<>(timeValue, null);
    }

    return serializablePair;
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("StringFirstAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("timeSelector", timeSelector);
    inspector.visit("valueSelector", valueSelector);
  }
}
