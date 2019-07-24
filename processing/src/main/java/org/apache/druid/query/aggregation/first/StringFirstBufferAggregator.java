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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

public class StringFirstBufferAggregator implements BufferAggregator
{
  private final BaseLongColumnValueSelector timeSelector;
  private final BaseObjectColumnValueSelector valueSelector;
  private final int maxStringBytes;

  public StringFirstBufferAggregator(
      BaseLongColumnValueSelector timeSelector,
      BaseObjectColumnValueSelector valueSelector,
      int maxStringBytes
  )
  {
    this.timeSelector = timeSelector;
    this.valueSelector = valueSelector;
    this.maxStringBytes = maxStringBytes;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putLong(position, Long.MAX_VALUE);
    buf.putInt(position + Long.BYTES, 0);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    Object value = valueSelector.getObject();

    long time = timeSelector.getLong();
    String firstString = null;

    if (value != null) {
      if (value instanceof SerializablePairLongString) {
        SerializablePairLongString serializablePair = (SerializablePairLongString) value;
        time = serializablePair.lhs;
        firstString = serializablePair.rhs;
      } else if (value instanceof String) {
        firstString = (String) value;
      } else {
        throw new ISE(
            "Try to aggregate unsuported class type [%s].Supported class types: String or SerializablePairLongString",
            value.getClass().getName()
        );
      }
    }

    long lastTime = mutationBuffer.getLong(position);

    if (time < lastTime) {
      if (firstString != null) {
        if (firstString.length() > maxStringBytes) {
          firstString = firstString.substring(0, maxStringBytes);
        }

        byte[] valueBytes = StringUtils.toUtf8(firstString);

        mutationBuffer.putLong(position, time);
        mutationBuffer.putInt(position + Long.BYTES, valueBytes.length);

        mutationBuffer.position(position + Long.BYTES + Integer.BYTES);
        mutationBuffer.put(valueBytes);
      } else {
        mutationBuffer.putLong(position, time);
        mutationBuffer.putInt(position + Long.BYTES, 0);
      }
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);

    Long timeValue = mutationBuffer.getLong(position);
    int stringSizeBytes = mutationBuffer.getInt(position + Long.BYTES);

    SerializablePairLongString serializablePair;

    if (stringSizeBytes > 0) {
      byte[] valueBytes = new byte[stringSizeBytes];
      mutationBuffer.position(position + Long.BYTES + Integer.BYTES);
      mutationBuffer.get(valueBytes, 0, stringSizeBytes);
      serializablePair = new SerializablePairLongString(timeValue, StringUtils.fromUtf8(valueBytes));
    } else {
      serializablePair = new SerializablePairLongString(timeValue, null);
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
