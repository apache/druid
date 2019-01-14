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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.first.StringFirstAggregatorFactory;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * The SerializablePairLongStringSerde serializes a Long-String pair (SerializablePairLongString).
 * The serialization structure is: Long:Integer:String
 * <p>
 * The class is used on first/last String aggregators to store the time and the first/last string.
 * Long:Integer:String -> Timestamp:StringSize:StringData
 */
public class SerializablePairLongStringSerde extends ComplexMetricSerde
{

  private static final String TYPE_NAME = "serializablePairLongString";

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<SerializablePairLongString> extractedClass()
      {
        return SerializablePairLongString.class;
      }

      @Override
      public Object extractValue(InputRow inputRow, String metricName)
      {
        return inputRow.getRaw(metricName);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
  {
    final GenericIndexed column = GenericIndexed.read(buffer, getObjectStrategy(), columnBuilder.getFileMapper());
    columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<SerializablePairLongString>()
    {
      @Override
      public int compare(@Nullable SerializablePairLongString o1, @Nullable SerializablePairLongString o2)
      {
        return StringFirstAggregatorFactory.VALUE_COMPARATOR.compare(o1, o2);
      }

      @Override
      public Class<? extends SerializablePairLongString> getClazz()
      {
        return SerializablePairLongString.class;
      }

      @Override
      public SerializablePairLongString fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();

        long lhs = readOnlyBuffer.getLong();
        int stringSize = readOnlyBuffer.getInt();

        String lastString = null;
        if (stringSize > 0) {
          byte[] stringBytes = new byte[stringSize];
          readOnlyBuffer.get(stringBytes, 0, stringSize);
          lastString = StringUtils.fromUtf8(stringBytes);
        }

        return new SerializablePairLongString(lhs, lastString);
      }

      @Override
      public byte[] toBytes(SerializablePairLongString val)
      {
        String rhsString = val.rhs;
        ByteBuffer bbuf;

        if (rhsString != null) {
          byte[] rhsBytes = StringUtils.toUtf8(rhsString);
          bbuf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + rhsBytes.length);
          bbuf.putLong(val.lhs);
          bbuf.putInt(Long.BYTES, rhsBytes.length);
          bbuf.position(Long.BYTES + Integer.BYTES);
          bbuf.put(rhsBytes);
        } else {
          bbuf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
          bbuf.putLong(val.lhs);
          bbuf.putInt(Long.BYTES, 0);
        }

        return bbuf.array();
      }
    };
  }

  @Override
  public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
  }
}
