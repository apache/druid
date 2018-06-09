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

package io.druid.query.aggregation;

import io.druid.data.input.InputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexColumnPartSupplier;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.nio.ByteBuffer;

/**
 * The SerializablePairLongStringSerde serializes a Long-String pair.
 * The serialization structure is: Long:Integer:String
 *
 * The class is used on first/last String aggregators to store the time and the first/last string.
 * Long:Integer:String -> Timestamp:StringSize:StringData
 */
public class SerializablePairLongStringSerde extends ComplexMetricSerde
{

  public static final String TYPE_NAME = "serializablePairLongString";

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
    columnBuilder.setComplexColumn(new ComplexColumnPartSupplier(getTypeName(), column));
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<SerializablePairLongString>()
    {
      @Override
      public int compare(SerializablePairLongString o1, SerializablePairLongString o2)
      {
        //TODO: DOCS
        int comparation = 0;

        if (o1.lhs > o2.lhs) {
          comparation = 1;
        } else if (o1.lhs < o2.lhs) {
          comparation = -1;
        }

        if (comparation == 0) {
          if (o1.rhs != null && o2.rhs != null) {
            if (o1.rhs.equals(o2.rhs)) {
              comparation = 0;
            } else {
              comparation = -1;
            }
          } else if (o1.rhs != null) {
            comparation = 1;
          } else if (o2.rhs != null) {
            comparation = -1;
          } else {
            comparation = 0;
          }
        }

        return comparation;
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

        Long lhs = readOnlyBuffer.getLong();
        Integer stringSize = readOnlyBuffer.getInt();

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
