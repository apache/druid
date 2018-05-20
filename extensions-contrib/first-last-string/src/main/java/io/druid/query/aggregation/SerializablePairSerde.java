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

import io.druid.collections.SerializablePair;
import io.druid.data.input.InputRow;
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
import java.nio.charset.StandardCharsets;

public class SerializablePairSerde extends ComplexMetricSerde
{

  public SerializablePairSerde()
  {

  }

  @Override
  public String getTypeName()
  {
    return "serializablePairLongString";
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    return new ComplexMetricExtractor()
    {
      @Override
      public Class<SerializablePair> extractedClass()
      {
        return SerializablePair.class;
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
    return new ObjectStrategy<SerializablePair>()
    {
      @Override
      public int compare(SerializablePair o1, SerializablePair o2)
      {
        return o1.lhs.equals(o2.lhs) && o1.rhs.equals(o2.rhs) ? 1 : 0;
      }

      @Override
      public Class<? extends SerializablePair> getClazz()
      {
        return SerializablePair.class;
      }

      @Override
      public SerializablePair<Long, String> fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();

        Long lhs = readOnlyBuffer.getLong();
        Integer stringSize = readOnlyBuffer.getInt();

        byte[] stringBytes = new byte[stringSize];
        readOnlyBuffer.get(stringBytes, 0, stringSize);

        return new SerializablePair<>(lhs, new String(stringBytes, StandardCharsets.UTF_8));
      }

      @Override
      public byte[] toBytes(SerializablePair val)
      {
        String rhsString = (String) val.rhs;

        ByteBuffer bbuf = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + rhsString.length());
        bbuf.putLong((Long) val.lhs);
        bbuf.putInt(Long.BYTES, rhsString.length());
        bbuf.position(Long.BYTES + Integer.BYTES);
        bbuf.put(rhsString.getBytes(StandardCharsets.UTF_8));

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
