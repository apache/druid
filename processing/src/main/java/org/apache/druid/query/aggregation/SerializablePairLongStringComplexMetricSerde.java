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

import org.apache.druid.collections.SerializablePair;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.cell.NativeClearedByteBufferProvider;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * The SerializablePairLongStringSerde serializes a Long-String pair (SerializablePairLongString).
 * The serialization structure is: Long:Integer:String
 * The Long is delta-encoded for the column in order to potentially reduce the size to an integer so it may be stored
 * as: Integer:Integer:String
 * <p>
 * Future work: dictionary encoding of the String may be performed
 * <p>
 * The class is used on first/last String aggregators to store the time and the first/last string.
 * [Integer|Long]:Integer:String -> delta:StringSize:StringData --(delta decoded)--> TimeStamp:StringSize:StringData
 * (see {@link SerializablePairLongStringDeltaEncodedStagedSerde )}
 */
public class SerializablePairLongStringComplexMetricSerde extends ComplexMetricSerde
{
  public static final int EXPECTED_VERSION = 3;
  public static final String TYPE_NAME = "serializablePairLongString";
  // Null SerializablePairLongString values are put first
  private static final Comparator<SerializablePairLongString> COMPARATOR = Comparator.nullsFirst(
      // assumes that the LHS of the pair will never be null
      Comparator.<SerializablePairLongString>comparingLong(SerializablePair::getLhs)
                .thenComparing(SerializablePair::getRhs, Comparator.nullsFirst(Comparator.naturalOrder()))
  );

  private static final SerializablePairLongStringSimpleStagedSerde SERDE =
      new SerializablePairLongStringSimpleStagedSerde();

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor<?> getExtractor()
  {
    return new ComplexMetricExtractor<Object>()
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
    byte version = buffer.get(buffer.position());

    if (version == 0 || version == 1 || version == 2) {
      GenericIndexed<?> column = GenericIndexed.read(buffer, getObjectStrategy(), columnBuilder.getFileMapper());
      columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
    } else {
      SerializablePairLongStringComplexColumn.Builder builder =
          new SerializablePairLongStringComplexColumn.Builder(buffer)
              .setByteBufferProvider(NativeClearedByteBufferProvider.INSTANCE);
      columnBuilder.setComplexColumnSupplier(builder::build);
    }
  }

  @Override
  public ObjectStrategy<?> getObjectStrategy()
  {
    return new ObjectStrategy<SerializablePairLongString>()
    {
      @Override
      public int compare(@Nullable SerializablePairLongString o1, @Nullable SerializablePairLongString o2)
      {
        return COMPARATOR.compare(o1, o2);
      }

      @Override
      public Class<? extends SerializablePairLongString> getClazz()
      {
        return SerializablePairLongString.class;
      }

      @Override
      public SerializablePairLongString fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        ByteBuffer readOnlyByteBuffer = buffer.asReadOnlyBuffer().order(buffer.order());

        readOnlyByteBuffer.limit(buffer.position() + numBytes);

        return SERDE.deserialize(readOnlyByteBuffer);
      }

      @SuppressWarnings("NullableProblems")
      @Override
      public byte[] toBytes(SerializablePairLongString val)
      {
        return SERDE.serialize(val);
      }
    };
  }

  @Override
  public GenericColumnSerializer<?> getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return new SerializablePairLongStringColumnSerializer(
        segmentWriteOutMedium,
        NativeClearedByteBufferProvider.INSTANCE
    );
  }
}
