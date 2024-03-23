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
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.cell.NativeClearedByteBufferProvider;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;

public class SerializablePairLongDoubleComplexMetricSerde extends AbstractSerializableLongObjectPairSerde<SerializablePairLongDouble>
{
  public static final int EXPECTED_VERSION = 3;
  public static final String TYPE_NAME = "serializablePairLongDouble";

  private static final SerializablePairLongDoubleSimpleStagedSerde SERDE = new SerializablePairLongDoubleSimpleStagedSerde();

  private static final Comparator<SerializablePair<Long, Double>> COMPARATOR = SerializablePair.createNullHandlingComparator(
      Double::compare,
      true
  );

  public SerializablePairLongDoubleComplexMetricSerde()
  {
    super(SerializablePairLongDouble.class);
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public GenericColumnSerializer<SerializablePairLongDouble> getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return new SerializablePairLongDoubleColumnSerializer(
        segmentWriteOutMedium,
        NativeClearedByteBufferProvider.INSTANCE
    );
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
  {
    SerializablePairLongDoubleComplexColumn.Builder builder =
        new SerializablePairLongDoubleComplexColumn.Builder(buffer)
            .setByteBufferProvier(NativeClearedByteBufferProvider.INSTANCE);

    columnBuilder.setComplexColumnSupplier(builder::build);
  }

  @Override
  public ObjectStrategy<SerializablePairLongDouble> getObjectStrategy()
  {
    return new ObjectStrategy<SerializablePairLongDouble>()
    {
      @Override
      public int compare(SerializablePairLongDouble o1, SerializablePairLongDouble o2)
      {
        return COMPARATOR.compare(o1, o2);
      }

      @Override
      public Class<? extends SerializablePairLongDouble> getClazz()
      {
        return SerializablePairLongDouble.class;
      }

      @Override
      public SerializablePairLongDouble fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        ByteBuffer readOnlyByteBuffer = buffer.asReadOnlyBuffer().order(buffer.order());

        readOnlyByteBuffer.limit(buffer.position() + numBytes);

        return SERDE.deserialize(readOnlyByteBuffer);
      }

      @Override
      public byte[] toBytes(@Nullable SerializablePairLongDouble inPair)
      {
        return SERDE.serialize(inPair);
      }
    };
  }
}
