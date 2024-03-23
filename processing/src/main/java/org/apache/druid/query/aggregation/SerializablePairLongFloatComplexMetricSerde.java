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

public class SerializablePairLongFloatComplexMetricSerde extends AbstractSerializableLongObjectPairSerde<SerializablePairLongFloat>
{
  public static final int EXPECTED_VERSION = 3;
  public static final String TYPE_NAME = "serializablePairLongFloat";

  private static final SerializablePairLongFloatSimpleStagedSerde SERDE = new SerializablePairLongFloatSimpleStagedSerde();

  private static final Comparator<SerializablePair<Long, Float>> COMPARATOR = SerializablePair.createNullHandlingComparator(
      Float::compare,
      true
  );

  public SerializablePairLongFloatComplexMetricSerde()
  {
    super(SerializablePairLongFloat.class);
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public GenericColumnSerializer<SerializablePairLongFloat> getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return new SerializablePairLongFloatColumnSerializer(
        segmentWriteOutMedium,
        NativeClearedByteBufferProvider.INSTANCE
    );
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
  {
    SerializablePairLongFloatComplexColumn.Builder builder =
        new SerializablePairLongFloatComplexColumn.Builder(buffer)
            .setByteBufferProvier(NativeClearedByteBufferProvider.INSTANCE);

    columnBuilder.setComplexColumnSupplier(builder::build);
  }


  @Override
  public ObjectStrategy<SerializablePairLongFloat> getObjectStrategy()
  {
    return new ObjectStrategy<SerializablePairLongFloat>()
    {
      @Override
      public int compare(SerializablePairLongFloat o1, SerializablePairLongFloat o2)
      {
        return COMPARATOR.compare(o1, o2);
      }

      @Override
      public Class<? extends SerializablePairLongFloat> getClazz()
      {
        return SerializablePairLongFloat.class;
      }

      @Override
      public SerializablePairLongFloat fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        ByteBuffer readOnlyByteBuffer = buffer.asReadOnlyBuffer().order(buffer.order());

        readOnlyByteBuffer.limit(buffer.position() + numBytes);

        return SERDE.deserialize(readOnlyByteBuffer);
      }

      @Override
      public byte[] toBytes(@Nullable SerializablePairLongFloat inPair)
      {
        return SERDE.serialize(inPair);
      }
    };
  }
}
