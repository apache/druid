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

public class SerializablePairLongLongComplexMetricSerde extends AbstractSerializableLongObjectPairSerde<SerializablePairLongLong>
{
  public static final int EXPECTED_VERSION = 3;
  public static final String TYPE_NAME = "serializablePairLongLong";

  private static final SerializablePairLongLongSimpleStagedSerde SERDE = new SerializablePairLongLongSimpleStagedSerde();

  private static final Comparator<SerializablePair<Long, Long>> COMPARATOR = SerializablePair.createNullHandlingComparator(
      Long::compare,
      true
  );

  public SerializablePairLongLongComplexMetricSerde()
  {
    super(SerializablePairLongLong.class);
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public GenericColumnSerializer<SerializablePairLongLong> getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column)
  {
    return new SerializablePairLongLongColumnSerializer(
        segmentWriteOutMedium,
        NativeClearedByteBufferProvider.INSTANCE
    );
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder columnBuilder)
  {
    SerializablePairLongLongComplexColumn.Builder builder =
        new SerializablePairLongLongComplexColumn.Builder(buffer)
            .setByteBufferProvier(NativeClearedByteBufferProvider.INSTANCE);

    columnBuilder.setComplexColumnSupplier(builder::build);
  }

  @Override
  public ObjectStrategy<SerializablePairLongLong> getObjectStrategy()
  {
    return new ObjectStrategy<SerializablePairLongLong>()
    {
      @Override
      public int compare(SerializablePairLongLong o1, SerializablePairLongLong o2)
      {
        return COMPARATOR.compare(o1, o2);
      }

      @Override
      public Class<? extends SerializablePairLongLong> getClazz()
      {
        return SerializablePairLongLong.class;
      }

      @Override
      public SerializablePairLongLong fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        ByteBuffer readOnlyByteBuffer = buffer.asReadOnlyBuffer().order(buffer.order());

        readOnlyByteBuffer.limit(buffer.position() + numBytes);

        return SERDE.deserialize(readOnlyByteBuffer);
      }

      @Override
      public byte[] toBytes(@Nullable SerializablePairLongLong inPair)
      {
        return SERDE.serialize(inPair);
      }
    };
  }
}
