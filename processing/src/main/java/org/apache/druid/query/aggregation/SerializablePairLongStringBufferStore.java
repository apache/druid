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

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.serde.cell.ByteBufferProvider;
import org.apache.druid.segment.serde.cell.CellWriter;
import org.apache.druid.segment.serde.cell.IOIterator;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class SerializablePairLongStringBufferStore
{
  private final SerializedStorage<SerializablePairLongString> serializedStorage;

  private long minValue = Long.MAX_VALUE;
  private long maxValue = Long.MIN_VALUE;

  public SerializablePairLongStringBufferStore(SerializedStorage<SerializablePairLongString> serializedStorage)
  {
    this.serializedStorage = serializedStorage;
  }

  public void store(@Nullable SerializablePairLongString pairLongString) throws IOException
  {
    if (pairLongString != null && pairLongString.lhs != null) {
      minValue = Math.min(minValue, pairLongString.lhs);
      maxValue = Math.max(maxValue, pairLongString.lhs);
    }

    serializedStorage.store(pairLongString);
  }

  /**
   * each call transfers the temporary buffer into an encoded, block-compessed buffer of the segment. It is ready to be
   * transferred to a {@link WritableByteChannel}
   *
   * @param byteBufferProvider    - provides a ByteBuffer used for block compressed encoding
   * @param segmentWriteOutMedium - used to create temporary storage
   * @return encoded buffer ready to be stored
   * @throws IOException
   */
  public TransferredBuffer transferToRowWriter(
      ByteBufferProvider byteBufferProvider,
      SegmentWriteOutMedium segmentWriteOutMedium
  ) throws IOException
  {
    SerializablePairLongStringColumnHeader columnHeader = createColumnHeader();
    SerializablePairLongStringDeltaEncodedStagedSerde serde =
        new SerializablePairLongStringDeltaEncodedStagedSerde(
            columnHeader.getMinValue(),
            columnHeader.isUseIntegerDeltas()
        );

    // try-with-resources will call cellWriter.close() an extra time in the normal case, but it protects against
    // buffer leaking in the case of an exception (close() is idempotent). In the normal path, close() performs some
    // finalization of the CellWriter object. We want that object state finalized before creating the TransferredBuffer
    // as a point of good style (though strictly speaking, it works fine to pass it in before calling close since
    // TransferredBuffer does not do anything in the constructor with the object)
    try (CellWriter cellWriter =
             new CellWriter.Builder(segmentWriteOutMedium).setByteBufferProvider(byteBufferProvider).build()) {
      try (IOIterator<SerializablePairLongString> bufferIterator = iterator()) {
        while (bufferIterator.hasNext()) {
          SerializablePairLongString pairLongString = bufferIterator.next();
          byte[] serialized = serde.serialize(pairLongString);

          cellWriter.write(serialized);
        }

        cellWriter.close();

        return new TransferredBuffer(cellWriter, columnHeader);
      }
    }
  }

  @Nonnull
  public SerializablePairLongStringColumnHeader createColumnHeader()
  {
    long maxDelta = maxValue - minValue;
    SerializablePairLongStringColumnHeader columnHeader;

    if (minValue < maxValue && maxDelta < 0 || minValue > maxValue) {
      // true iff
      // 1. we have overflow in our range || 2. we have only seen null values
      // in this case, effectively disable delta encoding by using longs and a min value of 0
      maxDelta = Long.MAX_VALUE;
      minValue = 0;
    }

    if (maxDelta <= Integer.MAX_VALUE) {
      columnHeader = new SerializablePairLongStringColumnHeader(
          SerializablePairLongStringComplexMetricSerde.EXPECTED_VERSION,
          true,
          minValue
      );
    } else {
      columnHeader = new SerializablePairLongStringColumnHeader(
          SerializablePairLongStringComplexMetricSerde.EXPECTED_VERSION,
          false,
          minValue
      );
    }
    return columnHeader;
  }

  public IOIterator<SerializablePairLongString> iterator() throws IOException
  {
    return serializedStorage.iterator();
  }

  /**
   * contains serialized data that is compressed and delta-encoded (Long)
   * It's ready to be transferred to a {@link WritableByteChannel}
   */
  public static class TransferredBuffer implements Serializer
  {
    private final CellWriter cellWriter;
    private final SerializablePairLongStringColumnHeader columnHeader;

    public TransferredBuffer(CellWriter cellWriter, SerializablePairLongStringColumnHeader columnHeader)
    {
      this.cellWriter = cellWriter;
      this.columnHeader = columnHeader;
    }

    @Override
    public void writeTo(WritableByteChannel channel, @Nullable FileSmoosher smoosher) throws IOException
    {
      columnHeader.transferTo(channel);
      cellWriter.writeTo(channel, smoosher);
    }

    @Override
    public long getSerializedSize()
    {
      return columnHeader.getSerializedSize() + cellWriter.getSerializedSize();
    }
  }
}
