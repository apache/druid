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

public abstract class AbstractSerializablePairLongObjectBufferStore<T extends SerializablePair<Long, ?>>
{
  private final SerializedStorage<T> serializedStorage;

  long minValue = Long.MAX_VALUE;
  long maxValue = Long.MIN_VALUE;

  AbstractSerializablePairLongObjectBufferStore(SerializedStorage<T> serializedStorage)
  {
    this.serializedStorage = serializedStorage;
  }

  public void store(@Nullable T pairLongObject) throws IOException
  {
    if (pairLongObject != null && pairLongObject.lhs != null) {
      minValue = Math.min(minValue, pairLongObject.lhs);
      maxValue = Math.max(maxValue, pairLongObject.lhs);
    }

    serializedStorage.store(pairLongObject);
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
    AbstractSerializablePairLongObjectColumnHeader<T> columnHeader = createColumnHeader();
    AbstractSerializablePairLongObjectDeltaEncodedStagedSerde<T> deltaEncodedSerde = createDeltaEncodedSerde(columnHeader);

    // try-with-resources will call cellWriter.close() an extra time in the normal case, but it protects against
    // buffer leaking in the case of an exception (close() is idempotent). In the normal path, close() performs some
    // finalization of the CellWriter object. We want that object state finalized before creating the TransferredBuffer
    // as a point of good style (though strictly speaking, it works fine to pass it in before calling close since
    // TransferredBuffer does not do anything in the constructor with the object)
    try (CellWriter cellWriter = new CellWriter.Builder(segmentWriteOutMedium).setByteBufferProvider(byteBufferProvider)
                                                                              .build()) {
      try (IOIterator<T> bufferIterator = iterator()) {
        while (bufferIterator.hasNext()) {
          T pairLongObject = bufferIterator.next();
          byte[] serialized = deltaEncodedSerde.serialize(pairLongObject);

          cellWriter.write(serialized);
        }

        cellWriter.close();

        return new TransferredBuffer(cellWriter, columnHeader);
      }
    }
  }

  // 1. we have overflow in our range || 2. we have only seen null values
  // in this case, effectively disable delta encoding by using longs and a min value of 0
  // else we shoudl return columnHeader with delta encding enabled
  @Nonnull
  public abstract AbstractSerializablePairLongObjectColumnHeader<T> createColumnHeader();
  public abstract AbstractSerializablePairLongObjectDeltaEncodedStagedSerde<T> createDeltaEncodedSerde(AbstractSerializablePairLongObjectColumnHeader<T> columnHeader);

  public IOIterator<T> iterator() throws IOException
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
    private final AbstractSerializablePairLongObjectColumnHeader<?> columnHeader;

    public TransferredBuffer(
        CellWriter cellWriter,
        AbstractSerializablePairLongObjectColumnHeader<?> columnHeader
    )
    {
      this.cellWriter = cellWriter;
      this.columnHeader = columnHeader;
    }

    @Override
    public long getSerializedSize()
    {
      return columnHeader.getSerializedSize() + cellWriter.getSerializedSize();
    }

    @Override
    public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      columnHeader.transferTo(channel);
      cellWriter.writeTo(channel, smoosher);
    }
  }
}
