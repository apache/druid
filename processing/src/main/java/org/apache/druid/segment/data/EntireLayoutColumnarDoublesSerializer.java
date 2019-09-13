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

package org.apache.druid.segment.data;

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Serializer that produces {@link EntireLayoutColumnarDoublesSupplier.EntireLayoutColumnarDoubles}.
 */
public class EntireLayoutColumnarDoublesSerializer implements ColumnarDoublesSerializer
{
  private static final MetaSerdeHelper<EntireLayoutColumnarDoublesSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((EntireLayoutColumnarDoublesSerializer x) -> CompressedColumnarDoublesSuppliers.VERSION)
      .writeInt(x -> x.numInserted)
      .writeInt(x -> 0)
      .writeByte(x -> CompressionStrategy.NONE.getId());

  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final ByteBuffer orderBuffer;
  private WriteOutBytes valuesOut;

  private int numInserted = 0;

  public EntireLayoutColumnarDoublesSerializer(SegmentWriteOutMedium segmentWriteOutMedium, ByteOrder order)
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.orderBuffer = ByteBuffer.allocate(Double.BYTES);
    orderBuffer.order(order);
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
  }

  @Override
  public void add(double value) throws IOException
  {
    orderBuffer.rewind();
    orderBuffer.putDouble(value);
    valuesOut.write(orderBuffer.array());
    ++numInserted;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return META_SERDE_HELPER.size(this) + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    META_SERDE_HELPER.writeTo(channel, this);
    valuesOut.writeTo(channel);
  }
}
