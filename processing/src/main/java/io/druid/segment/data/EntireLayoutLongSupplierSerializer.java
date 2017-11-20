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

package io.druid.segment.data;

import io.druid.java.util.common.io.smoosh.FileSmoosher;
import io.druid.segment.serde.MetaSerdeHelper;
import io.druid.segment.writeout.SegmentWriteOutMedium;
import io.druid.segment.writeout.WriteOutBytes;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;

public class EntireLayoutLongSupplierSerializer implements LongSupplierSerializer
{
  private static final MetaSerdeHelper<EntireLayoutLongSupplierSerializer> metaSerdeHelper = MetaSerdeHelper
      .firstWriteByte((EntireLayoutLongSupplierSerializer x) -> CompressedLongsIndexedSupplier.VERSION)
      .writeInt(x -> x.numInserted)
      .writeInt(x -> 0)
      .writeSomething(CompressionFactory.longEncodingWriter(x -> x.writer, x -> CompressionStrategy.NONE));

  private final CompressionFactory.LongEncodingWriter writer;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private WriteOutBytes valuesOut;

  private int numInserted = 0;

  EntireLayoutLongSupplierSerializer(
      SegmentWriteOutMedium segmentWriteOutMedium,
      CompressionFactory.LongEncodingWriter writer
  )
  {
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.writer = writer;
  }

  @Override
  public void open() throws IOException
  {
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
    writer.setOutputStream(valuesOut);
  }

  @Override
  public int size()
  {
    return numInserted;
  }

  @Override
  public void add(long value) throws IOException
  {
    writer.write(value);
    ++numInserted;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    writer.flush();
    return metaSerdeHelper.size(this) + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writer.flush();
    metaSerdeHelper.writeTo(channel, this);
    valuesOut.writeTo(channel);
  }
}
