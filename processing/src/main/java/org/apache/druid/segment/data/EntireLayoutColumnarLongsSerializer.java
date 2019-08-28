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
import java.nio.channels.WritableByteChannel;

/**
 * Serializer that produces {@link EntireLayoutColumnarLongsSupplier.EntireLayoutColumnarLongs}.
 */
public class EntireLayoutColumnarLongsSerializer implements ColumnarLongsSerializer
{
  private static final MetaSerdeHelper<EntireLayoutColumnarLongsSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((EntireLayoutColumnarLongsSerializer x) -> CompressedColumnarLongsSupplier.VERSION)
      .writeInt(x -> x.numInserted)
      .writeInt(x -> 0)
      .writeSomething(CompressionFactory.longEncodingWriter(x -> x.writer, x -> CompressionStrategy.NONE));

  private final CompressionFactory.LongEncodingWriter writer;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private WriteOutBytes valuesOut;

  private int numInserted = 0;

  EntireLayoutColumnarLongsSerializer(
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
    return META_SERDE_HELPER.size(this) + valuesOut.size();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    writer.flush();
    META_SERDE_HELPER.writeTo(channel, this);
    valuesOut.writeTo(channel);
  }
}
