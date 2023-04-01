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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.V3CompressedVSizeColumnarMultiIntsSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Locale;

/**
 * Column Serializer that understands converting CompressedBigDecimal to 4 byte long values for better storage.
 */
public class CompressedBigDecimalLongColumnSerializer implements GenericColumnSerializer<CompressedBigDecimal>
{
  private static final byte VERSION = CompressedBigDecimalColumnPartSupplier.VERSION;

  /**
   * Static constructor.
   *
   * @param segmentWriteOutMedium the peon
   * @param filenameBase          filename of the index
   * @return a constructed AccumulatingBigDecimalLongColumnSerializer
   */
  public static CompressedBigDecimalLongColumnSerializer create(
      SegmentWriteOutMedium segmentWriteOutMedium,
      String filenameBase)
  {
    return new CompressedBigDecimalLongColumnSerializer(
        CompressedVSizeColumnarIntsSerializer.create(
            "dummy",
            segmentWriteOutMedium,
            String.format(Locale.ROOT, "%s.scale", filenameBase),
            16,
            CompressionStrategy.LZ4),
        V3CompressedVSizeColumnarMultiIntsSerializer.create(
            "dummy",
            segmentWriteOutMedium,
            String.format(Locale.ROOT, "%s.magnitude", filenameBase),
            Integer.MAX_VALUE,
            CompressionStrategy.LZ4));
  }

  private final CompressedVSizeColumnarIntsSerializer scaleWriter;
  private final V3CompressedVSizeColumnarMultiIntsSerializer magnitudeWriter;

  /**
   * Constructor.
   *
   * @param scaleWriter     the scale writer
   * @param magnitudeWriter the magnitude writer
   */
  public CompressedBigDecimalLongColumnSerializer(
      CompressedVSizeColumnarIntsSerializer scaleWriter,
      V3CompressedVSizeColumnarMultiIntsSerializer magnitudeWriter
  )
  {
    this.scaleWriter = scaleWriter;
    this.magnitudeWriter = magnitudeWriter;
  }

  @Override
  public void open() throws IOException
  {
    scaleWriter.open();
    magnitudeWriter.open();
  }

  @Override
  public void serialize(ColumnValueSelector<? extends CompressedBigDecimal> obj) throws IOException
  {
    CompressedBigDecimal abd = obj.getObject();
    int[] array = new int[abd.getArraySize()];
    for (int ii = 0; ii < abd.getArraySize(); ++ii) {
      array[ii] = abd.getArrayEntry(ii);
    }

    scaleWriter.addValue(abd.getScale());
    magnitudeWriter.addValues(new ArrayBasedIndexedInts(array));
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return 1 +   // version
        scaleWriter.getSerializedSize() +
        magnitudeWriter.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[] {VERSION}));
    scaleWriter.writeTo(channel, smoosher);
    magnitudeWriter.writeTo(channel, smoosher);
  }
}
