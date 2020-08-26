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

package org.apache.druid.segment.data.codecs;

import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.ShapeShiftingColumn;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Generic Shapeshifting form decoder for chunks that are block compressed using any of {@link CompressionStrategy}.
 * Data is decompressed to 'decompressedDataBuffer' to be further decoded by another {@link BaseFormDecoder},
 * via calling 'transform' again on the decompressed chunk.
 */
public final class CompressedFormDecoder<TShapeShiftImpl extends ShapeShiftingColumn<TShapeShiftImpl>>
    extends BaseFormDecoder<TShapeShiftImpl>
{
  private final byte header;

  public CompressedFormDecoder(byte logValuesPerChunk, ByteOrder byteOrder, byte header)
  {
    super(logValuesPerChunk, byteOrder);
    this.header = header;
  }

  @Override
  public void transform(TShapeShiftImpl shapeshiftingColumn)
  {
    final ByteBuffer buffer = shapeshiftingColumn.getBuffer();
    final ByteBuffer decompressed = shapeshiftingColumn.getDecompressedDataBuffer();
    int startOffset = shapeshiftingColumn.getCurrentChunkStartOffset();
    int endOffset = startOffset + shapeshiftingColumn.getCurrentChunkSize();
    decompressed.clear();

    final CompressionStrategy.Decompressor decompressor =
        CompressionStrategy.forId(buffer.get(startOffset++)).getDecompressor();

    // metadata for inner encoding is stored outside of the compressed values chunk, so set column metadata offsets
    // accordingly
    final byte chunkCodec = buffer.get(startOffset++);
    shapeshiftingColumn.setCurrentChunkStartOffset(startOffset);
    FormDecoder<TShapeShiftImpl> innerForm = shapeshiftingColumn.getFormDecoder(chunkCodec);
    startOffset += innerForm.getMetadataSize();
    final int size = endOffset - startOffset;

    decompressor.decompress(buffer, startOffset, size, decompressed);
    shapeshiftingColumn.setCurrentValueBuffer(decompressed);
    shapeshiftingColumn.setCurrentValuesStartOffset(0);
    if (decompressed.isDirect()) {
      shapeshiftingColumn.setCurrentValuesAddress(((DirectBuffer) decompressed).address());
    }
    // transform again, this time using the inner form against the the decompressed buffer
    shapeshiftingColumn.transform(innerForm);
  }

  @Override
  public byte getHeader()
  {
    return header;
  }
}
