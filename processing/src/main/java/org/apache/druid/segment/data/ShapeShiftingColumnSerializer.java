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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.codecs.CompressedFormEncoder;
import org.apache.druid.segment.data.codecs.FormEncoder;
import org.apache.druid.segment.data.codecs.FormMetrics;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

/**
 * Base serializer for {@link ShapeShiftingColumn} implementations, providing most common functionality such as headers,
 * value-chunking, encoder selection, and writing out values.
 *
 * Encoding Selection:
 * The intention of this base structure is that implementors of this class will analyze incoming values and aggregate
 * facts about the data which matching {@link FormEncoder} implementations might find interesting, while storing raw,
 * unencoded values in {@link ShapeShiftingColumnSerializer#currentChunk}. When the threshold of
 * {@link ShapeShiftingColumnSerializer#valuesPerChunk} is reached, {@link ShapeShiftingColumnSerializer} will attempt
 * to find the "best" encoding by first computing the encoded size with
 * {@link FormEncoder#getEncodedSize} and then applying a modifier to scale this value in order to influence behavior
 * when sizes are relatively close according to the chosen {@link IndexSpec.ShapeShiftOptimizationTarget}. This
 * effectively sways the decision towards using encodings with faster decoding speed or smaller encoded size as
 * appropriate. Note that very often the best encoding is unambiguous and these settings don't matter, the nuanced
 * differences of behavior of {@link IndexSpec.ShapeShiftOptimizationTarget} mainly come into play when things are
 * close.
 *
 * Implementors need only supply an initialize method to allocate storage for {@code <TChunk>}, an add value method to
 * populate {@code <TChunk>}, a reset method to prepare {@code <TChunkMetrics>} for the next chunk after a flush, and
 * of course, matching {@link FormEncoder} implementations to perform actual value encoding. Generic compression is
 * available to {@link FormEncoder} implementations by implementing
 * {@link org.apache.druid.segment.data.codecs.CompressibleFormEncoder} and wrapping in a
 * {@link CompressedFormEncoder} in the codec list passed to the serializer.
 *
 * layout:
 * | version (byte) | headerSize (int) | numValues (int) | numChunks (int) | logValuesPerChunk (byte) |  compositionOffset (int) | compositionSize (int) | offsetsOffset (int) | offsetsSize (int) | composition | offsets | values |
 *
 * @param <TChunk>
 * @param <TChunkMetrics>
 */
public abstract class ShapeShiftingColumnSerializer<TChunk, TChunkMetrics extends FormMetrics> implements Serializer
{
  /**
   * | version (byte) | headerSize (int) | numValues (int) | numChunks (int) | logValuesPerChunk (byte) | compositionOffset (int) | compositionSize (int) |  offsetsOffset (int) | offsetsSize (int) |
   */
  private static final int BASE_HEADER_BYTES = 1 + (3 * Integer.BYTES) + 1 + (4 * Integer.BYTES);

  private static Logger log = new Logger(ShapeShiftingColumnSerializer.class);

  protected final SegmentWriteOutMedium segmentWriteOutMedium;
  protected final FormEncoder<TChunk, TChunkMetrics>[] codecs;
  protected final byte version;
  protected final byte logValuesPerChunk;
  protected final int valuesPerChunk;
  protected final ByteBuffer intToBytesHelperBuffer;
  protected final Object2IntMap<FormEncoder> composition;
  protected final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget;
  protected WriteOutBytes offsetsOut;
  protected WriteOutBytes valuesOut;
  protected boolean wroteFinalOffset = false;
  protected TChunkMetrics chunkMetrics;
  protected TChunk currentChunk;
  protected int currentChunkPos = 0;
  protected int numChunks = 0;
  protected int numValues = 0;

  public ShapeShiftingColumnSerializer(
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final FormEncoder<TChunk, TChunkMetrics>[] codecs,
      final IndexSpec.ShapeShiftOptimizationTarget optimizationTarget,
      final IndexSpec.ShapeShiftBlockSize blockSize,
      final int logBytesPerValue,
      final byte version,
      @Nullable final ByteOrder overrideByteOrder,
      @Nullable final Byte overrideLogValuesPerChunk
  )
  {
    Preconditions.checkArgument(codecs.length > 0, "must have at least one encoder");
    this.segmentWriteOutMedium = Preconditions.checkNotNull(segmentWriteOutMedium, "segmentWriteOutMedium");
    this.version = version;
    this.logValuesPerChunk = overrideLogValuesPerChunk != null
                             ? overrideLogValuesPerChunk
                             : (byte) (blockSize.getLogBlockSize() - logBytesPerValue);
    this.valuesPerChunk = 1 << logValuesPerChunk;
    this.codecs = codecs;
    this.optimizationTarget = optimizationTarget;
    ByteOrder byteOrder = overrideByteOrder == null ? ByteOrder.nativeOrder() : overrideByteOrder;
    this.intToBytesHelperBuffer = ByteBuffer.allocate(Integer.BYTES).order(byteOrder);
    this.composition = new Object2IntArrayMap<>();
  }

  public void open() throws IOException
  {
    offsetsOut = segmentWriteOutMedium.makeWriteOutBytes();
    valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
    initializeChunk();
    resetChunkCollector();
  }

  /**
   * Initialize/allocate {@link ShapeShiftingColumnSerializer#currentChunk} to hold unencoded chunk values until
   * {@link ShapeShiftingColumnSerializer#flushCurrentChunk()} is performed.
   */
  public abstract void initializeChunk();

  /**
   * Reset {@link ShapeShiftingColumnSerializer#chunkMetrics} to prepare for analyzing the next incoming chunk of data
   * after performing {@link ShapeShiftingColumnSerializer#flushCurrentChunk()}
   */
  public abstract void resetChunkCollector();

  @Override
  public long getSerializedSize() throws IOException
  {
    if (currentChunkPos > 0) {
      flushCurrentChunk();
    }

    writeFinalOffset();

    return getHeaderSize() + valuesOut.size();
  }

  @Override
  public void writeTo(
      final WritableByteChannel channel,
      final FileSmoosher smoosher
  ) throws IOException
  {
    if (currentChunkPos > 0) {
      flushCurrentChunk();
    }

    writeShapeShiftHeader(channel);
    valuesOut.writeTo(channel);
  }


  protected ByteBuffer toBytes(final int n)
  {
    intToBytesHelperBuffer.putInt(0, n);
    intToBytesHelperBuffer.rewind();
    return intToBytesHelperBuffer;
  }

  /**
   * Encode values of {@link ShapeShiftingColumnSerializer#currentChunk} with the 'best' available {@link FormEncoder}
   * given the information collected in {@link ShapeShiftingColumnSerializer#chunkMetrics}. The best is chosen by
   * computing the smallest 'modified' size, where {@link FormEncoder#getModifiedEncodedSize} is tuned based
   * on decoding speed for each encoding in relation to all other available encodings.
   *
   * @throws IOException
   */
  protected void flushCurrentChunk() throws IOException
  {
    Preconditions.checkState(!wroteFinalOffset, "!wroteFinalOffset");
    Preconditions.checkState(currentChunkPos > 0, "currentChunkPos > 0");
    Preconditions.checkState(offsetsOut.isOpen(), "offsetsOut.isOpen");
    Preconditions.checkState(valuesOut.isOpen(), "valuesOut.isOpen");

    offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));

    int bestSize = Integer.MAX_VALUE;
    FormEncoder<TChunk, TChunkMetrics> bestCodec = null;
    if (codecs.length > 1) {
      for (FormEncoder<TChunk, TChunkMetrics> codec : codecs) {
        double modifiedSize = codec.getModifiedEncodedSize(currentChunk, currentChunkPos, chunkMetrics);
        if (modifiedSize < bestSize) {
          bestCodec = codec;
          bestSize = (int) modifiedSize;
        }
      }
    } else {
      bestCodec = codecs[0];
    }

    if (bestCodec == null) {
      throw new RuntimeException("WTF? Unable to select an encoder.");
    }

    if (!composition.containsKey(bestCodec)) {
      composition.put(bestCodec, 0);
    }
    composition.computeIfPresent(bestCodec, (k, v) -> v + 1);
    if (bestCodec instanceof CompressedFormEncoder) {
      FormEncoder inner = ((CompressedFormEncoder) bestCodec).getInnerEncoder();
      if (!composition.containsKey(inner)) {
        composition.put(inner, 0);
      }
      composition.computeIfPresent(inner, (k, v) -> v + 1);
    }
    valuesOut.write(new byte[]{bestCodec.getHeader()});
    bestCodec.encode(valuesOut, currentChunk, currentChunkPos, chunkMetrics);

    numChunks++;
    resetChunk();
  }

  private void resetChunk()
  {
    currentChunkPos = 0;
    resetChunkCollector();
  }


  private int getHeaderSize() throws IOException
  {
    return BASE_HEADER_BYTES + (composition.size() * 5) + Ints.checkedCast(offsetsOut.size());
  }


  private void writeFinalOffset() throws IOException
  {
    if (!wroteFinalOffset) {
      offsetsOut.write(toBytes(Ints.checkedCast(valuesOut.size())));
      wroteFinalOffset = true;
    }
  }

  private void writeShapeShiftHeader(WritableByteChannel channel) throws IOException
  {
    writeFinalOffset();

    int compositionSizeBytes = composition.entrySet().size() * 5;
    int offsetsSizeBytes = Ints.checkedCast(offsetsOut.size());
    int headerSizeBytes = BASE_HEADER_BYTES + compositionSizeBytes + offsetsSizeBytes;

    channel.write(ByteBuffer.wrap(new byte[]{version}));
    channel.write(toBytes(headerSizeBytes));
    channel.write(toBytes(numValues));
    channel.write(toBytes(numChunks));
    channel.write(ByteBuffer.wrap(new byte[]{logValuesPerChunk}));
    channel.write(toBytes(BASE_HEADER_BYTES));
    channel.write(toBytes(compositionSizeBytes));
    channel.write(toBytes(BASE_HEADER_BYTES + compositionSizeBytes));
    channel.write(toBytes(offsetsSizeBytes));

    // write composition map
    for (Map.Entry<FormEncoder, Integer> enc : composition.entrySet()) {
      channel.write(ByteBuffer.wrap(new byte[]{enc.getKey().getHeader()}));
      channel.write(toBytes(enc.getValue()));
      log.info(enc.getKey().getName() + ": " + enc.getValue());
    }

    // write offsets
    offsetsOut.writeTo(channel);
  }
}
