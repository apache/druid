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

package org.apache.druid.segment.nested;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.FixedIndexedIntWriter;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public abstract class ScalarNestedCommonFormatColumnSerializer<T> extends NestedCommonFormatColumnSerializer
{
  protected static final Logger log = new Logger(ScalarNestedCommonFormatColumnSerializer.class);

  protected final String name;
  protected final SegmentWriteOutMedium segmentWriteOutMedium;
  protected final IndexSpec indexSpec;
  @SuppressWarnings("unused")
  protected final Closer closer;

  protected DictionaryIdLookup dictionaryIdLookup;
  protected DictionaryWriter<T> dictionaryWriter;
  protected boolean closedForWrite = false;
  protected boolean dictionarySerialized = false;
  protected FixedIndexedIntWriter intermediateValueWriter;
  protected ByteBuffer columnNameBytes = null;

  protected boolean hasNulls;


  public ScalarNestedCommonFormatColumnSerializer(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      Closer closer
  )
  {
    this.name = name;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.indexSpec = indexSpec;
    this.closer = closer;
  }

  /**
   * Called during {@link #serialize(ColumnValueSelector)} to convert value to dictionary id.
   * <p>
   * Implementations may optionally also serialize the value to a type specific value column if they opened one with
   * {@link #openValueColumnSerializer()}, or do whatever else is useful to do while handling a single row value.
   */
  protected abstract int processValue(@Nullable Object rawValue) throws IOException;

  /**
   * Called during {@link #open()} to allow opening any separate type specific value column serializers
   */
  protected abstract void openValueColumnSerializer() throws IOException;

  /**
   * Called during {@link #writeTo(WritableByteChannel, FileSmoosher)} to allow any type specific value column
   * serializers to use the {@link FileSmoosher} to write stuff to places.
   */
  protected abstract void writeValueColumn(FileSmoosher smoosher) throws IOException;

  protected abstract void writeDictionaryFile(FileSmoosher smoosher) throws IOException;

  @Override
  public String getColumnName()
  {
    return name;
  }

  @Override
  public DictionaryIdLookup getGlobalLookup()
  {
    return dictionaryIdLookup;
  }

  @Override
  public boolean hasNulls()
  {
    return hasNulls;
  }

  @Override
  public void open() throws IOException
  {
    if (!dictionarySerialized) {
      throw new IllegalStateException("Dictionary not serialized, cannot open value serializer");
    }
    intermediateValueWriter = new FixedIndexedIntWriter(segmentWriteOutMedium, false);
    intermediateValueWriter.open();
    openValueColumnSerializer();
  }

  @Override
  public void serialize(ColumnValueSelector<? extends StructuredData> selector) throws IOException
  {
    if (!dictionarySerialized) {
      throw new ISE("Must serialize value dictionaries before serializing values for column [%s]", name);
    }

    final Object value = StructuredData.unwrap(selector.getObject());
    final int dictId = processValue(value);
    intermediateValueWriter.write(dictId);
    hasNulls = hasNulls || dictId == 0;
  }

  private void closeForWrite()
  {
    if (!closedForWrite) {
      columnNameBytes = computeFilenameBytes();
      closedForWrite = true;
    }
  }

  @Override
  public long getSerializedSize()
  {
    closeForWrite();

    // standard string version
    long size = 1 + columnNameBytes.capacity();
    // the value dictionaries, raw column, and null index are all stored in separate files
    return size;
  }

  @Override
  public void writeTo(
      WritableByteChannel channel,
      FileSmoosher smoosher
  ) throws IOException
  {
    Preconditions.checkState(closedForWrite, "Not closed yet!");
    Preconditions.checkArgument(dictionaryWriter.isSorted(), "Dictionary not sorted?!?");

    // write out compressed dictionaryId int column and bitmap indexes by iterating intermediate value column
    // the intermediate value column should be replaced someday by a cooler compressed int column writer that allows
    // easy iteration of the values it writes out, so that we could just build the bitmap indexes here instead of
    // doing both things
    String filenameBase = StringUtils.format("%s.forward_dim", name);
    final CompressionStrategy compression = indexSpec.getDimensionCompression();
    final CompressionStrategy compressionToUse;
    if (compression != CompressionStrategy.UNCOMPRESSED && compression != CompressionStrategy.NONE) {
      compressionToUse = compression;
    } else {
      compressionToUse = CompressionStrategy.LZ4;
    }
    final SingleValueColumnarIntsSerializer encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
        name,
        segmentWriteOutMedium,
        filenameBase,
        dictionaryWriter.getCardinality(),
        compressionToUse,
        segmentWriteOutMedium.getCloser()
    );
    encodedValueSerializer.open();

    final GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        name,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    bitmapIndexWriter.open();
    bitmapIndexWriter.setObjectsNotSorted();
    final MutableBitmap[] bitmaps;
    bitmaps = new MutableBitmap[dictionaryWriter.getCardinality()];
    for (int i = 0; i < bitmaps.length; i++) {
      bitmaps[i] = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }

    final IntIterator rows = intermediateValueWriter.getIterator();
    int rowCount = 0;
    while (rows.hasNext()) {
      final int dictId = rows.nextInt();
      encodedValueSerializer.addValue(dictId);
      bitmaps[dictId].add(rowCount++);
    }

    for (int i = 0; i < bitmaps.length; i++) {
      final MutableBitmap bitmap = bitmaps[i];
      bitmapIndexWriter.write(
          indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeImmutableBitmap(bitmap)
      );
      bitmaps[i] = null; // Reclaim memory
    }

    writeV0Header(channel, columnNameBytes);
    writeDictionaryFile(smoosher);
    writeInternal(smoosher, encodedValueSerializer, ENCODED_VALUE_COLUMN_FILE_NAME);
    writeValueColumn(smoosher);
    writeInternal(smoosher, bitmapIndexWriter, BITMAP_INDEX_FILE_NAME);

    log.info("Column [%s] serialized successfully.", name);
  }
}
