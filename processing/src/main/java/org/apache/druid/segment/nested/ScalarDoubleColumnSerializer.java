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
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ColumnarDoublesSerializer;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Serializer for a {@link ScalarDoubleColumn}
 */
public class ScalarDoubleColumnSerializer extends NestedCommonFormatColumnSerializer
{
  private static final Logger log = new Logger(ScalarDoubleColumnSerializer.class);

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  @SuppressWarnings("unused")
  private final Closer closer;
  private DictionaryIdLookup dictionaryIdLookup;
  private FixedIndexedWriter<Double> doubleDictionaryWriter;
  private int rowCount = 0;
  private boolean closedForWrite = false;
  private boolean dictionarySerialized = false;

  private SingleValueColumnarIntsSerializer encodedValueSerializer;
  private ColumnarDoublesSerializer doublesSerializer;
  private GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter;
  private MutableBitmap[] bitmaps;
  private ByteBuffer columnNameBytes = null;

  public ScalarDoubleColumnSerializer(
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
    this.dictionaryIdLookup = new DictionaryIdLookup();
  }

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
    return !bitmaps[0].isEmpty();
  }

  @Override
  public void open() throws IOException
  {
    if (!dictionarySerialized) {
      throw new IllegalStateException("Dictionary not serialized, cannot open value serializer");
    }
    String filenameBase = StringUtils.format("%s.forward_dim", name);
    final CompressionStrategy compression = indexSpec.getDimensionCompression();
    final CompressionStrategy compressionToUse;
    if (compression != CompressionStrategy.UNCOMPRESSED && compression != CompressionStrategy.NONE) {
      compressionToUse = compression;
    } else {
      compressionToUse = CompressionStrategy.LZ4;
    }
    encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
        name,
        segmentWriteOutMedium,
        filenameBase,
        doubleDictionaryWriter.getCardinality(),
        compressionToUse
    );
    encodedValueSerializer.open();

    doublesSerializer = CompressionFactory.getDoubleSerializer(
        name,
        segmentWriteOutMedium,
        StringUtils.format("%s.double_column", name),
        ByteOrder.nativeOrder(),
        indexSpec.getDimensionCompression()
    );
    doublesSerializer.open();

    bitmapIndexWriter = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        name,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    bitmapIndexWriter.open();
    bitmapIndexWriter.setObjectsNotSorted();
    bitmaps = new MutableBitmap[doubleDictionaryWriter.getCardinality()];
    for (int i = 0; i < bitmaps.length; i++) {
      bitmaps[i] = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }
  }

  @Override
  public void openDictionaryWriter() throws IOException
  {
    doubleDictionaryWriter = new FixedIndexedWriter<>(
        segmentWriteOutMedium,
        ColumnType.DOUBLE.getStrategy(),
        ByteOrder.nativeOrder(),
        Long.BYTES,
        true
    );
    doubleDictionaryWriter.open();
  }

  @Override
  public void serializeDictionaries(
      Iterable<String> strings,
      Iterable<Long> longs,
      Iterable<Double> doubles,
      Iterable<int[]> arrays
  ) throws IOException
  {
    if (dictionarySerialized) {
      throw new ISE("String dictionary already serialized for column [%s], cannot serialize again", name);
    }

    // null is always 0
    doubleDictionaryWriter.write(null);
    dictionaryIdLookup.addNumericNull();

    for (Double value : doubles) {
      if (value == null) {
        continue;
      }
      doubleDictionaryWriter.write(value);
      dictionaryIdLookup.addDouble(value);
    }
    dictionarySerialized = true;
  }

  @Override
  public void serialize(ColumnValueSelector<? extends StructuredData> selector) throws IOException
  {
    if (!dictionarySerialized) {
      throw new ISE("Must serialize value dictionaries before serializing values for column [%s]", name);
    }

    final Object value = StructuredData.unwrap(selector.getObject());
    final ExprEval<?> eval = ExprEval.bestEffortOf(value);

    final double val = eval.asDouble();
    final int dictId = eval.isNumericNull() ? 0 : dictionaryIdLookup.lookupDouble(val);
    encodedValueSerializer.addValue(dictId);
    doublesSerializer.add(dictId == 0 ? 0.0 : val);
    bitmaps[dictId].add(rowCount);
    rowCount++;
  }


  private void closeForWrite() throws IOException
  {
    if (!closedForWrite) {
      for (int i = 0; i < bitmaps.length; i++) {
        final MutableBitmap bitmap = bitmaps[i];
        bitmapIndexWriter.write(
            indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeImmutableBitmap(bitmap)
        );
        bitmaps[i] = null; // Reclaim memory
      }
      columnNameBytes = computeFilenameBytes();
      closedForWrite = true;
    }
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    closeForWrite();

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

    writeV0Header(channel, columnNameBytes);
    writeInternal(smoosher, doubleDictionaryWriter, DOUBLE_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, encodedValueSerializer, ENCODED_VALUE_COLUMN_FILE_NAME);
    writeInternal(smoosher, doublesSerializer, DOUBLE_VALUE_COLUMN_FILE_NAME);
    writeInternal(smoosher, bitmapIndexWriter, BITMAP_INDEX_FILE_NAME);

    log.info("Column [%s] serialized successfully.", name);
  }
}
