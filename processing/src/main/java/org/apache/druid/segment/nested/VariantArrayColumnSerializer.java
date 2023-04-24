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
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectRBTreeMap;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.FixedIndexedIntWriter;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexedWriter;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class VariantArrayColumnSerializer extends NestedCommonFormatColumnSerializer
{
  private static final Logger log = new Logger(VariantArrayColumnSerializer.class);

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  @SuppressWarnings("unused")
  private final Closer closer;
  private DictionaryIdLookup dictionaryIdLookup;
  private DictionaryWriter<String> dictionaryWriter;
  private FixedIndexedWriter<Long> longDictionaryWriter;
  private FixedIndexedWriter<Double> doubleDictionaryWriter;
  private FrontCodedIntArrayIndexedWriter arrayDictionaryWriter;
  private FixedIndexedIntWriter arrayElementDictionaryWriter;

  private int rowCount = 0;
  private boolean closedForWrite = false;

  private boolean dictionarySerialized = false;

  private SingleValueColumnarIntsSerializer encodedValueSerializer;
  private GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter;
  private GenericIndexedWriter<ImmutableBitmap> arrayElementIndexWriter;
  private MutableBitmap[] bitmaps;
  private ByteBuffer columnNameBytes = null;
  private final Int2ObjectRBTreeMap<MutableBitmap> arrayElements = new Int2ObjectRBTreeMap<>();

  public VariantArrayColumnSerializer(
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
  public void openDictionaryWriter() throws IOException
  {
    dictionaryWriter = StringEncodingStrategies.getStringDictionaryWriter(
        indexSpec.getStringDictionaryEncoding(),
        segmentWriteOutMedium,
        name
    );
    dictionaryWriter.open();

    longDictionaryWriter = new FixedIndexedWriter<>(
        segmentWriteOutMedium,
        ColumnType.LONG.getStrategy(),
        ByteOrder.nativeOrder(),
        Long.BYTES,
        true
    );
    longDictionaryWriter.open();

    doubleDictionaryWriter = new FixedIndexedWriter<>(
        segmentWriteOutMedium,
        ColumnType.DOUBLE.getStrategy(),
        ByteOrder.nativeOrder(),
        Double.BYTES,
        true
    );
    doubleDictionaryWriter.open();

    arrayDictionaryWriter = new FrontCodedIntArrayIndexedWriter(
        segmentWriteOutMedium,
        ByteOrder.nativeOrder(),
        4
    );
    arrayDictionaryWriter.open();
    arrayElementDictionaryWriter = new FixedIndexedIntWriter(segmentWriteOutMedium, true);
    arrayElementDictionaryWriter.open();
  }

  @Override
  public void open() throws IOException
  {
    if (!dictionarySerialized) {
      throw new IllegalStateException("Dictionary not serialized, cannot open value serializer");
    }
    String filenameBase = StringUtils.format("%s.forward_dim", name);
    final int cardinality = dictionaryWriter.getCardinality()
                            + longDictionaryWriter.getCardinality()
                            + doubleDictionaryWriter.getCardinality()
                            + arrayDictionaryWriter.getCardinality();
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
        cardinality,
        compressionToUse
    );
    encodedValueSerializer.open();

    bitmapIndexWriter = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        name,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    bitmapIndexWriter.open();
    bitmapIndexWriter.setObjectsNotSorted();
    bitmaps = new MutableBitmap[cardinality];
    for (int i = 0; i < bitmaps.length; i++) {
      bitmaps[i] = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }
    arrayElementIndexWriter = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        name + "_arrays",
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    arrayElementIndexWriter.open();
    arrayElementIndexWriter.setObjectsNotSorted();
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
    dictionaryWriter.write(null);
    dictionaryIdLookup.addString(null);
    for (String value : strings) {
      value = NullHandling.emptyToNullIfNeeded(value);
      if (value == null) {
        continue;
      }

      dictionaryWriter.write(value);
      dictionaryIdLookup.addString(value);
    }

    for (Long value : longs) {
      if (value == null) {
        continue;
      }
      longDictionaryWriter.write(value);
      dictionaryIdLookup.addLong(value);
    }

    for (Double value : doubles) {
      if (value == null) {
        continue;
      }
      doubleDictionaryWriter.write(value);
      dictionaryIdLookup.addDouble(value);
    }

    for (int[] value : arrays) {
      if (value == null) {
        continue;
      }
      arrayDictionaryWriter.write(value);
      dictionaryIdLookup.addArray(value);
    }
    dictionarySerialized = true;
  }

  @Override
  public void serialize(ColumnValueSelector<? extends StructuredData> selector) throws IOException
  {
    if (!dictionarySerialized) {
      throw new ISE("Must serialize value dictionaries before serializing values for column [%s]", name);
    }

    ExprEval eval = ExprEval.bestEffortOf(StructuredData.unwrap(selector.getObject()));
    int[] globalIds = null;
    if (eval.isArray()) {
      Object[] array = eval.asArray();
      globalIds = new int[array.length];
      for (int i = 0; i < array.length; i++) {
        if (array[i] == null) {
          globalIds[i] = 0;
        } else if (array[i] instanceof String) {
          globalIds[i] = dictionaryIdLookup.lookupString((String) array[i]);
        } else if (array[i] instanceof Long) {
          globalIds[i] = dictionaryIdLookup.lookupLong((Long) array[i]);
        } else if (array[i] instanceof Double) {
          globalIds[i] = dictionaryIdLookup.lookupDouble((Double) array[i]);
        } else {
          globalIds[i] = -1;
        }
        Preconditions.checkArgument(globalIds[i] >= 0, "unknown global id [%s] for value [%s]", globalIds[i], array[i]);
        arrayElements.computeIfAbsent(
            globalIds[i],
            (id) -> indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap()
        ).add(rowCount);
      }
    }
    final int dictId = globalIds == null ? 0 : dictionaryIdLookup.lookupArray(globalIds);
    encodedValueSerializer.addValue(dictId);
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
      for (Int2ObjectMap.Entry<MutableBitmap> arrayElement : arrayElements.int2ObjectEntrySet()) {
        arrayElementDictionaryWriter.write(arrayElement.getIntKey());
        arrayElementIndexWriter.write(
            indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeImmutableBitmap(arrayElement.getValue())
        );
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
    Preconditions.checkArgument(dictionaryWriter.isSorted(), "Dictionary not sorted?!?");

    writeV0Header(channel, columnNameBytes);

    writeInternal(smoosher, dictionaryWriter, STRING_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, longDictionaryWriter, LONG_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, doubleDictionaryWriter, DOUBLE_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, arrayDictionaryWriter, ARRAY_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, arrayElementDictionaryWriter, ARRAY_ELEMENT_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, encodedValueSerializer, ENCODED_VALUE_COLUMN_FILE_NAME);
    writeInternal(smoosher, bitmapIndexWriter, BITMAP_INDEX_FILE_NAME);
    writeInternal(smoosher, arrayElementIndexWriter, ARRAY_ELEMENT_BITMAP_INDEX_FILE_NAME);

    log.info("Column [%s] serialized successfully.", name);
  }
}
