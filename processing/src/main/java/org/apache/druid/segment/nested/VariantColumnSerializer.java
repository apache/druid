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
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

/**
 * Serializer for a {@link NestedCommonFormatColumn} for single type arrays and mixed type columns, but not columns
 * with nested data. If {@link #variantTypeSetByte} is set then the column has mixed types, which is added to the base
 * metadata stored in the column file.
 */
public class VariantColumnSerializer extends NestedCommonFormatColumnSerializer
{
  private static final Logger log = new Logger(VariantColumnSerializer.class);

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
  private boolean closedForWrite = false;
  private boolean dictionarySerialized = false;
  private FixedIndexedIntWriter intermediateValueWriter;

  private ByteBuffer columnNameBytes = null;
  private boolean hasNulls;
  @Nullable
  private final ExpressionType expectedExpressionType;
  @Nullable
  private final Byte variantTypeSetByte;

  public VariantColumnSerializer(
      String name,
      @Nullable ColumnType logicalType,
      @Nullable Byte variantTypeSetByte,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      Closer closer
  )
  {
    this.name = name;
    this.expectedExpressionType = logicalType != null ? ExpressionType.fromColumnTypeStrict(logicalType) : null;
    this.variantTypeSetByte = variantTypeSetByte;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.indexSpec = indexSpec;
    this.closer = closer;
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
    return hasNulls;
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
    dictionaryIdLookup = closer.register(
        new DictionaryIdLookup(
            name,
            FileUtils.getTempDir(),
            dictionaryWriter,
            longDictionaryWriter,
            doubleDictionaryWriter,
            arrayDictionaryWriter
        )
    );
  }

  @Override
  public void open() throws IOException
  {
    if (!dictionarySerialized) {
      throw new IllegalStateException("Dictionary not serialized, cannot open value serializer");
    }
    intermediateValueWriter = new FixedIndexedIntWriter(segmentWriteOutMedium, false);
    intermediateValueWriter.open();
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
      throw new ISE("Value dictionaries already serialized for column [%s], cannot serialize again", name);
    }

    // null is always 0
    dictionaryWriter.write(null);
    for (String value : strings) {
      value = NullHandling.emptyToNullIfNeeded(value);
      if (value == null) {
        continue;
      }

      dictionaryWriter.write(value);
    }

    for (Long value : longs) {
      if (value == null) {
        continue;
      }
      longDictionaryWriter.write(value);
    }

    for (Double value : doubles) {
      if (value == null) {
        continue;
      }
      doubleDictionaryWriter.write(value);
    }

    for (int[] value : arrays) {
      if (value == null) {
        continue;
      }
      arrayDictionaryWriter.write(value);
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
    if (expectedExpressionType != null) {
      try {
        eval = eval.castTo(expectedExpressionType);
      }
      catch (IAE invalidCast) {
        // write null
        intermediateValueWriter.write(0);
        hasNulls = true;
        return;
      }
    }
    if (eval.isArray()) {
      Object[] array = eval.asArray();
      if (array == null) {
        intermediateValueWriter.write(0);
        hasNulls = true;
        return;
      }
      int[] globalIds = new int[array.length];
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
      }
      final int dictId = dictionaryIdLookup.lookupArray(globalIds);
      intermediateValueWriter.write(dictId);
      hasNulls = hasNulls || dictId == 0;
    } else {
      final Object o = eval.value();
      final int dictId;
      if (o == null) {
        dictId = 0;
      } else if (o instanceof String) {
        dictId = dictionaryIdLookup.lookupString((String) o);
      } else if (o instanceof Long) {
        dictId = dictionaryIdLookup.lookupLong((Long) o);
      } else if (o instanceof Double) {
        dictId = dictionaryIdLookup.lookupDouble((Double) o);
      } else {
        dictId = -1;
      }
      Preconditions.checkArgument(dictId >= 0, "unknown global id [%s] for value [%s]", dictId, o);
      intermediateValueWriter.write(dictId);
      hasNulls = hasNulls || dictId == 0;
    }
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

    long size = 1 + columnNameBytes.capacity();
    // the value dictionaries, raw column, and null index are all stored in separate files
    if (variantTypeSetByte != null) {
      size += 1;
    }
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

    // write out compressed dictionaryId int column, bitmap indexes, and array element bitmap indexes
    // by iterating intermediate value column the intermediate value column should be replaced someday by a cooler
    // compressed int column writer that allows easy iteration of the values it writes out, so that we could just
    // build the bitmap indexes here instead of doing both things
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

    final SingleValueColumnarIntsSerializer encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
        name,
        segmentWriteOutMedium,
        filenameBase,
        cardinality,
        compressionToUse
    );
    encodedValueSerializer.open();

    final GenericIndexedWriter<ImmutableBitmap> bitmapIndexWriter = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        name,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    bitmapIndexWriter.open();
    bitmapIndexWriter.setObjectsNotSorted();
    final MutableBitmap[] bitmaps = new MutableBitmap[cardinality];
    final Int2ObjectRBTreeMap<MutableBitmap> arrayElements = new Int2ObjectRBTreeMap<>();
    for (int i = 0; i < bitmaps.length; i++) {
      bitmaps[i] = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
    }
    final GenericIndexedWriter<ImmutableBitmap> arrayElementIndexWriter = new GenericIndexedWriter<>(
        segmentWriteOutMedium,
        name + "_arrays",
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    arrayElementIndexWriter.open();
    arrayElementIndexWriter.setObjectsNotSorted();

    final IntIterator rows = intermediateValueWriter.getIterator();
    int rowCount = 0;
    final int arrayBaseId = dictionaryWriter.getCardinality()
                            + longDictionaryWriter.getCardinality()
                            + doubleDictionaryWriter.getCardinality();
    while (rows.hasNext()) {
      final int dictId = rows.nextInt();
      encodedValueSerializer.addValue(dictId);
      bitmaps[dictId].add(rowCount);
      if (dictId >= arrayBaseId) {
        int[] array = arrayDictionaryWriter.get(dictId - arrayBaseId);
        for (int elementId : array) {
          arrayElements.computeIfAbsent(
              elementId,
              (id) -> indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap()
          ).add(rowCount);
        }
      }
      rowCount++;
    }

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

    writeV0Header(channel, columnNameBytes);
    if (variantTypeSetByte != null) {
      channel.write(ByteBuffer.wrap(new byte[]{variantTypeSetByte}));
    }

    if (dictionaryIdLookup.getStringBufferMapper() != null) {
      SmooshedFileMapper fileMapper = dictionaryIdLookup.getStringBufferMapper();
      for (String internalName : fileMapper.getInternalFilenames()) {
        smoosher.add(internalName, fileMapper.mapFile(internalName));
      }
    } else {
      writeInternal(smoosher, dictionaryWriter, STRING_DICTIONARY_FILE_NAME);
    }
    if (dictionaryIdLookup.getLongBuffer() != null) {
      writeInternal(smoosher, dictionaryIdLookup.getLongBuffer(), LONG_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, longDictionaryWriter, LONG_DICTIONARY_FILE_NAME);
    }
    if (dictionaryIdLookup.getDoubleBuffer() != null) {
      writeInternal(smoosher, dictionaryIdLookup.getDoubleBuffer(), DOUBLE_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, doubleDictionaryWriter, DOUBLE_DICTIONARY_FILE_NAME);
    }
    if (dictionaryIdLookup.getArrayBuffer() != null) {
      writeInternal(smoosher, dictionaryIdLookup.getArrayBuffer(), ARRAY_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, arrayDictionaryWriter, ARRAY_DICTIONARY_FILE_NAME);
    }

    writeInternal(smoosher, arrayElementDictionaryWriter, ARRAY_ELEMENT_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, encodedValueSerializer, ENCODED_VALUE_COLUMN_FILE_NAME);
    writeInternal(smoosher, bitmapIndexWriter, BITMAP_INDEX_FILE_NAME);
    writeInternal(smoosher, arrayElementIndexWriter, ARRAY_ELEMENT_BITMAP_INDEX_FILE_NAME);

    log.info("Column [%s] serialized successfully.", name);
  }
}
