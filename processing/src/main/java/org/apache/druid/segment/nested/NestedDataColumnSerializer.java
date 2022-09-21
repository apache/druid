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
import com.google.common.collect.Maps;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Map;
import java.util.SortedMap;

public class NestedDataColumnSerializer implements GenericColumnSerializer<StructuredData>
{
  private static final Logger log = new Logger(NestedDataColumnSerializer.class);
  public static final IntTypeStrategy INT_TYPE_STRATEGY = new IntTypeStrategy();
  public static final String STRING_DICTIONARY_FILE_NAME = "__stringDictionary";
  public static final String LONG_DICTIONARY_FILE_NAME = "__longDictionary";
  public static final String DOUBLE_DICTIONARY_FILE_NAME = "__doubleDictionary";
  public static final String RAW_FILE_NAME = "__raw";
  public static final String NULL_BITMAP_FILE_NAME = "__nullIndex";

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  @SuppressWarnings("unused")
  private final Closer closer;

  private final StructuredDataProcessor fieldProcessor = new StructuredDataProcessor()
  {
    @Override
    public int processLiteralField(String fieldName, Object fieldValue)
    {
      final GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.get(fieldName);
      if (writer != null) {
        try {
          ExprEval<?> eval = ExprEval.bestEffortOf(fieldValue);
          writer.addValue(rowCount, eval.value());
          // serializer doesn't use size estimate
          return 0;
        }
        catch (IOException e) {
          throw new RuntimeException(":(");
        }
      }
      return 0;
    }
  };

  private byte[] metadataBytes;
  private GlobalDictionaryIdLookup globalDictionaryIdLookup;
  private SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> fields;
  private GenericIndexedWriter<String> fieldsWriter;
  private NestedLiteralTypeInfo.Writer fieldsInfoWriter;
  private GenericIndexedWriter<String> dictionaryWriter;
  private FixedIndexedWriter<Long> longDictionaryWriter;
  private FixedIndexedWriter<Double> doubleDictionaryWriter;
  private CompressedVariableSizedBlobColumnSerializer rawWriter;
  private ByteBufferWriter<ImmutableBitmap> nullBitmapWriter;
  private MutableBitmap nullRowsBitmap;
  private Map<String, GlobalDictionaryEncodedFieldColumnWriter<?>> fieldWriters;
  private int rowCount = 0;
  private boolean closedForWrite = false;

  public NestedDataColumnSerializer(
      String name,
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      @SuppressWarnings("unused") ProgressIndicator progressIndicator,
      Closer closer
  )
  {
    this.name = name;
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.indexSpec = indexSpec;
    this.closer = closer;
    this.globalDictionaryIdLookup = new GlobalDictionaryIdLookup();
  }

  @Override
  public void open() throws IOException
  {
    fieldsWriter = new GenericIndexedWriter<>(segmentWriteOutMedium, name, GenericIndexed.STRING_STRATEGY);
    fieldsWriter.open();

    fieldsInfoWriter = new NestedLiteralTypeInfo.Writer(segmentWriteOutMedium);
    fieldsInfoWriter.open();

    dictionaryWriter = new GenericIndexedWriter<>(segmentWriteOutMedium, name, GenericIndexed.STRING_STRATEGY);
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

    rawWriter = new CompressedVariableSizedBlobColumnSerializer(
        getInternalFileName(name, RAW_FILE_NAME),
        segmentWriteOutMedium,
        indexSpec.getJsonCompression() != null ? indexSpec.getJsonCompression() : CompressionStrategy.LZ4
    );
    rawWriter.open();

    nullBitmapWriter = new ByteBufferWriter<>(
        segmentWriteOutMedium,
        indexSpec.getBitmapSerdeFactory().getObjectStrategy()
    );
    nullBitmapWriter.open();

    nullRowsBitmap = indexSpec.getBitmapSerdeFactory().getBitmapFactory().makeEmptyMutableBitmap();
  }

  public void serializeFields(SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> fields) throws IOException
  {
    this.fields = fields;
    this.fieldWriters = Maps.newHashMapWithExpectedSize(fields.size());
    for (Map.Entry<String, NestedLiteralTypeInfo.MutableTypeSet> field : fields.entrySet()) {
      final String fieldName = field.getKey();
      fieldsWriter.write(fieldName);
      fieldsInfoWriter.write(field.getValue());
      final GlobalDictionaryEncodedFieldColumnWriter<?> writer;
      final ColumnType type = field.getValue().getSingleType();
      if (type != null) {
        if (Types.is(type, ValueType.STRING)) {
          writer = new StringFieldColumnWriter(
              name,
              fieldName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else if (Types.is(type, ValueType.LONG)) {
          writer = new LongFieldColumnWriter(
              name,
              fieldName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else {
          writer = new DoubleFieldColumnWriter(
              name,
              fieldName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        }
      } else {
        writer = new VariantLiteralFieldColumnWriter(
            name,
            fieldName,
            segmentWriteOutMedium,
            indexSpec,
            globalDictionaryIdLookup
        );
      }
      writer.open();
      fieldWriters.put(fieldName, writer);
    }
  }

  public void serializeStringDictionary(Iterable<String> dictionaryValues) throws IOException
  {
    dictionaryWriter.write(null);
    globalDictionaryIdLookup.addString(null);
    for (String value : dictionaryValues) {
      if (NullHandling.emptyToNullIfNeeded(value) == null) {
        continue;
      }
      dictionaryWriter.write(value);
      value = NullHandling.emptyToNullIfNeeded(value);
      globalDictionaryIdLookup.addString(value);
    }
  }

  public void serializeLongDictionary(Iterable<Long> dictionaryValues) throws IOException
  {
    for (Long value : dictionaryValues) {
      if (value == null) {
        continue;
      }
      longDictionaryWriter.write(value);
      globalDictionaryIdLookup.addLong(value);
    }
  }

  public void serializeDoubleDictionary(Iterable<Double> dictionaryValues) throws IOException
  {
    for (Double value : dictionaryValues) {
      if (value == null) {
        continue;
      }
      doubleDictionaryWriter.write(value);
      globalDictionaryIdLookup.addDouble(value);
    }
  }

  @Override
  public void serialize(ColumnValueSelector<? extends StructuredData> selector) throws IOException
  {
    StructuredData data = selector.getObject();
    if (data == null) {
      nullRowsBitmap.add(rowCount);
    }
    rawWriter.addValue(NestedDataComplexTypeSerde.INSTANCE.toBytes(data));
    if (data != null) {
      fieldProcessor.processFields(data.getValue());
    }
    rowCount++;
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    if (!closedForWrite) {
      closedForWrite = true;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      IndexMerger.SERIALIZER_UTILS.writeString(
          baos,
          NestedDataComplexTypeSerde.OBJECT_MAPPER.writeValueAsString(
              new NestedDataColumnMetadata(
                  ByteOrder.nativeOrder(),
                  indexSpec.getBitmapSerdeFactory(),
                  name,
                  !nullRowsBitmap.isEmpty()
              )
          )
      );
      this.metadataBytes = baos.toByteArray();
      this.nullBitmapWriter.write(nullRowsBitmap);
    }

    long size = 1;
    size += metadataBytes.length;
    if (fieldsWriter != null) {
      size += fieldsWriter.getSerializedSize();
    }
    if (fieldsInfoWriter != null) {
      size += fieldsInfoWriter.getSerializedSize();
    }
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

    // version 3
    channel.write(ByteBuffer.wrap(new byte[]{0x03}));
    channel.write(ByteBuffer.wrap(metadataBytes));
    fieldsWriter.writeTo(channel, smoosher);
    fieldsInfoWriter.writeTo(channel, smoosher);

    // version 3 stores large components in separate files to prevent exceeding smoosh file limit (int max)
    writeInternal(smoosher, dictionaryWriter, STRING_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, longDictionaryWriter, LONG_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, doubleDictionaryWriter, DOUBLE_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, rawWriter, RAW_FILE_NAME);
    if (!nullRowsBitmap.isEmpty()) {
      writeInternal(smoosher, nullBitmapWriter, NULL_BITMAP_FILE_NAME);
    }


    // close the SmooshedWriter since we are done here, so we don't write to a temporary file per sub-column
    // In the future, it would be best if the writeTo() itself didn't take a channel but was expected to actually
    // open its own channels on the FileSmoosher object itself.  Or some other thing that give this Serializer
    // total control over when resources are opened up and when they are closed.  Until then, we are stuck
    // with a very tight coupling of this code with how the external "driver" is working.
    if (channel instanceof SmooshedWriter) {
      channel.close();
    }
    for (Map.Entry<String, NestedLiteralTypeInfo.MutableTypeSet> field : fields.entrySet()) {
      // remove writer so that it can be collected when we are done with it
      GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.remove(field.getKey());
      writer.writeTo(rowCount, smoosher);
    }
    log.info("Column [%s] serialized successfully with [%d] nested columns.", name, fields.size());
  }

  private void writeInternal(FileSmoosher smoosher, Serializer serializer, String fileName) throws IOException
  {
    final String internalName = getInternalFileName(name, fileName);
    try (SmooshedWriter smooshChannel = smoosher.addWithSmooshedWriter(internalName, serializer.getSerializedSize())) {
      serializer.writeTo(smooshChannel, smoosher);
    }
  }

  public static String getFieldFileName(String fileNameBase, String field)
  {
    return StringUtils.format("%s_%s", fileNameBase, field);
  }

  public static String getInternalFileName(String fileNameBase, String field)
  {
    return StringUtils.format("%s.%s", fileNameBase, field);
  }

  private static final class IntTypeStrategy implements TypeStrategy<Integer>
  {
    @Override
    public int estimateSizeBytes(Integer value)
    {
      return Integer.BYTES;
    }

    @Override
    public Integer read(ByteBuffer buffer)
    {
      return buffer.getInt();
    }

    @Override
    public boolean readRetainsBufferReference()
    {
      return false;
    }

    @Override
    public int write(ByteBuffer buffer, Integer value, int maxSizeBytes)
    {
      TypeStrategies.checkMaxSize(buffer.remaining(), maxSizeBytes, ColumnType.LONG);
      final int sizeBytes = Integer.BYTES;
      final int remaining = maxSizeBytes - sizeBytes;
      if (remaining >= 0) {
        buffer.putInt(value);
        return sizeBytes;
      }
      return remaining;
    }

    @Override
    public int compare(Integer o1, Integer o2)
    {
      return Integer.compare(o1, o2);
    }
  }
}
