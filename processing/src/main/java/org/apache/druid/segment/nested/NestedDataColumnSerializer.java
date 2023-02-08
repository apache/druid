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
import org.apache.druid.java.util.common.ISE;
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
import org.apache.druid.segment.column.StringEncodingStrategies;
import org.apache.druid.segment.column.TypeStrategies;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ByteBufferWriter;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSerializer;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.DictionaryWriter;
import org.apache.druid.segment.data.FixedIndexedWriter;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;

public class NestedDataColumnSerializer implements GenericColumnSerializer<StructuredData>
{
  private static final Logger log = new Logger(NestedDataColumnSerializer.class);
  public static final IntTypeStrategy INT_TYPE_STRATEGY = new IntTypeStrategy();
  public static final String STRING_DICTIONARY_FILE_NAME = "__stringDictionary";
  public static final String LONG_DICTIONARY_FILE_NAME = "__longDictionary";
  public static final String DOUBLE_DICTIONARY_FILE_NAME = "__doubleDictionary";
  public static final String ARRAY_DICTIONARY_FILE_NAME = "__arrayDictionary";
  public static final String RAW_FILE_NAME = "__raw";
  public static final String NULL_BITMAP_FILE_NAME = "__nullIndex";

  public static final String NESTED_FIELD_PREFIX = "__field_";

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  @SuppressWarnings("unused")
  private final Closer closer;

  private final StructuredDataProcessor fieldProcessor = new StructuredDataProcessor()
  {
    @Override
    public StructuredDataProcessor.ProcessedLiteral<?> processLiteralField(ArrayList<NestedPathPart> fieldPath, @Nullable Object fieldValue)
    {
      final GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.get(
          NestedPathFinder.toNormalizedJsonPath(fieldPath)
      );
      if (writer != null) {
        try {
          ExprEval<?> eval = ExprEval.bestEffortOf(fieldValue);
          if (eval.type().isPrimitive() || (eval.type().isArray() && eval.type().getElementType().isPrimitive())) {
            writer.addValue(rowCount, eval.value());
          } else {
            // behave consistently with nested column indexer, which defaults to string
            writer.addValue(rowCount, eval.asString());
          }
          // serializer doesn't use size estimate
          return StructuredDataProcessor.ProcessedLiteral.NULL_LITERAL;
        }
        catch (IOException e) {
          throw new RuntimeException(":(");
        }
      }
      return StructuredDataProcessor.ProcessedLiteral.NULL_LITERAL;
    }

    @Nullable
    @Override
    public ProcessedLiteral<?> processArrayOfLiteralsField(
        ArrayList<NestedPathPart> fieldPath,
        @Nullable Object maybeArrayOfLiterals
    )
    {
      ExprEval<?> eval = ExprEval.bestEffortOf(maybeArrayOfLiterals);
      if (eval.type().isArray() && eval.type().getElementType().isPrimitive()) {
        final GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.get(
            NestedPathFinder.toNormalizedJsonPath(fieldPath)
        );
        if (writer != null) {
          try {
            writer.addValue(rowCount, eval.value());
            // serializer doesn't use size estimate
            return StructuredDataProcessor.ProcessedLiteral.NULL_LITERAL;
          }
          catch (IOException e) {
            throw new RuntimeException(":(");
          }
        }
      }
      return null;
    }
  };

  private byte[] metadataBytes;
  private GlobalDictionaryIdLookup globalDictionaryIdLookup;
  private SortedMap<String, NestedLiteralTypeInfo.MutableTypeSet> fields;
  private GenericIndexedWriter<String> fieldsWriter;
  private NestedLiteralTypeInfo.Writer fieldsInfoWriter;
  private DictionaryWriter<String> dictionaryWriter;
  private FixedIndexedWriter<Long> longDictionaryWriter;
  private FixedIndexedWriter<Double> doubleDictionaryWriter;
  private FrontCodedIntArrayIndexedWriter arrayDictionaryWriter;
  private CompressedVariableSizedBlobColumnSerializer rawWriter;
  private ByteBufferWriter<ImmutableBitmap> nullBitmapWriter;
  private MutableBitmap nullRowsBitmap;
  private Map<String, GlobalDictionaryEncodedFieldColumnWriter<?>> fieldWriters;
  private int rowCount = 0;
  private boolean closedForWrite = false;

  private boolean stringDictionarySerialized = false;
  private boolean longDictionarySerialized = false;
  private boolean doubleDictionarySerialized = false;
  private boolean arrayDictionarySerialized = false;

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

  public GlobalDictionaryIdLookup getGlobalLookup()
  {
    return globalDictionaryIdLookup;
  }

  @Override
  public void open() throws IOException
  {
    fieldsWriter = new GenericIndexedWriter<>(segmentWriteOutMedium, name, GenericIndexed.STRING_STRATEGY);
    fieldsWriter.open();

    fieldsInfoWriter = new NestedLiteralTypeInfo.Writer(segmentWriteOutMedium);
    fieldsInfoWriter.open();

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
    int ctr = 0;
    for (Map.Entry<String, NestedLiteralTypeInfo.MutableTypeSet> field : fields.entrySet()) {
      final String fieldName = field.getKey();
      final String fieldFileName = NESTED_FIELD_PREFIX + ctr++;
      fieldsWriter.write(fieldName);
      fieldsInfoWriter.write(field.getValue());
      final GlobalDictionaryEncodedFieldColumnWriter<?> writer;
      final ColumnType type = field.getValue().getSingleType();
      if (type != null) {
        if (Types.is(type, ValueType.STRING)) {
          writer = new StringFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else if (Types.is(type, ValueType.LONG)) {
          writer = new LongFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else if (Types.is(type, ValueType.DOUBLE)) {
          writer = new DoubleFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else if (Types.is(type, ValueType.ARRAY)) {
          writer = new ArrayOfLiteralsFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else {
          throw new IllegalArgumentException("wtf");
        }
      } else {
        writer = new VariantLiteralFieldColumnWriter(
            name,
            fieldFileName,
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
    if (stringDictionarySerialized) {
      throw new ISE("String dictionary already serialized for column [%s], cannot serialize again", name);
    }
    // null is always 0
    dictionaryWriter.write(null);
    globalDictionaryIdLookup.addString(null);
    for (String value : dictionaryValues) {
      value = NullHandling.emptyToNullIfNeeded(value);
      if (value == null) {
        continue;
      }

      dictionaryWriter.write(value);
      globalDictionaryIdLookup.addString(value);
    }
    stringDictionarySerialized = true;
  }

  public void serializeLongDictionary(Iterable<Long> dictionaryValues) throws IOException
  {
    if (!stringDictionarySerialized) {
      throw new ISE("Must serialize string value dictionary before serializing long dictionary for column [%s]", name);
    }
    if (longDictionarySerialized) {
      throw new ISE("Long dictionary already serialized for column [%s], cannot serialize again", name);
    }
    for (Long value : dictionaryValues) {
      if (value == null) {
        continue;
      }
      longDictionaryWriter.write(value);
      globalDictionaryIdLookup.addLong(value);
    }
    longDictionarySerialized = true;
  }

  public void serializeDoubleDictionary(Iterable<Double> dictionaryValues) throws IOException
  {
    if (!stringDictionarySerialized) {
      throw new ISE("Must serialize string value dictionary before serializing double dictionary for column [%s]", name);
    }
    if (!longDictionarySerialized) {
      throw new ISE("Must serialize long value dictionary before serializing double dictionary for column [%s]", name);
    }
    if (doubleDictionarySerialized) {
      throw new ISE("Double dictionary already serialized for column [%s], cannot serialize again", name);
    }
    for (Double value : dictionaryValues) {
      if (value == null) {
        continue;
      }
      doubleDictionaryWriter.write(value);
      globalDictionaryIdLookup.addDouble(value);
    }
    doubleDictionarySerialized = true;
  }

  public void serializeArrayDictionary(Iterable<int[]> dictionaryValues) throws IOException
  {
    if (!stringDictionarySerialized) {
      throw new ISE("Must serialize string value dictionary before serializing array dictionary for column [%s]", name);
    }
    if (!longDictionarySerialized) {
      throw new ISE("Must serialize long value dictionary before serializing array dictionary for column [%s]", name);
    }
    if (!doubleDictionarySerialized) {
      throw new ISE("Must serialize double value dictionary before serializing array dictionary for column [%s]", name);
    }
    if (arrayDictionarySerialized) {
      throw new ISE("Array dictionary already serialized for column [%s], cannot serialize again", name);
    }
    for (int[] value : dictionaryValues) {
      if (value == null) {
        continue;
      }
      arrayDictionaryWriter.write(value);
      globalDictionaryIdLookup.addArray(value);
    }
    arrayDictionarySerialized = true;
  }

  @Override
  public void serialize(ColumnValueSelector<? extends StructuredData> selector) throws IOException
  {
    if (!arrayDictionarySerialized) {
      throw new ISE("Must serialize value dictionaries before serializing values for column [%s]", name);
    }
    StructuredData data = StructuredData.wrap(selector.getObject());
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
    Preconditions.checkArgument(dictionaryWriter.isSorted(), "Dictionary not sorted?!?");
    // version 5
    channel.write(ByteBuffer.wrap(new byte[]{0x05}));
    channel.write(ByteBuffer.wrap(metadataBytes));
    fieldsWriter.writeTo(channel, smoosher);
    fieldsInfoWriter.writeTo(channel, smoosher);

    // version 3 stores large components in separate files to prevent exceeding smoosh file limit (int max)
    writeInternal(smoosher, dictionaryWriter, STRING_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, longDictionaryWriter, LONG_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, doubleDictionaryWriter, DOUBLE_DICTIONARY_FILE_NAME);
    writeInternal(smoosher, arrayDictionaryWriter, ARRAY_DICTIONARY_FILE_NAME);
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
    public Integer read(ByteBuffer buffer, int offset)
    {
      return buffer.getInt(offset);
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
    public int compare(Object o1, Object o2)
    {
      return Integer.compare(((Number) o1).intValue(), ((Number) o2).intValue());
    }
  }
}
