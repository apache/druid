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
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringEncodingStrategies;
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
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * Serializer for {@link NestedCommonFormatColumn} which can store nested data. The serializer stores several components
 * including:
 * - a field list and associated type info
 * - value dictionaries for string, long, double, and array values (where the arrays are stored as int[] that point to
 *   the string, long, and double values)
 * - raw data is stored with a {@link CompressedVariableSizedBlobColumnSerializer} as blobs of SMILE encoded data
 * - a null value bitmap to track which 'raw' rows are null
 *
 * For each nested field, a {@link GlobalDictionaryEncodedFieldColumnWriter} will write a sub-column to specialize
 * fast reading and filtering of that path.
 *
 * @see ScalarDoubleFieldColumnWriter - single type double field
 * @see ScalarLongFieldColumnWriter   - single type long field
 * @see ScalarStringFieldColumnWriter - single type string field
 * @see VariantArrayFieldColumnWriter - single type array of string, long, and double field
 * @see VariantFieldColumnWriter      - mixed type field
 */
public class NestedDataColumnSerializer extends NestedCommonFormatColumnSerializer
{
  private static final Logger log = new Logger(NestedDataColumnSerializer.class);

  private final String name;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final IndexSpec indexSpec;
  @SuppressWarnings("unused")
  private final Closer closer;

  private final StructuredDataProcessor fieldProcessor = new StructuredDataProcessor()
  {
    @Override
    public ProcessedValue<?> processField(ArrayList<NestedPathPart> fieldPath, @Nullable Object fieldValue)
    {
      final GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.get(
          NestedPathFinder.toNormalizedJsonPath(fieldPath)
      );
      if (writer != null) {
        try {
          final ExprEval<?> eval = ExprEval.bestEffortOf(fieldValue);
          if (eval.type().isPrimitive() || eval.type().isPrimitiveArray()) {
            writer.addValue(rowCount, eval.value());
          } else {
            // behave consistently with nested column indexer, which defaults to string
            writer.addValue(rowCount, eval.asString());
          }
          // serializer doesn't use size estimate
          return ProcessedValue.NULL_LITERAL;
        }
        catch (IOException e) {
          throw new RE(e, "Failed to write field [%s], unhandled value", fieldPath);
        }
      }
      return ProcessedValue.NULL_LITERAL;
    }

    @Nullable
    @Override
    public ProcessedValue<?> processArrayField(
        ArrayList<NestedPathPart> fieldPath,
        @Nullable List<?> array
    )
    {
      final ExprEval<?> eval = ExprEval.bestEffortArray(array);
      if (eval.type().isPrimitiveArray()) {
        final GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.get(
            NestedPathFinder.toNormalizedJsonPath(fieldPath)
        );
        if (writer != null) {
          try {
            writer.addValue(rowCount, eval.value());
            // serializer doesn't use size estimate
            return ProcessedValue.NULL_LITERAL;
          }
          catch (IOException e) {
            throw new RE(e, "Failed to write field [%s] value [%s]", fieldPath, array);
          }
        }
      }
      return null;
    }
  };

  private DictionaryIdLookup globalDictionaryIdLookup;
  private SortedMap<String, FieldTypeInfo.MutableTypeSet> fields;
  private GenericIndexedWriter<String> fieldsWriter;
  private FieldTypeInfo.Writer fieldsInfoWriter;
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

  private boolean dictionarySerialized = false;
  private ByteBuffer columnNameBytes = null;

  public NestedDataColumnSerializer(
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

  @Override
  public String getColumnName()
  {
    return name;
  }

  @Override
  public DictionaryIdLookup getGlobalLookup()
  {
    return globalDictionaryIdLookup;
  }

  @Override
  public boolean hasNulls()
  {
    return !nullRowsBitmap.isEmpty();
  }

  @Override
  public void openDictionaryWriter() throws IOException
  {
    fieldsWriter = new GenericIndexedWriter<>(segmentWriteOutMedium, name, GenericIndexed.STRING_STRATEGY);
    fieldsWriter.open();

    fieldsInfoWriter = new FieldTypeInfo.Writer(segmentWriteOutMedium);
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
    globalDictionaryIdLookup = closer.register(
        new DictionaryIdLookup(
            name,
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

  @Override
  public void serializeFields(SortedMap<String, FieldTypeInfo.MutableTypeSet> fields) throws IOException
  {
    this.fields = fields;
    this.fieldWriters = Maps.newHashMapWithExpectedSize(fields.size());
    int ctr = 0;
    for (Map.Entry<String, FieldTypeInfo.MutableTypeSet> field : fields.entrySet()) {
      final String fieldName = field.getKey();
      final String fieldFileName = NESTED_FIELD_PREFIX + ctr++;
      fieldsWriter.write(fieldName);
      fieldsInfoWriter.write(field.getValue());
      final GlobalDictionaryEncodedFieldColumnWriter<?> writer;
      final ColumnType type = field.getValue().getSingleType();
      if (type != null) {
        if (Types.is(type, ValueType.STRING)) {
          writer = new ScalarStringFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else if (Types.is(type, ValueType.LONG)) {
          writer = new ScalarLongFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else if (Types.is(type, ValueType.DOUBLE)) {
          writer = new ScalarDoubleFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else if (Types.is(type, ValueType.ARRAY)) {
          writer = new VariantArrayFieldColumnWriter(
              name,
              fieldFileName,
              segmentWriteOutMedium,
              indexSpec,
              globalDictionaryIdLookup
          );
        } else {
          throw new ISE("Invalid field type [%s], how did this happen?", type);
        }
      } else {
        writer = new VariantFieldColumnWriter(
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
    for (String value : strings) {
      value = NullHandling.emptyToNullIfNeeded(value);
      if (value == null) {
        continue;
      }

      dictionaryWriter.write(value);
    }
    dictionarySerialized = true;

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

  private void closeForWrite() throws IOException
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
      nullBitmapWriter.write(nullRowsBitmap);
      columnNameBytes = computeFilenameBytes();
    }
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    closeForWrite();

    long size = 1 + columnNameBytes.capacity();
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

    writeV0Header(channel, columnNameBytes);
    fieldsWriter.writeTo(channel, smoosher);
    fieldsInfoWriter.writeTo(channel, smoosher);


    if (globalDictionaryIdLookup.getStringBufferMapper() != null) {
      SmooshedFileMapper fileMapper = globalDictionaryIdLookup.getStringBufferMapper();
      for (String internalName : fileMapper.getInternalFilenames()) {
        smoosher.add(internalName, fileMapper.mapFile(internalName));
      }
    } else {
      writeInternal(smoosher, dictionaryWriter, STRING_DICTIONARY_FILE_NAME);
    }
    if (globalDictionaryIdLookup.getLongBuffer() != null) {
      writeInternal(smoosher, globalDictionaryIdLookup.getLongBuffer(), LONG_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, longDictionaryWriter, LONG_DICTIONARY_FILE_NAME);
    }
    if (globalDictionaryIdLookup.getDoubleBuffer() != null) {
      writeInternal(smoosher, globalDictionaryIdLookup.getDoubleBuffer(), DOUBLE_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, doubleDictionaryWriter, DOUBLE_DICTIONARY_FILE_NAME);
    }
    if (globalDictionaryIdLookup.getArrayBuffer() != null) {
      writeInternal(smoosher, globalDictionaryIdLookup.getArrayBuffer(), ARRAY_DICTIONARY_FILE_NAME);
    } else {
      writeInternal(smoosher, arrayDictionaryWriter, ARRAY_DICTIONARY_FILE_NAME);
    }
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

    for (Map.Entry<String, FieldTypeInfo.MutableTypeSet> field : fields.entrySet()) {
      // remove writer so that it can be collected when we are done with it
      GlobalDictionaryEncodedFieldColumnWriter<?> writer = fieldWriters.remove(field.getKey());
      writer.writeTo(rowCount, smoosher);
    }
    log.info("Column [%s] serialized successfully with [%d] nested columns.", name, fields.size());
  }
}
