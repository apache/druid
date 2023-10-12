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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.data.VByte;
import org.apache.druid.segment.serde.Serializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.SortedMap;

/**
 * Basic serializer implementation for the {@link NestedCommonFormatColumn} family of columns. The
 * {@link org.apache.druid.segment.AutoTypeColumnIndexer} catalogs the types and fields present in the data it processes
 * using a {@link StructuredDataProcessor}. When persisting and merging segments, the
 * {@link org.apache.druid.segment.AutoTypeColumnMerger} will choose the most appropriate serializer based on the data
 * which was processed as follows:
 *
 * @see ScalarDoubleColumnSerializer  - single type double columns
 * @see ScalarLongColumnSerializer    - single type long columns
 * @see ScalarStringColumnSerializer  - single type string columns
 * @see VariantColumnSerializer       - single type array columns of string, long, double, and mixed type columns
 *
 * @see NestedDataColumnSerializer    - nested columns
 *
 */
public abstract class NestedCommonFormatColumnSerializer implements GenericColumnSerializer<StructuredData>
{
  public static final byte V0 = 0x00;
  public static final String STRING_DICTIONARY_FILE_NAME = "__stringDictionary";
  public static final String LONG_DICTIONARY_FILE_NAME = "__longDictionary";
  public static final String DOUBLE_DICTIONARY_FILE_NAME = "__doubleDictionary";
  public static final String ARRAY_DICTIONARY_FILE_NAME = "__arrayDictionary";
  public static final String ARRAY_ELEMENT_DICTIONARY_FILE_NAME = "__arrayElementDictionary";
  public static final String ENCODED_VALUE_COLUMN_FILE_NAME = "__encodedColumn";
  public static final String LONG_VALUE_COLUMN_FILE_NAME = "__longColumn";
  public static final String DOUBLE_VALUE_COLUMN_FILE_NAME = "__doubleColumn";
  public static final String BITMAP_INDEX_FILE_NAME = "__valueIndexes";
  public static final String ARRAY_ELEMENT_BITMAP_INDEX_FILE_NAME = "__arrayElementIndexes";
  public static final String RAW_FILE_NAME = "__raw";
  public static final String NULL_BITMAP_FILE_NAME = "__nullIndex";
  public static final String NESTED_FIELD_PREFIX = "__field_";

  public abstract void openDictionaryWriter() throws IOException;

  public void serializeFields(SortedMap<String, FieldTypeInfo.MutableTypeSet> fields) throws IOException
  {
    // nothing to do unless we actually have nested fields
  }

  public abstract void serializeDictionaries(
      Iterable<String> strings,
      Iterable<Long> longs,
      Iterable<Double> doubles,
      Iterable<int[]> arrays
  ) throws IOException;

  public abstract String getColumnName();

  public abstract DictionaryIdLookup getGlobalLookup();

  public abstract boolean hasNulls();

  protected void writeInternal(FileSmoosher smoosher, Serializer serializer, String fileName) throws IOException
  {
    final String internalName = getInternalFileName(getColumnName(), fileName);
    try (SmooshedWriter smooshChannel = smoosher.addWithSmooshedWriter(internalName, serializer.getSerializedSize())) {
      serializer.writeTo(smooshChannel, smoosher);
    }
  }

  protected void writeInternal(FileSmoosher smoosher, ByteBuffer buffer, String fileName) throws IOException
  {
    final String internalName = getInternalFileName(getColumnName(), fileName);
    try (SmooshedWriter smooshChannel = smoosher.addWithSmooshedWriter(internalName, buffer.capacity())) {
      smooshChannel.write(buffer);
    }
  }

  protected void writeV0Header(WritableByteChannel channel, ByteBuffer columnNameBuffer) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{NestedCommonFormatColumnSerializer.V0}));
    channel.write(columnNameBuffer);
  }

  protected ByteBuffer computeFilenameBytes()
  {
    final String columnName = getColumnName();
    final byte[] bytes = StringUtils.toUtf8(columnName);
    final int length = VByte.computeIntSize(bytes.length);
    final ByteBuffer buffer = ByteBuffer.allocate(length + bytes.length).order(ByteOrder.nativeOrder());
    VByte.writeInt(buffer, bytes.length);
    buffer.put(bytes);
    buffer.flip();
    return buffer;
  }

  /**
   * Nested field columns are stored in separate
   */
  public static String getInternalFileName(String fileNameBase, String field)
  {
    return StringUtils.format("%s.%s", fileNameBase, field);
  }
}
