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

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.serde.ColumnSerializerUtils;
import org.apache.druid.segment.serde.Serializer;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
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
  public static final String RAW_FILE_NAME = "__raw";
  public static final String NESTED_FIELD_PREFIX = "__field_";

  public abstract void openDictionaryWriter(File segmentBaseDir) throws IOException;

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

  public abstract DictionaryIdLookup getDictionaryIdLookup();

  public abstract void setDictionaryIdLookup(DictionaryIdLookup dictionaryIdLookup);

  public abstract boolean hasNulls();

  protected void writeInternal(FileSmoosher smoosher, Serializer serializer, String fileName) throws IOException
  {
    ColumnSerializerUtils.writeInternal(smoosher, serializer, getColumnName(), fileName);
  }

  protected static void copyFromTempSmoosh(FileSmoosher smoosher, SmooshedFileMapper fileMapper) throws IOException
  {
    for (String internalName : fileMapper.getInternalFilenames()) {
      smoosher.add(internalName, fileMapper.mapFile(internalName));
    }
  }

  public static void writeV0Header(WritableByteChannel channel, ByteBuffer columnNameBuffer) throws IOException
  {
    channel.write(ByteBuffer.wrap(new byte[]{NestedCommonFormatColumnSerializer.V0}));
    channel.write(columnNameBuffer);
  }

  protected ByteBuffer computeFilenameBytes()
  {
    final String columnName = getColumnName();
    return ColumnSerializerUtils.stringToUtf8InVSizeByteBuffer(columnName);
  }
}
