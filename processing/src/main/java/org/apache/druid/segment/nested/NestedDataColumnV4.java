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

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CompressedVariableSizedBlobColumnSupplier;
import org.apache.druid.segment.data.FixedIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public final class NestedDataColumnV4<TStringDictionary extends Indexed<ByteBuffer>>
    extends CompressedNestedDataComplexColumn<TStringDictionary>
{
  public NestedDataColumnV4(
      String columnName,
      ColumnType logicalType,
      ColumnConfig columnConfig,
      CompressedVariableSizedBlobColumnSupplier compressedRawColumnSupplier,
      ImmutableBitmap nullValues,
      GenericIndexed<String> fields,
      FieldTypeInfo fieldInfo,
      Supplier<TStringDictionary> stringDictionary,
      Supplier<FixedIndexed<Long>> longDictionarySupplier,
      Supplier<FixedIndexed<Double>> doubleDictionarySupplier,
      SmooshedFileMapper fileMapper,
      BitmapSerdeFactory bitmapSerdeFactory,
      ByteOrder byteOrder
  )
  {
    super(
        columnName,
        logicalType,
        columnConfig,
        compressedRawColumnSupplier,
        nullValues,
        fields,
        fieldInfo,
        stringDictionary,
        longDictionarySupplier,
        doubleDictionarySupplier,
        null,
        fileMapper,
        bitmapSerdeFactory,
        byteOrder,
        NestedPathFinder.JSON_PATH_ROOT
    );
  }

  @Override
  public List<NestedPathPart> parsePath(String path)
  {
    return NestedPathFinder.parseJsonPath(path);
  }

  @Override
  public String getFieldFileName(String fileNameBase, String field, int fieldIndex)
  {
    return NestedCommonFormatColumnSerializer.getInternalFileName(
        fileNameBase,
        NestedCommonFormatColumnSerializer.NESTED_FIELD_PREFIX + fieldIndex
    );
  }

  @Override
  public String getField(List<NestedPathPart> path)
  {
    return NestedPathFinder.toNormalizedJsonPath(path);
  }
}
