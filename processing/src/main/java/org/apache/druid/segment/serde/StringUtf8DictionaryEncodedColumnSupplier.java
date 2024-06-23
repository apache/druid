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

package org.apache.druid.segment.serde;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.column.ColumnPartSize;
import org.apache.druid.segment.column.ColumnPartSupplier;
import org.apache.druid.segment.column.ColumnSize;
import org.apache.druid.segment.column.ColumnSupplier;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Supplier for {@link StringUtf8DictionaryEncodedColumn}
 */
public class StringUtf8DictionaryEncodedColumnSupplier<TIndexed extends Indexed<ByteBuffer>> implements ColumnSupplier<DictionaryEncodedColumn<?>>
{
  private final ColumnPartSupplier<TIndexed> utf8Dictionary;
  private final @Nullable ColumnPartSupplier<ColumnarInts> singleValuedColumn;
  private final @Nullable ColumnPartSupplier<ColumnarMultiInts> multiValuedColumn;

  public StringUtf8DictionaryEncodedColumnSupplier(
      ColumnPartSupplier<TIndexed> utf8Dictionary,
      @Nullable ColumnPartSupplier<ColumnarInts> singleValuedColumn,
      @Nullable ColumnPartSupplier<ColumnarMultiInts> multiValuedColumn
  )
  {
    this.utf8Dictionary = utf8Dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
  }

  @Override
  public DictionaryEncodedColumn<?> get()
  {
    final TIndexed suppliedUtf8Dictionary = utf8Dictionary.get();

    if (NullHandling.mustCombineNullAndEmptyInDictionary(suppliedUtf8Dictionary)) {
      return new StringUtf8DictionaryEncodedColumn(
          singleValuedColumn != null ? new CombineFirstTwoValuesColumnarInts(singleValuedColumn.get()) : null,
          multiValuedColumn != null ? new CombineFirstTwoValuesColumnarMultiInts(multiValuedColumn.get()) : null,
          CombineFirstTwoEntriesIndexed.returnNull(suppliedUtf8Dictionary)
      );
    } else if (NullHandling.mustReplaceFirstValueWithNullInDictionary(suppliedUtf8Dictionary)) {
      return new StringUtf8DictionaryEncodedColumn(
          singleValuedColumn != null ? singleValuedColumn.get() : null,
          multiValuedColumn != null ? multiValuedColumn.get() : null,
          new ReplaceFirstValueWithNullIndexed<>(suppliedUtf8Dictionary)
      );
    } else {
      return new StringUtf8DictionaryEncodedColumn(
          singleValuedColumn != null ? singleValuedColumn.get() : null,
          multiValuedColumn != null ? multiValuedColumn.get() : null,
          suppliedUtf8Dictionary
      );
    }
  }

  @Override
  public Map<String, ColumnPartSize> getComponents()
  {
    return ImmutableMap.of(
        ColumnSize.ENCODED_VALUE_COLUMN_PART, singleValuedColumn != null ? singleValuedColumn.getColumnPartSize() : multiValuedColumn.getColumnPartSize(),
        ColumnSize.STRING_VALUE_DICTIONARY_COLUMN_PART, utf8Dictionary.getColumnPartSize()
    );
  }
}
