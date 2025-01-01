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

import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Supplier for {@link StringUtf8DictionaryEncodedColumn}
 */
public class StringUtf8DictionaryEncodedColumnSupplier<TIndexed extends Indexed<ByteBuffer>> implements Supplier<DictionaryEncodedColumn<?>>
{
  private final Supplier<TIndexed> utf8Dictionary;
  private final @Nullable Supplier<ColumnarInts> singleValuedColumn;
  private final @Nullable Supplier<ColumnarMultiInts> multiValuedColumn;
  private final BitmapFactory bitmapFactory;

  public StringUtf8DictionaryEncodedColumnSupplier(
      Supplier<TIndexed> utf8Dictionary,
      @Nullable Supplier<ColumnarInts> singleValuedColumn,
      @Nullable Supplier<ColumnarMultiInts> multiValuedColumn,
      BitmapFactory bitmapFactory
  )
  {
    this.utf8Dictionary = utf8Dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
    this.bitmapFactory = bitmapFactory;
  }

  public Supplier<TIndexed> getDictionary()
  {
    return utf8Dictionary;
  }

  @Override
  public DictionaryEncodedColumn<?> get()
  {
    final TIndexed suppliedUtf8Dictionary = utf8Dictionary.get();

    if (NullHandling.mustCombineNullAndEmptyInDictionary(suppliedUtf8Dictionary)) {
      return new StringUtf8DictionaryEncodedColumn(
          singleValuedColumn != null ? new CombineFirstTwoValuesColumnarInts(singleValuedColumn.get()) : null,
          multiValuedColumn != null ? new CombineFirstTwoValuesColumnarMultiInts(multiValuedColumn.get()) : null,
          CombineFirstTwoEntriesIndexed.returnNull(suppliedUtf8Dictionary),
          bitmapFactory
      );
    } else if (NullHandling.mustReplaceFirstValueWithNullInDictionary(suppliedUtf8Dictionary)) {
      return new StringUtf8DictionaryEncodedColumn(
          singleValuedColumn != null ? singleValuedColumn.get() : null,
          multiValuedColumn != null ? multiValuedColumn.get() : null,
          new ReplaceFirstValueWithNullIndexed<>(suppliedUtf8Dictionary),
          bitmapFactory
      );
    } else {
      return new StringUtf8DictionaryEncodedColumn(
          singleValuedColumn != null ? singleValuedColumn.get() : null,
          multiValuedColumn != null ? multiValuedColumn.get() : null,
          suppliedUtf8Dictionary,
          bitmapFactory
      );
    }
  }
}
