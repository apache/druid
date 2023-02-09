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
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.StringFrontCodedDictionaryEncodedColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.FrontCodedIndexed;

import javax.annotation.Nullable;

/**
 * {@link DictionaryEncodedColumnSupplier} but for columns using a {@link StringFrontCodedDictionaryEncodedColumn}
 * instead of the traditional {@link org.apache.druid.segment.column.StringDictionaryEncodedColumn}
 */
public class StringFrontCodedDictionaryEncodedColumnSupplier implements Supplier<DictionaryEncodedColumn<?>>
{
  private final Supplier<FrontCodedIndexed> utf8Dictionary;
  private final @Nullable Supplier<ColumnarInts> singleValuedColumn;
  private final @Nullable Supplier<ColumnarMultiInts> multiValuedColumn;

  public StringFrontCodedDictionaryEncodedColumnSupplier(
      Supplier<FrontCodedIndexed> utf8Dictionary,
      @Nullable Supplier<ColumnarInts> singleValuedColumn,
      @Nullable Supplier<ColumnarMultiInts> multiValuedColumn
  )
  {
    this.utf8Dictionary = utf8Dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
  }

  @Override
  public DictionaryEncodedColumn<?> get()
  {
    return new StringFrontCodedDictionaryEncodedColumn(
        singleValuedColumn != null ? singleValuedColumn.get() : null,
        multiValuedColumn != null ? multiValuedColumn.get() : null,
        utf8Dictionary.get()
    );
  }
}
