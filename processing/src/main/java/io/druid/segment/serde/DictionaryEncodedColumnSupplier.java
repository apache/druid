/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.serde;

import com.google.common.base.Supplier;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.SimpleDictionaryEncodedColumn;
import io.druid.segment.data.CachingIndexed;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;

/**
*/
public class DictionaryEncodedColumnSupplier implements Supplier<DictionaryEncodedColumn>
{
  private final GenericIndexed<String> dictionary;
  private final Supplier<IndexedInts> singleValuedColumn;
  private final Supplier<IndexedMultivalue<IndexedInts>> multiValuedColumn;
  private final int lookupCacheSize;

  public DictionaryEncodedColumnSupplier(
      GenericIndexed<String> dictionary,
      Supplier<IndexedInts> singleValuedColumn,
      Supplier<IndexedMultivalue<IndexedInts>> multiValuedColumn,
      int lookupCacheSize
  )
  {
    this.dictionary = dictionary;
    this.singleValuedColumn = singleValuedColumn;
    this.multiValuedColumn = multiValuedColumn;
    this.lookupCacheSize = lookupCacheSize;
  }

  @Override
  public DictionaryEncodedColumn get()
  {
    return new SimpleDictionaryEncodedColumn(
        singleValuedColumn != null ? singleValuedColumn.get() : null,
        multiValuedColumn != null ? multiValuedColumn.get() : null,
        new CachingIndexed<>(dictionary, lookupCacheSize)
    );
  }
}
