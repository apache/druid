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

package io.druid.segment.column;

import com.google.common.base.Strings;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.segment.data.CachingIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.IndexedMultivalue;

import java.io.IOException;

/**
*/
public class SimpleDictionaryEncodedColumn
    implements DictionaryEncodedColumn<String>
{
  private final IndexedInts column;
  private final IndexedMultivalue<IndexedInts> multiValueColumn;
  private final CachingIndexed<String> cachedLookups;

  public SimpleDictionaryEncodedColumn(
      IndexedInts singleValueColumn,
      IndexedMultivalue<IndexedInts> multiValueColumn,
      CachingIndexed<String> cachedLookups
  )
  {
    this.column = singleValueColumn;
    this.multiValueColumn = multiValueColumn;
    this.cachedLookups = cachedLookups;
  }

  @Override
  public int length()
  {
    return hasMultipleValues() ? multiValueColumn.size() : column.size();
  }

  @Override
  public boolean hasMultipleValues()
  {
    return column == null;
  }

  @Override
  public int getSingleValueRow(int rowNum)
  {
    return column.get(rowNum);
  }

  @Override
  public IndexedInts getMultiValueRow(int rowNum)
  {
    return multiValueColumn.get(rowNum);
  }

  @Override
  public String lookupName(int id)
  {
    //Empty to Null will ensure that null and empty are equivalent for extraction function
    return Strings.emptyToNull(cachedLookups.get(id));
  }

  @Override
  public int lookupId(String name)
  {
    return cachedLookups.indexOf(name);
  }

  @Override
  public int getCardinality()
  {
    return cachedLookups.size();
  }

  @Override
  public void close() throws IOException
  {
    CloseQuietly.close(cachedLookups);

    if(column != null) {
      column.close();
    }
    if(multiValueColumn != null) {
      multiValueColumn.close();
    }
  }
}
