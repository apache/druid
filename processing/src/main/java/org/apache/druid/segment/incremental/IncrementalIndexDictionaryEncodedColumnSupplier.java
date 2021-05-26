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

package org.apache.druid.segment.incremental;

import com.google.common.base.Supplier;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.StringDimensionIndexer;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;

/**
 */
public class IncrementalIndexDictionaryEncodedColumnSupplier implements Supplier<DictionaryEncodedColumn<?>>
{
  private final StringDimensionIndexer stringDimensionIndexer;

  public IncrementalIndexDictionaryEncodedColumnSupplier(
      StringDimensionIndexer stringDimensionIndexer
  )
  {
    this.stringDimensionIndexer = stringDimensionIndexer;
  }

  @Override
  public DictionaryEncodedColumn<String> get()
  {
    return new DictionaryEncodedColumn<String>()
    {
      @Override
      public int length()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean hasMultipleValues()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getSingleValueRow(int rowNum)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public IndexedInts getMultiValueRow(int rowNum)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public String lookupName(int id)
      {
        return stringDimensionIndexer.getValue(id);
      }

      @Override
      public int lookupId(String name)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getCardinality()
      {
        return stringDimensionIndexer.getCardinality();
      }

      @Override
      public DimensionSelector makeDimensionSelector(
          ReadableOffset offset,
          @Nullable ExtractionFn extractionFn
      )
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close()
      {
      }
    };
  }
}
