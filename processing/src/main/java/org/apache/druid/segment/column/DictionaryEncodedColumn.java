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

package org.apache.druid.segment.column;

import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 *
 */
public interface DictionaryEncodedColumn<ActualType extends Comparable<? super ActualType>> extends BaseColumn
{
  int length();

  boolean hasMultipleValues();

  int getSingleValueRow(int rowNum);

  IndexedInts getMultiValueRow(int rowNum);

  @Nullable
  ActualType lookupName(int id);

  /**
   * Returns a ByteBuffer pointing to the underlying bytes for the value of a particular dictionary id.
   *
   * The returned buffer is in big-endian order. It is not reused, so callers may modify the position, limit, byte
   * order, etc of the buffer.
   *
   * The returned buffer points to the original data, so callers must take care not to use it outside the valid
   * lifetime of this column.
   *
   * @param id id to lookup the dictionary value for
   *
   * @return dictionary value for the given id, or null if the value is itself null
   */
  @Nullable
  ByteBuffer lookupNameBinary(int id);

  int lookupId(ActualType name);

  int getCardinality();

  DimensionSelector makeDimensionSelector(ReadableOffset offset, @Nullable ExtractionFn extractionFn);

  @Override
  default ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return makeDimensionSelector(offset, null);
  }

  SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(ReadableVectorOffset vectorOffset);

  MultiValueDimensionVectorSelector makeMultiValueDimensionVectorSelector(ReadableVectorOffset vectorOffset);
}
