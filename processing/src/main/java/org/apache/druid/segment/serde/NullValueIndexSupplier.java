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
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnPartSize;
import org.apache.druid.segment.column.ColumnSize;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.SimpleImmutableBitmapIndex;
import org.apache.druid.segment.index.semantic.NullValueIndex;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;

/**
 * {@link ColumnIndexSupplier} for columns which only have an {@link ImmutableBitmap} to indicate which rows only have
 * null values, such as {@link LongNumericColumnPartSerdeV2}, {@link DoubleNumericColumnPartSerdeV2}, and
 * {@link FloatNumericColumnPartSerdeV2}.
 *
 */
public class NullValueIndexSupplier implements ColumnIndexSupplier
{
  private final int sizeBytes;
  private final SimpleImmutableBitmapIndex nullValueIndex;

  public NullValueIndexSupplier(ImmutableBitmap nullValueBitmap, int sizeBytes)
  {
    this.nullValueIndex = new SimpleImmutableBitmapIndex(nullValueBitmap);
    this.sizeBytes = sizeBytes;
  }

  @Nullable
  @Override
  public <T> T as(Class<T> clazz)
  {
    if (clazz.equals(NullValueIndex.class)) {
      return (T) new NullableNumericNullValueIndex();
    }
    return null;
  }

  @Override
  public Map<String, ColumnPartSize> getIndexComponents()
  {
    if (sizeBytes > 0) {
      return ImmutableMap.of(
          ColumnSize.NULL_VALUE_INDEX_COLUMN_PART, ColumnPartSize.simple("nullBitmap", sizeBytes)
      );
    }
    return Collections.emptyMap();
  }

  private final class NullableNumericNullValueIndex implements NullValueIndex
  {
    @Override
    public BitmapColumnIndex get()
    {
      return nullValueIndex;
    }
  }
}
