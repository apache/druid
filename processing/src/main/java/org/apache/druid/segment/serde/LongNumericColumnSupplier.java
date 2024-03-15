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
import org.apache.druid.segment.column.ColumnPartSize;
import org.apache.druid.segment.column.ColumnSize;
import org.apache.druid.segment.column.ColumnSupplier;
import org.apache.druid.segment.column.LongsColumn;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;

import java.util.Map;

/**
 */
public class LongNumericColumnSupplier implements ColumnSupplier<NumericColumn>
{
  private final CompressedColumnarLongsSupplier column;
  private final ImmutableBitmap nullValueBitmap;

  public LongNumericColumnSupplier(CompressedColumnarLongsSupplier column, ImmutableBitmap nullValueBitmap)
  {
    this.column = column;
    this.nullValueBitmap = nullValueBitmap;
  }

  @Override
  public NumericColumn get()
  {
    return LongsColumn.create(column.get(), nullValueBitmap);
  }

  @Override
  public Map<String, ColumnPartSize> getComponents()
  {
    return ImmutableMap.of(
        ColumnSize.LONG_COLUMN_PART, column.getColumnPartSize()
    );
  }
}
