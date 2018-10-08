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

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.ReadableOffset;

/**
 * LongsColumn with null values.
 */
class LongsColumnWithNulls extends LongsColumn
{
  private final ImmutableBitmap nullValueBitmap;

  LongsColumnWithNulls(ColumnarLongs columnarLongs, ImmutableBitmap nullValueBitmap)
  {
    super(columnarLongs);
    this.nullValueBitmap = nullValueBitmap;
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(ReadableOffset offset)
  {
    return column.makeColumnValueSelector(offset, nullValueBitmap);
  }

  @Override
  public long getLongSingleValueRow(int rowNum)
  {
    assert !nullValueBitmap.get(rowNum);
    return super.getLongSingleValueRow(rowNum);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    super.inspectRuntimeShape(inspector);
    inspector.visit("nullValueBitmap", nullValueBitmap);
  }
}
