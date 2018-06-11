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

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.data.ColumnarFloats;
import io.druid.segment.data.ReadableOffset;

/**
 */
public class FloatsColumn implements GenericColumn
{
  /**
   * Factory method to create FloatsColumn.
   */
  public static FloatsColumn create(ColumnarFloats column, ImmutableBitmap nullValueBitmap)
  {
    if (nullValueBitmap.isEmpty()) {
      return new FloatsColumn(column);
    } else {
      return new FloatsColumnWithNulls(column, nullValueBitmap);
    }
  }

  final ColumnarFloats column;

  FloatsColumn(final ColumnarFloats column)
  {
    this.column = column;
  }

  @Override
  public int length()
  {
    return column.size();
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(ReadableOffset offset)
  {
    return column.makeColumnValueSelector(offset, IndexIO.LEGACY_FACTORY.getBitmapFactory().makeEmptyImmutableBitmap());
  }

  @Override
  public long getLongSingleValueRow(int rowNum)
  {
    return (long) column.get(rowNum);
  }

  @Override
  public void close()
  {
    column.close();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("column", column);
  }
}
