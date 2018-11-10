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

package org.apache.druid.segment.data;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.historical.HistoricalColumnSelector;

import java.io.Closeable;

/**
 * Resource that provides random access to a packed array of primitive longs. Backs up {@link
 * org.apache.druid.segment.column.LongsColumn}.
 */
public interface ColumnarLongs extends Closeable
{
  int size();

  long get(int index);

  void fill(int index, long[] toFill);

  @Override
  void close();

  default ColumnValueSelector<Long> makeColumnValueSelector(ReadableOffset offset, ImmutableBitmap nullValueBitmap)
  {
    if (nullValueBitmap.isEmpty()) {
      class HistoricalLongColumnSelector implements LongColumnSelector, HistoricalColumnSelector<Long>
      {
        @Override
        public boolean isNull()
        {
          return false;
        }

        @Override
        public long getLong()
        {
          return ColumnarLongs.this.get(offset.getOffset());
        }

        @Override
        public double getDouble(int offset)
        {
          return ColumnarLongs.this.get(offset);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("columnar", ColumnarLongs.this);
          inspector.visit("offset", offset);
        }
      }
      return new HistoricalLongColumnSelector();
    } else {
      class HistoricalLongColumnSelectorWithNulls implements LongColumnSelector, HistoricalColumnSelector<Long>
      {
        @Override
        public boolean isNull()
        {
          return nullValueBitmap.get(offset.getOffset());
        }

        @Override
        public long getLong()
        {
          assert NullHandling.replaceWithDefault() || !isNull();
          return ColumnarLongs.this.get(offset.getOffset());
        }

        @Override
        public double getDouble(int offset)
        {
          assert NullHandling.replaceWithDefault() || !nullValueBitmap.get(offset);
          return ColumnarLongs.this.get(offset);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("columnar", ColumnarLongs.this);
          inspector.visit("offset", offset);
          inspector.visit("nullValueBitmap", nullValueBitmap);
        }
      }
      return new HistoricalLongColumnSelectorWithNulls();
    }
  }
}
