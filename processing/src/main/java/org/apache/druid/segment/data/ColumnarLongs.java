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
import org.apache.druid.segment.vector.BaseLongVectorValueSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorSelectorUtils;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * Resource that provides random access to a packed array of primitive longs. Backs up {@link
 * org.apache.druid.segment.column.LongsColumn}.
 */
public interface ColumnarLongs extends Closeable
{
  int size();

  long get(int index);

  default void get(long[] out, int start, int length)
  {
    for (int i = 0; i < length; i++) {
      out[i] = get(i + start);
    }
  }

  default void get(long[] out, int[] indexes, int length)
  {
    for (int i = 0; i < length; i++) {
      out[i] = get(indexes[i]);
    }
  }

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

  default VectorValueSelector makeVectorValueSelector(
      final ReadableVectorOffset theOffset,
      final ImmutableBitmap nullValueBitmap
  )
  {
    class ColumnarLongsVectorValueSelector extends BaseLongVectorValueSelector
    {
      private final long[] longVector;

      private int id = ReadableVectorOffset.NULL_ID;

      @Nullable
      private boolean[] nullVector = null;

      private ColumnarLongsVectorValueSelector()
      {
        super(theOffset);
        this.longVector = new long[offset.getMaxVectorSize()];
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        computeVectorsIfNeeded();
        return nullVector;
      }

      @Override
      public long[] getLongVector()
      {
        computeVectorsIfNeeded();
        return longVector;
      }

      private void computeVectorsIfNeeded()
      {
        if (id == offset.getId()) {
          return;
        }

        if (offset.isContiguous()) {
          ColumnarLongs.this.get(longVector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          ColumnarLongs.this.get(longVector, offset.getOffsets(), offset.getCurrentVectorSize());
        }

        nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullValueBitmap);

        id = offset.getId();
      }
    }

    return new ColumnarLongsVectorValueSelector();
  }
}
