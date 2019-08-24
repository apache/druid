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
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.historical.HistoricalColumnSelector;
import org.apache.druid.segment.vector.BaseFloatVectorValueSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.VectorSelectorUtils;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * Resource that provides random access to a packed array of primitive floats. Backs up {@link
 * org.apache.druid.segment.column.FloatsColumn}.
 */
public interface ColumnarFloats extends Closeable
{
  int size();

  float get(int index);

  default void get(float[] out, int start, int length)
  {
    for (int i = 0; i < length; i++) {
      out[i] = get(i + start);
    }
  }

  default void get(float[] out, int[] indexes, int length)
  {
    for (int i = 0; i < length; i++) {
      out[i] = get(indexes[i]);
    }
  }

  @Override
  void close();

  default ColumnValueSelector<Float> makeColumnValueSelector(ReadableOffset offset, ImmutableBitmap nullValueBitmap)
  {
    if (nullValueBitmap.isEmpty()) {
      class HistoricalFloatColumnSelector implements FloatColumnSelector, HistoricalColumnSelector<Float>
      {
        @Override
        public boolean isNull()
        {
          return false;
        }

        @Override
        public float getFloat()
        {
          return ColumnarFloats.this.get(offset.getOffset());
        }

        @Override
        public double getDouble(int offset)
        {
          return ColumnarFloats.this.get(offset);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("columnar", ColumnarFloats.this);
          inspector.visit("offset", offset);
        }
      }
      return new HistoricalFloatColumnSelector();
    } else {
      class HistoricalFloatColumnSelectorwithNulls implements FloatColumnSelector, HistoricalColumnSelector<Float>
      {
        @Override
        public boolean isNull()
        {
          return nullValueBitmap.get(offset.getOffset());
        }

        @Override
        public float getFloat()
        {
          assert NullHandling.replaceWithDefault() || !isNull();
          return ColumnarFloats.this.get(offset.getOffset());
        }

        @Override
        public double getDouble(int offset)
        {
          assert NullHandling.replaceWithDefault() || !nullValueBitmap.get(offset);
          return ColumnarFloats.this.get(offset);
        }

        @Override
        public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
          inspector.visit("columnar", ColumnarFloats.this);
          inspector.visit("offset", offset);
          inspector.visit("nullValueBitmap", nullValueBitmap);
        }
      }
      return new HistoricalFloatColumnSelectorwithNulls();
    }
  }

  default VectorValueSelector makeVectorValueSelector(
      final ReadableVectorOffset theOffset,
      final ImmutableBitmap nullValueBitmap
  )
  {
    class ColumnarFloatsVectorValueSelector extends BaseFloatVectorValueSelector
    {
      private final float[] floatVector;

      private int id = ReadableVectorOffset.NULL_ID;

      @Nullable
      private boolean[] nullVector = null;

      private ColumnarFloatsVectorValueSelector()
      {
        super(theOffset);
        this.floatVector = new float[offset.getMaxVectorSize()];
      }

      @Nullable
      @Override
      public boolean[] getNullVector()
      {
        computeVectorsIfNeeded();
        return nullVector;
      }

      @Override
      public float[] getFloatVector()
      {
        computeVectorsIfNeeded();
        return floatVector;
      }

      private void computeVectorsIfNeeded()
      {
        if (id == offset.getId()) {
          return;
        }

        if (offset.isContiguous()) {
          ColumnarFloats.this.get(floatVector, offset.getStartOffset(), offset.getCurrentVectorSize());
        } else {
          ColumnarFloats.this.get(floatVector, offset.getOffsets(), offset.getCurrentVectorSize());
        }

        nullVector = VectorSelectorUtils.populateNullVector(nullVector, offset, nullValueBitmap);

        id = offset.getId();
      }
    }

    return new ColumnarFloatsVectorValueSelector();
  }
}
