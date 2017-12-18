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

package io.druid.segment.data;

import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.historical.HistoricalColumnSelector;

import java.io.Closeable;

/**
 * Resource that provides random access to a packed array of primitive floats. Backs up {@link
 * io.druid.segment.column.FloatsColumn}.
 */
public interface ColumnarFloats extends Closeable
{
  int size();
  float get(int index);
  void fill(int index, float[] toFill);

  @Override
  void close();

  default ColumnValueSelector<Float> makeColumnValueSelector(ReadableOffset offset)
  {
    class HistoricalFloatColumnSelector implements FloatColumnSelector, HistoricalColumnSelector<Float>
    {
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
  }
}
