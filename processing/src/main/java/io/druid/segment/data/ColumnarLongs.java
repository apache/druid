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
import io.druid.segment.LongColumnSelector;
import io.druid.segment.historical.HistoricalColumnSelector;

import java.io.Closeable;

/**
 * Resource that provides random access to a packed array of primitive longs. Backs up {@link
 * io.druid.segment.column.LongsColumn}.
 */
public interface ColumnarLongs extends Closeable
{
  int size();
  long get(int index);
  void fill(int index, long[] toFill);

  @Override
  void close();

  default ColumnValueSelector<Long> makeColumnValueSelector(ReadableOffset offset)
  {
    class HistoricalLongColumnSelector implements LongColumnSelector, HistoricalColumnSelector<Long>
    {
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
  }
}
