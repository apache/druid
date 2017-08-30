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
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.historical.HistoricalFloatColumnSelector;

import java.io.Closeable;

public interface IndexedDoubles extends Closeable
{
  public int size();
  public double get(int index);
  public void fill(int index, double[] toFill);

  @Override
  void close();

  default DoubleColumnSelector makeDoubleColumnSelector(ReadableOffset offset)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public double getDouble()
      {
        return IndexedDoubles.this.get(offset.getOffset());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("indexed", IndexedDoubles.this);
        inspector.visit("offset", offset);
      }
    };
  }

  default HistoricalFloatColumnSelector makeFloatColumnSelector(ReadableOffset offset)
  {
    return new HistoricalFloatColumnSelector()
    {
      @Override
      public float get(int offset)
      {
        return (float) IndexedDoubles.this.get(offset);
      }

      @Override
      public float getFloat()
      {
        return (float) IndexedDoubles.this.get(offset.getOffset());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("indexed", IndexedDoubles.this);
        inspector.visit("offset", offset);
      }
    };
  }

  default LongColumnSelector makeLongColumnSelector(ReadableOffset offset)
  {
    return new LongColumnSelector()
    {
      @Override
      public long getLong()
      {
        return (long) IndexedDoubles.this.get(offset.getOffset());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("indexed", IndexedDoubles.this);
        inspector.visit("offset", offset);
      }
    };
  }
}

