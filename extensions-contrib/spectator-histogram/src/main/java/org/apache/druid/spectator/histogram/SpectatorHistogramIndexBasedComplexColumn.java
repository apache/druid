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

package org.apache.druid.spectator.histogram;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;

public class SpectatorHistogramIndexBasedComplexColumn implements ComplexColumn
{
  private final SpectatorHistogramIndexed index;
  private final String typeName;
  private static final Number ZERO = 0;

  public SpectatorHistogramIndexBasedComplexColumn(String typeName, SpectatorHistogramIndexed index)
  {
    this.index = index;
    this.typeName = typeName;
  }

  @Override
  public Class<?> getClazz()
  {
    return index.getClazz();
  }

  @Override
  public String getTypeName()
  {
    return typeName;
  }

  @Override
  public Object getRowValue(int rowNum)
  {
    return index.get(rowNum);
  }

  @Override
  public int getLength()
  {
    return index.size();
  }

  @Override
  public void close()
  {
  }

  @Override
  public ColumnValueSelector<SpectatorHistogram> makeColumnValueSelector(ReadableOffset offset)
  {
    // Use ColumnValueSelector directly so that we support being queried as a Number using
    // longSum or doubleSum aggregators, the NullableNumericBufferAggregator will call isNull.
    // This allows us to behave as a Number or SpectatorHistogram object.
    // When queried as a Number, we're returning the count of entries in the histogram.
    // As such, we can safely return 0 where the histogram is null.
    return new ColumnValueSelector<SpectatorHistogram>()
    {
      @Override
      public boolean isNull()
      {
        return getObject() == null;
      }

      private Number getOrZero()
      {
        SpectatorHistogram histogram = getObject();
        return histogram != null ? histogram : ZERO;
      }

      @Override
      public long getLong()
      {
        return getOrZero().longValue();
      }

      @Override
      public float getFloat()
      {
        return getOrZero().floatValue();
      }

      @Override
      public double getDouble()
      {
        return getOrZero().doubleValue();
      }

      @Nullable
      @Override
      public SpectatorHistogram getObject()
      {
        return (SpectatorHistogram) getRowValue(offset.getOffset());
      }

      @Override
      public Class classOfObject()
      {
        return getClazz();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", SpectatorHistogramIndexBasedComplexColumn.this);
      }
    };
  }
}
