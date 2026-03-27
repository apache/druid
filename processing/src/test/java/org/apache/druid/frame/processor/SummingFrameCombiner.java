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

package org.apache.druid.frame.processor;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameCursor;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;

/**
 * Simple test combiner that sums a long column at {@link #sumColumnNumber}.
 * All columns before {@link #sumColumnNumber} are treated as key columns.
 */
public class SummingFrameCombiner implements FrameCombiner
{
  private final RowSignature signature;
  private final int sumColumnNumber;

  private FrameReader frameReader;
  private FrameCursor keyCursor;
  private long summedValue;

  public SummingFrameCombiner(final RowSignature signature, final int sumColumnNumber)
  {
    this.signature = signature;
    this.sumColumnNumber = sumColumnNumber;
  }

  @Override
  public void init(final FrameReader frameReader)
  {
    this.frameReader = frameReader;
  }

  @Override
  public void reset(final Frame frame, final int row)
  {
    this.keyCursor = FrameProcessors.makeCursor(frame, frameReader);
    this.keyCursor.setCurrentRow(row);
    this.summedValue = readLongValue(frame, row);
  }

  @Override
  public void combine(final Frame frame, final int row)
  {
    this.summedValue += readLongValue(frame, row);
  }

  @Override
  public ColumnSelectorFactory getCombinedColumnSelectorFactory()
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(final DimensionSpec dimensionSpec)
      {
        final int columnNumber = signature.indexOf(dimensionSpec.getDimension());
        if (columnNumber < 0) {
          return DimensionSelector.constant(null, dimensionSpec.getExtractionFn());
        } else if (columnNumber == sumColumnNumber) {
          throw new UnsupportedOperationException();
        } else {
          return keyCursor.getColumnSelectorFactory().makeDimensionSelector(dimensionSpec);
        }
      }

      @Override
      public ColumnValueSelector<?> makeColumnValueSelector(final String columnName)
      {
        final int columnNumber = signature.indexOf(columnName);
        if (columnNumber < 0) {
          return NilColumnValueSelector.instance();
        } else if (columnNumber == sumColumnNumber) {
          return new ColumnValueSelector<Long>()
          {
            @Override
            public double getDouble()
            {
              return summedValue;
            }

            @Override
            public float getFloat()
            {
              return summedValue;
            }

            @Override
            public long getLong()
            {
              return summedValue;
            }

            @Override
            public boolean isNull()
            {
              return false;
            }

            @Override
            public Long getObject()
            {
              return summedValue;
            }

            @Override
            public Class<Long> classOfObject()
            {
              return Long.class;
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
              // Nothing to do.
            }
          };
        } else {
          return keyCursor.getColumnSelectorFactory().makeColumnValueSelector(columnName);
        }
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(final String column)
      {
        return signature.getColumnCapabilities(column);
      }
    };
  }

  private long readLongValue(final Frame frame, final int row)
  {
    final FrameCursor cursor = FrameProcessors.makeCursor(frame, frameReader);
    cursor.setCurrentRow(row);
    final String columnName = signature.getColumnName(sumColumnNumber);
    return cursor.getColumnSelectorFactory().makeColumnValueSelector(columnName).getLong();
  }
}
