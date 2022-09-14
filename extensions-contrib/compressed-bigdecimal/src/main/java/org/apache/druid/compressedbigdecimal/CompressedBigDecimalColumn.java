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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarMultiInts;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Accumulating Big Decimal column in druid segment.
 */
public class CompressedBigDecimalColumn implements ComplexColumn
{
  public static final Logger LOGGER = new Logger(CompressedBigDecimalColumn.class);

  private final ColumnarInts scale;
  private final ColumnarMultiInts magnitude;

  /**
   * Constructor.
   *
   * @param scale     scale of the rows
   * @param magnitude LongColumn representing magnitudes
   */
  public CompressedBigDecimalColumn(ColumnarInts scale, ColumnarMultiInts magnitude)
  {
    this.scale = scale;
    this.magnitude = magnitude;
  }

  @Override
  public Class<CompressedBigDecimalColumn> getClazz()
  {
    return CompressedBigDecimalColumn.class;
  }

  @Override
  public String getTypeName()
  {
    return CompressedBigDecimalModule.COMPRESSED_BIG_DECIMAL;
  }

  @Override
  public CompressedBigDecimal getRowValue(int rowNum)
  {
    int s = scale.get(rowNum);

    // make a copy of the value from magnitude because the IndexedInts returned
    // from druid is mutable and druid reuses the object for future calls.
    IndexedInts vals = magnitude.get(rowNum);
    int size = vals.size();
    int[] array = new int[size];
    for (int ii = 0; ii < size; ++ii) {
      array[ii] = vals.get(ii);
    }

    return ArrayCompressedBigDecimal.wrap(array, s);
  }

  @Override
  public int getLength()
  {
    return scale.size();
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(final ReadableOffset offset)
  {
    return new ObjectColumnSelector<CompressedBigDecimal>()
    {
      @Override @Nullable
      public CompressedBigDecimal getObject()
      {
        return getRowValue(offset.getOffset());
      }

      @Override @SuppressWarnings("unchecked")
      public Class<CompressedBigDecimal> classOfObject()
      {
        return (Class<CompressedBigDecimal>) (Class) CompressedBigDecimal.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("column", CompressedBigDecimalColumn.this);
      }
    };
  }

  @Override
  public void close()
  {
    try {
      scale.close();
    }
    catch (IOException ex) {
      LOGGER.error(ex, "failed to clean up scale part of CompressedBigDecimalColumn");
    }
    try {
      magnitude.close();
    }
    catch (IOException ex) {
      LOGGER.error(ex, "failed to clean up magnitude part of CompressedBigDecimalColumn");
    }
  }
}
