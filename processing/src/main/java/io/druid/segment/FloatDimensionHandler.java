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

package io.druid.segment;

import io.druid.common.config.NullHandling;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.data.Indexed;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.Closeable;

public class FloatDimensionHandler implements DimensionHandler<Float, Float, Float>
{
  private final String dimensionName;

  public FloatDimensionHandler(String dimensionName)
  {
    this.dimensionName = dimensionName;
  }

  @Override
  public String getDimensionName()
  {
    return dimensionName;
  }

  @Override
  public DimensionIndexer<Float, Float, Float> makeIndexer()
  {
    return new FloatDimensionIndexer();
  }

  @Override
  public DimensionMergerV9<Float> makeMerger(
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress
  )
  {
    return new FloatDimensionMergerV9(
        dimensionName,
        indexSpec,
        segmentWriteOutMedium
    );
  }

  @Override
  public int getLengthOfEncodedKeyComponent(Float dimVals)
  {
    return 1;
  }

  @Override
  public int compareSortedEncodedKeyComponents(Float lhs, Float rhs)
  {
    return lhs.compareTo(rhs);
  }

  @Override
  public void validateSortedEncodedKeyComponents(
      Float lhs, Float rhs, Indexed<Float> lhsEncodings, Indexed<Float> rhsEncodings
  ) throws SegmentValidationException
  {
    if (!lhs.equals(rhs)) {
      throw new SegmentValidationException(
          "Dim [%s] value not equal. Expected [%s] found [%s]",
          dimensionName,
          lhs,
          rhs
      );
    }
  }

  @Override
  public Closeable getSubColumn(Column column)
  {
    return column.getGenericColumn();
  }

  @Override
  public Float getEncodedKeyComponentFromColumn(Closeable column, int currRow)
  {
    GenericColumn genericColumn = (GenericColumn) column;
    if (NullHandling.sqlCompatible() && genericColumn.isNull(currRow)) {
      return null;
    }
    return genericColumn.getFloatSingleValueRow(currRow);
  }
}
