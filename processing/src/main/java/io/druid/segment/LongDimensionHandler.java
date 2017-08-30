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

import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.LongColumn;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.Indexed;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public class LongDimensionHandler implements DimensionHandler<Long, Long, Long>
{
  private final String dimensionName;

  public LongDimensionHandler(String dimensionName)
  {
    this.dimensionName = dimensionName;
  }

  @Override
  public String getDimensionName()
  {
    return dimensionName;
  }

  @Override
  public DimensionIndexer<Long, Long, Long> makeIndexer()
  {
    return new LongDimensionIndexer();
  }

  @Override
  public DimensionMergerV9<Long> makeMerger(
      IndexSpec indexSpec, File outDir, IOPeon ioPeon, ColumnCapabilities capabilities, ProgressIndicator progress
  ) throws IOException
  {
    return new LongDimensionMergerV9(
        dimensionName,
        indexSpec,
        outDir,
        ioPeon,
        capabilities,
        progress
    );
  }

  @Override
  public int getLengthOfEncodedKeyComponent(Long dimVals)
  {
    return LongColumn.ROW_SIZE;
  }

  @Override
  public int compareSortedEncodedKeyComponents(Long lhs, Long rhs)
  {
    return lhs.compareTo(rhs);
  }

  @Override
  public void validateSortedEncodedKeyComponents(
      Long lhs, Long rhs, Indexed<Long> lhsEncodings, Indexed<Long> rhsEncodings
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
  public Long getEncodedKeyComponentFromColumn(Closeable column, int currRow)
  {
    return ((GenericColumn) column).getLongSingleValueRow(currRow);
  }
}
