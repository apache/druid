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

import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.selector.settable.SettableColumnValueSelector;
import io.druid.segment.selector.settable.SettableFloatColumnValueSelector;
import io.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Comparator;

public class FloatDimensionHandler implements DimensionHandler<Float, Float, Float>
{
  private static Comparator<ColumnValueSelector> FLOAT_COLUMN_COMPARATOR = (s1, s2) -> {
    if (s1.isNull()) {
      return s2.isNull() ? 0 : -1;
    } else if (s2.isNull()) {
      return 1;
    } else {
      return Float.compare(s1.getFloat(), s2.getFloat());
    }
  };

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
  public DimensionMergerV9 makeMerger(
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
  public Comparator<ColumnValueSelector> getEncodedValueSelectorComparator()
  {
    return FLOAT_COLUMN_COMPARATOR;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableEncodedValueSelector()
  {
    return new SettableFloatColumnValueSelector();
  }
}
