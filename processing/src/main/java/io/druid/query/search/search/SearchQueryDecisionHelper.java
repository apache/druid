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

package io.druid.query.search.search;

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;

public abstract class SearchQueryDecisionHelper
{
  protected final double lowFilterSelectivityThreshold;
  protected final int lowCardinalityThreshold;

  protected SearchQueryDecisionHelper(final double lowFilterSelectivityThreshold, final int lowCardinalityThreshold)
  {
    this.lowFilterSelectivityThreshold = lowFilterSelectivityThreshold;
    this.lowCardinalityThreshold = lowCardinalityThreshold;
  }

  public double getLowFilterSelectivityThreshold()
  {
    return lowFilterSelectivityThreshold;
  }

  public int getLowCardinalityThreshold()
  {
    return lowCardinalityThreshold;
  }

  public boolean hasLowCardinality(final QueryableIndex index, final Iterable<DimensionSpec> dimensionSpecs)
  {
    long totalCard = 0;
    for (DimensionSpec dimension : dimensionSpecs) {
      final Column column = index.getColumn(dimension.getDimension());
      if (column != null) {
        final BitmapIndex bitmapIndex = column.getBitmapIndex();
        if (bitmapIndex != null) {
          totalCard += bitmapIndex.getCardinality();
        }
      }
    }

    return totalCard < lowCardinalityThreshold;
  }

  public boolean hasLowSelectivity(final QueryableIndex index, final ImmutableBitmap bitmap)
  {
    return index.getNumRows() * lowFilterSelectivityThreshold < bitmap.size();
  }
}
