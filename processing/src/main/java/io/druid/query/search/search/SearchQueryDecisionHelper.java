package io.druid.query.search.search;

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;

public abstract class SearchQueryDecisionHelper
{
  protected final double highFilterSelectivityThreshold;
  protected final int lowCardinalityThreshold;

  protected SearchQueryDecisionHelper(final double highFilterSelectivityThreshold, final int lowCardinalityThreshold)
  {
    this.highFilterSelectivityThreshold = highFilterSelectivityThreshold;
    this.lowCardinalityThreshold = lowCardinalityThreshold;
  }

  public double getHighFilterSelectivityThreshold()
  {
    return highFilterSelectivityThreshold;
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

  public boolean hasHighSelectivity(final QueryableIndex index, final ImmutableBitmap bitmap)
  {
    return index.getNumRows() * highFilterSelectivityThreshold < bitmap.size();
  }
}
