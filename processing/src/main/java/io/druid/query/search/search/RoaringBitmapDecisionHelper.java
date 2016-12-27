package io.druid.query.search.search;

public class RoaringBitmapDecisionHelper extends SearchQueryDecisionHelper
{
  private static final double LOW_FILTER_SELECTIVITY_THRESHOLD = 0.65;
  private static final int LOW_CARDINALITY_THRESHOLD = 1000;

  public RoaringBitmapDecisionHelper()
  {
    super(LOW_FILTER_SELECTIVITY_THRESHOLD, LOW_CARDINALITY_THRESHOLD);
  }
}
