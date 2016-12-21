package io.druid.query.search.search;

public class RoaringBitmapDecisionHelper extends SearchQueryDecisionHelper
{
  private static final double HIGH_FILTER_SELECTIVITY_THRESHOLD = 0.65;
  private static final int LOW_CARDINALITY_THRESHOLD = 1000;
  private static final RoaringBitmapDecisionHelper instance = new RoaringBitmapDecisionHelper();

  public static RoaringBitmapDecisionHelper getInstance() {
    return instance;
  }

  protected RoaringBitmapDecisionHelper()
  {
    super(HIGH_FILTER_SELECTIVITY_THRESHOLD, LOW_CARDINALITY_THRESHOLD);
  }
}
