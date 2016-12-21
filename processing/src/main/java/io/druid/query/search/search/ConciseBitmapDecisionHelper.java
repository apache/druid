package io.druid.query.search.search;

public class ConciseBitmapDecisionHelper extends SearchQueryDecisionHelper
{
  private static final double HIGH_FILTER_SELECTIVITY_THRESHOLD = 0.99;
  private static final int LOW_CARDINALITY_THRESHOLD = 5000;
  private static final ConciseBitmapDecisionHelper instance = new ConciseBitmapDecisionHelper();

  protected ConciseBitmapDecisionHelper()
  {
    super(HIGH_FILTER_SELECTIVITY_THRESHOLD, LOW_CARDINALITY_THRESHOLD);
  }

  public static ConciseBitmapDecisionHelper getInstance() {
    return instance;
  }
}
