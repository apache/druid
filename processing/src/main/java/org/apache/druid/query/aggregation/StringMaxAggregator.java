package org.apache.druid.query.aggregation;

import javax.annotation.Nullable;

public class StringMaxAggregator implements Aggregator
{
  static String combineValues(String lhs, String rhs) {
    return lhs.compareTo(rhs) > 0
        ? lhs
        : rhs;
  }

  private String max;
  // TODO: TO BE REPLACED WITH BaseStringColumnValueSelector.
  private String sthElse;

  @Override
  public void aggregate()
  {
    max = combineValues(max, sthElse);
  }

  @Nullable
  @Override
  public Object get()
  {
    return max;
  }

  @Override
  public float getFloat()
  {
    // TODO: THROW EXCEPTION MAYBE?
    return 0;
  }

  @Override
  public long getLong()
  {
    // TODO: THROW EXCEPTION MAYBE?
    return 0;
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
