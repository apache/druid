package com.metamx.druid;

import com.google.common.primitives.Longs;
import com.metamx.druid.result.Result;

import java.util.Comparator;

/**
 */
public class ResultGranularTimestampComparator<T> implements Comparator<Result<T>>
{
  private final QueryGranularity gran;

  public ResultGranularTimestampComparator(QueryGranularity granularity)
  {
    this.gran = granularity;
  }

  @Override
  public int compare(Result<T> r1, Result<T> r2)
  {
    return Longs.compare(
        gran.truncate(r1.getTimestamp().getMillis()),
        gran.truncate(r2.getTimestamp().getMillis())
    );
  }
}
