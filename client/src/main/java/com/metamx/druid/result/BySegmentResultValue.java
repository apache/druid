package com.metamx.druid.result;

import java.util.List;

/**
 */
public interface BySegmentResultValue<T>
{
  public List<Result<T>> getResults();

  public String getSegmentId();

  public String getIntervalString();
}
