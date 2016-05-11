package io.druid.timeline.partition;

import com.google.common.collect.Range;

/**
 */
public class SingleDimensionShardData
{
  private final String dimension;
  private final String start;
  private final String finish;

  public SingleDimensionShardData(String dimension, String start, String finish) {
    this.dimension = dimension;
    this.start = start;
    this.finish = finish;
  }

  public String getDimension() {
    return dimension;
  }

  public String getStart() {
    return start;
  }

  public String getFinish() {
    return finish;
  }

  public Range<String> getRange() {
    return Range.closed(start, finish);
  }
}
