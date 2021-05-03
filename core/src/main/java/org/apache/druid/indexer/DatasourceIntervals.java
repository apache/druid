package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.joda.time.Interval;

import java.util.List;

/**
 * Contains a List of Intervals for a datasource.
 */
public class DatasourceIntervals
{
  private final String datasource;
  private final List<Interval> intervals;

  @JsonCreator
  public DatasourceIntervals(
      @JsonProperty("datasource") String datasource,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.datasource = datasource;
    this.intervals = intervals;
  }

  @JsonProperty("datasource")
  public String getDatasource()
  {
    return datasource;
  }

  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("datasource", datasource)
                  .add("intervals", intervals)
                  .toString();
  }
}
