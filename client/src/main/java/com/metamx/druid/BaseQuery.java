package com.metamx.druid;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public abstract class BaseQuery<T> implements Query<T>
{
  private final String dataSource;
  private final Map<String, String> context;
  private final QuerySegmentSpec querySegmentSpec;

  private volatile Duration duration;

  public BaseQuery(
      String dataSource,
      QuerySegmentSpec querySegmentSpec,
      Map<String, String> context
  )
  {
    Preconditions.checkNotNull(dataSource, "dataSource can't be null");
    Preconditions.checkNotNull(querySegmentSpec, "querySegmentSpec can't be null");

    this.dataSource = dataSource.toLowerCase();
    this.context = context;
    this.querySegmentSpec = querySegmentSpec;

  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  @Override
  public Sequence<T> run(QuerySegmentWalker walker)
  {
    return querySegmentSpec.lookup(this, walker).run(this);
  }

  @Override
  public List<Interval> getIntervals()
  {
    return querySegmentSpec.getIntervals();
  }

  @Override
  public Duration getDuration()
  {
    if (duration == null) {
      Duration totalDuration = new Duration(0);
      for (Interval interval : querySegmentSpec.getIntervals()) {
        if (interval != null) {
          totalDuration = totalDuration.plus(interval.toDuration());
        }
      }
      duration = totalDuration;
    }

    return duration;
  }

  @JsonProperty
  public Map<String, String> getContext()
  {
    return context;
  }

  @Override
  public String getContextValue(String key)
  {
    return context == null ? null : context.get(key);
  }

  @Override
  public String getContextValue(String key, String defaultValue)
  {
    String retVal = getContextValue(key);
    return retVal == null ? defaultValue : retVal;
  }

  protected Map<String, String> computeOverridenContext(Map<String, String> overrides)
  {
    Map<String, String> overridden = Maps.newTreeMap();
    final Map<String, String> context = getContext();
    if (context != null) {
      overridden.putAll(context);
    }
    overridden.putAll(overrides);

    return overridden;
  }
}
