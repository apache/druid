package com.metamx.druid;

import com.metamx.common.guava.Sequence;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "queryType")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = Query.SEARCH, value = SearchQuery.class),
    @JsonSubTypes.Type(name = Query.TIME_BOUNDARY, value = TimeBoundaryQuery.class),
    @JsonSubTypes.Type(name = "groupBy", value= GroupByQuery.class)
})
public interface Query<T>
{
  public static final String SEARCH = "search";
  public static final String TIME_BOUNDARY = "timeBoundary";

  public String getDataSource();

  public boolean hasFilters();

  public String getType();

  public Sequence<T> run(QuerySegmentWalker walker);

  public List<Interval> getIntervals();

  public Duration getDuration();

  public String getContextValue(String key);

  public String getContextValue(String key, String defaultValue);

  public Query<T> withOverriddenContext(Map<String, String> contextOverride);

  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);
}
