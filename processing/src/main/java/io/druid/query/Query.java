/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.common.guava.Sequence;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.select.SelectQuery;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.topn.TopNQuery;
import org.joda.time.Duration;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "queryType")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = Query.TIMESERIES, value = TimeseriesQuery.class),
    @JsonSubTypes.Type(name = Query.SEARCH, value = SearchQuery.class),
    @JsonSubTypes.Type(name = Query.TIME_BOUNDARY, value = TimeBoundaryQuery.class),
    @JsonSubTypes.Type(name = Query.GROUP_BY, value = GroupByQuery.class),
    @JsonSubTypes.Type(name = Query.SEGMENT_METADATA, value = SegmentMetadataQuery.class),
    @JsonSubTypes.Type(name = Query.SELECT, value = SelectQuery.class),
    @JsonSubTypes.Type(name = Query.TOPN, value = TopNQuery.class)
})
public interface Query<T>
{
  public static final String TIMESERIES = "timeseries";
  public static final String SEARCH = "search";
  public static final String TIME_BOUNDARY = "timeBoundary";
  public static final String GROUP_BY = "groupBy";
  public static final String SEGMENT_METADATA = "segmentMetadata";
  public static final String SELECT = "select";
  public static final String TOPN = "topN";

  public DataSource getDataSource();

  public boolean hasFilters();

  public String getType();

  public Sequence<T> run(QuerySegmentWalker walker);

  public Sequence<T> run(QueryRunner<T> runner);

  public List<Interval> getIntervals();

  public Duration getDuration();

  public <ContextType> ContextType getContextValue(String key);

  public <ContextType> ContextType getContextValue(String key, ContextType defaultValue);

  // For backwards compatibility
  @Deprecated public int getContextPriority(int defaultValue);
  @Deprecated public boolean getContextBySegment(boolean defaultValue);
  @Deprecated public boolean getContextPopulateCache(boolean defaultValue);
  @Deprecated public boolean getContextUseCache(boolean defaultValue);
  @Deprecated public boolean getContextFinalize(boolean defaultValue);

  public Query<T> withOverriddenContext(Map<String, Object> contextOverride);

  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);

  public Query<T> withId(String id);

  public String getId();
}
