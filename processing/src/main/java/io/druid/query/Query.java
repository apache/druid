/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.common.guava.Sequence;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
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
    @JsonSubTypes.Type(name = Query.TOPN, value = TopNQuery.class),
    @JsonSubTypes.Type(name = Query.DATASOURCE_METADATA, value = DataSourceMetadataQuery.class)

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
  public static final String DATASOURCE_METADATA = "dataSourceMetadata";

  public DataSource getDataSource();

  public boolean hasFilters();

  public String getType();

  public Sequence<T> run(QuerySegmentWalker walker, Map<String, Object> context);

  public Sequence<T> run(QueryRunner<T> runner, Map<String, Object> context);

  public List<Interval> getIntervals();

  public Duration getDuration();

  public Map<String, Object> getContext();

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

  Query<T> withDataSource(DataSource dataSource);
}
