/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.metadata.SegmentMetadataQuery;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.query.timeseries.TimeseriesQuery;

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
    @JsonSubTypes.Type(name = Query.SEGMENT_METADATA, value = SegmentMetadataQuery.class)
})
public interface Query<T>
{
  public static final String TIMESERIES = "timeseries";
  public static final String SEARCH = "search";
  public static final String TIME_BOUNDARY = "timeBoundary";
  public static final String GROUP_BY = "groupBy";
  public static final String SEGMENT_METADATA = "segmentMetadata";

  public String getDataSource();

  public boolean hasFilters();

  public String getType();

  public Sequence<T> run(QuerySegmentWalker walker);

  public Sequence<T> run(QueryRunner<T> runner);

  public List<Interval> getIntervals();

  public Duration getDuration();

  public String getContextValue(String key);

  public String getContextValue(String key, String defaultValue);

  public Query<T> withOverriddenContext(Map<String, String> contextOverride);

  public Query<T> withQuerySegmentSpec(QuerySegmentSpec spec);
}
