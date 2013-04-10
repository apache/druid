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

package com.metamx.druid.query.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.query.segment.QuerySegmentSpec;

import java.util.Map;

public class SegmentMetadataQuery extends BaseQuery<SegmentAnalysis>
{

  private final ColumnIncluderator toInclude;
  private final boolean merge;

  @JsonCreator
  public SegmentMetadataQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("toInclude") ColumnIncluderator toInclude,
      @JsonProperty("merge") Boolean merge,
      @JsonProperty("context") Map<String, String> context
  )
  {
    super(dataSource, querySegmentSpec, context);

    this.toInclude = toInclude == null ? new AllColumnIncluderator() : toInclude;
    this.merge = merge == null ? false : merge;
  }

  @JsonProperty
  public ColumnIncluderator getToInclude()
  {
    return toInclude;
  }

  @JsonProperty
  public boolean isMerge()
  {
    return merge;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public String getType()
  {
    return Query.SEGMENT_METADATA;
  }

  @Override
  public Query<SegmentAnalysis> withOverriddenContext(Map<String, String> contextOverride)
  {
    return new SegmentMetadataQuery(
        getDataSource(), getQuerySegmentSpec(), toInclude, merge, computeOverridenContext(contextOverride)
    );
  }

  @Override
  public Query<SegmentAnalysis> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SegmentMetadataQuery(getDataSource(), spec, toInclude, merge, getContext());
  }
}
