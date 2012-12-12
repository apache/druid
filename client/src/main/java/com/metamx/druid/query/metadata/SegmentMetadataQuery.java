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

import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SegmentMetadataResultValue;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public class SegmentMetadataQuery extends BaseQuery<Result<SegmentMetadataResultValue>>
{

  public SegmentMetadataQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("context") Map<String, String> context
  )
  {
    super(
        dataSource,
        querySegmentSpec,
        context
    );
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public String getType()
  {
    return "segmentMetadata";
  }

  @Override
  public Query<Result<SegmentMetadataResultValue>> withOverriddenContext(Map<String, String> contextOverride)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public Query<Result<SegmentMetadataResultValue>> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        spec,
        getContext()
    );
  }
}
