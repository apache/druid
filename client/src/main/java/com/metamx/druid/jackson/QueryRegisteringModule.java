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

package com.metamx.druid.jackson;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.metamx.druid.query.dimension.DefaultDimensionSpec;
import com.metamx.druid.query.dimension.ExtractionDimensionSpec;
import com.metamx.druid.query.dimension.LegacyDimensionSpec;
import com.metamx.druid.query.extraction.PartialDimExtractionFn;
import com.metamx.druid.query.extraction.RegexDimExtractionFn;
import com.metamx.druid.query.extraction.SearchQuerySpecDimExtractionFn;
import com.metamx.druid.query.extraction.TimeDimExtractionFn;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.metadata.SegmentMetadataQuery;
import com.metamx.druid.query.search.SearchQuery;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.Query;
import io.druid.query.spec.DimExtractionFn;
import io.druid.query.spec.DimensionSpec;

/**
 */
public class QueryRegisteringModule extends SimpleModule
{
  public QueryRegisteringModule()
  {
    super("QueryRegistering");

    setMixInAnnotation(Query.class, QueriesMixin.class);
    setMixInAnnotation(DimensionSpec.class, DimensionSpecMixin.class);
    setMixInAnnotation(DimExtractionFn.class, DimensionSpecMixin.class);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "queryType")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "timeseries", value = TimeseriesQuery.class),
      @JsonSubTypes.Type(name = "search", value = SearchQuery.class),
      @JsonSubTypes.Type(name = "timeBoundary", value = TimeBoundaryQuery.class),
      @JsonSubTypes.Type(name = "groupBy", value = GroupByQuery.class),
      @JsonSubTypes.Type(name = "segmentMetadata", value = SegmentMetadataQuery.class)
  })
  public static interface QueriesMixin
  {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = LegacyDimensionSpec.class)
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "default", value = DefaultDimensionSpec.class),
      @JsonSubTypes.Type(name = "extraction", value = ExtractionDimensionSpec.class)
  })
  public static interface DimensionSpecMixin
  {
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property="type")
  @JsonSubTypes(value = {
      @JsonSubTypes.Type(name = "time", value = TimeDimExtractionFn.class),
      @JsonSubTypes.Type(name = "regex", value = RegexDimExtractionFn.class),
      @JsonSubTypes.Type(name = "partial", value = PartialDimExtractionFn.class),
      @JsonSubTypes.Type(name = "searchQuery", value = SearchQuerySpecDimExtractionFn.class)
  })
  public static interface DimExtractionFnMixin
  {
  }
}
