package com.metamx.druid.query.search;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 */
public class SearchQuery extends BaseQuery<Result<SearchResultValue>>
{
  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<String> dimensions;
  private final SearchQuerySpec querySpec;
  private final int limit;

  @JsonCreator
  public SearchQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("limit") int limit,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("searchDimensions") List<String> dimensions,
      @JsonProperty("query") SearchQuerySpec querySpec,
      @JsonProperty("context") Map<String, String> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity == null ? QueryGranularity.ALL : granularity;
    this.limit = (limit == 0) ? 1000 : limit;
    this.dimensions = (dimensions == null) ? null : Lists.transform(
        dimensions,
        new Function<String, String>()
        {
          @Override
          public String apply(@Nullable String input)
          {
            return input.toLowerCase();
          }
        }
    );
    this.querySpec = querySpec;

    Preconditions.checkNotNull(querySegmentSpec, "Must specify an interval");
    Preconditions.checkNotNull(querySpec, "Must specify a query");
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return Query.SEARCH;
  }

  @Override
  public SearchQuery withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SearchQuery(
        getDataSource(),
        dimFilter,
        granularity,
        limit,
        spec,
        dimensions,
        querySpec,
        getContext()
    );
  }

  @Override
  public SearchQuery withOverriddenContext(Map<String, String> contextOverrides)
  {
    return new SearchQuery(
        getDataSource(),
        dimFilter,
        granularity,
        limit,
        getQuerySegmentSpec(),
        dimensions,
        querySpec,
        computeOverridenContext(contextOverrides)
    );
  }

  @JsonProperty("filter")
  public DimFilter getDimensionsFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public QueryGranularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public int getLimit()
  {
    return limit;
  }

  @JsonProperty("searchDimensions")
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("query")
  public SearchQuerySpec getQuery()
  {
    return querySpec;
  }

  public SearchQuery withLimit(int newLimit)
  {
    return new SearchQuery(
        getDataSource(),
        dimFilter,
        granularity,
        newLimit,
        getQuerySegmentSpec(),
        dimensions,
        querySpec,
        getContext()
    );
  }

  @Override
  public String toString()
  {
    return "SearchResultValue{" +
           "dataSource='" + getDataSource() + '\'' +
           ", dimFilter=" + dimFilter +
           ", granularity='" + granularity + '\'' +
           ", dimensions=" + dimensions +
           ", querySpec=" + querySpec +
           ", querySegmentSpec=" + getQuerySegmentSpec() +
           ", limit=" + limit +
           '}';
  }
}
