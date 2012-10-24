package com.metamx.druid.query.group;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.druid.BaseQuery;
import com.metamx.druid.Query;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.dimension.DefaultDimensionSpec;
import com.metamx.druid.query.dimension.DimensionSpec;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.segment.LegacySegmentSpec;
import com.metamx.druid.query.segment.QuerySegmentSpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Map;

/**
 */
public class GroupByQuery extends BaseQuery<Row>
{
  public static Builder builder()
  {
    return new Builder();
  }

  private final DimFilter dimFilter;
  private final QueryGranularity granularity;
  private final List<DimensionSpec> dimensions;
  private final List<AggregatorFactory> aggregatorSpecs;
  private final List<PostAggregator> postAggregatorSpecs;

  @JsonCreator
  public GroupByQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("filter") DimFilter dimFilter,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("aggregations") List<AggregatorFactory> aggregatorSpecs,
      @JsonProperty("postAggregations") List<PostAggregator> postAggregatorSpecs,
      @JsonProperty("context") Map<String, String> context
  )
  {
    super(dataSource, querySegmentSpec, context);
    this.dimFilter = dimFilter;
    this.granularity = granularity;
    this.dimensions = dimensions == null ? ImmutableList.<DimensionSpec>of() : dimensions;
    this.aggregatorSpecs = aggregatorSpecs;
    this.postAggregatorSpecs = postAggregatorSpecs == null ? ImmutableList.<PostAggregator>of() : postAggregatorSpecs;

    Preconditions.checkNotNull(this.granularity, "Must specify a granularity");
    Preconditions.checkNotNull(this.aggregatorSpecs, "Must specify at least one aggregator");
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public QueryGranularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregatorSpecs()
  {
    return aggregatorSpecs;
  }

  @JsonProperty("postAggregations")
  public List<PostAggregator> getPostAggregatorSpecs()
  {
    return postAggregatorSpecs;
  }

  @Override
  public boolean hasFilters()
  {
    return dimFilter != null;
  }

  @Override
  public String getType()
  {
    return "groupBy";
  }

  @Override
  public Query withOverriddenContext(Map<String, String> contextOverride)
  {
    return new GroupByQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public Query withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new GroupByQuery(
        getDataSource(),
        spec,
        dimFilter,
        granularity,
        dimensions,
        aggregatorSpecs,
        postAggregatorSpecs,
        getContext()
    );
  }

  public static class Builder
  {
    private String dataSource;
    private QuerySegmentSpec querySegmentSpec;
    private DimFilter dimFilter;
    private QueryGranularity granularity;
    private List<DimensionSpec> dimensions;
    private List<AggregatorFactory> aggregatorSpecs;
    private List<PostAggregator> postAggregatorSpecs;
    private Map<String, String> context;

    private Builder() {}

    private Builder(Builder builder)
    {
      dataSource = builder.dataSource;
      querySegmentSpec = builder.querySegmentSpec;
      dimFilter = builder.dimFilter;
      granularity = builder.granularity;
      dimensions = builder.dimensions;
      aggregatorSpecs = builder.aggregatorSpecs;
      postAggregatorSpecs = builder.postAggregatorSpecs;
      context = builder.context;
    }

    public Builder setDataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder setInterval(Object interval)
    {
      return setQuerySegmentSpec(new LegacySegmentSpec(interval));
    }

    public Builder setQuerySegmentSpec(QuerySegmentSpec querySegmentSpec)
    {
      this.querySegmentSpec = querySegmentSpec;
      return this;
    }

    public Builder setDimFilter(DimFilter dimFilter)
    {
      this.dimFilter = dimFilter;
      return this;
    }

    public Builder setGranularity(QueryGranularity granularity)
    {
      this.granularity = granularity;
      return this;
    }

    public Builder addDimension(String column)
    {
      return addDimension(column, column);
    }

    public Builder addDimension(String column, String outputName)
    {
      return addDimension(new DefaultDimensionSpec(column, outputName));
    }

    public Builder addDimension(DimensionSpec dimension)
    {
      if (dimensions == null) {
        dimensions = Lists.newArrayList();
      }

      dimensions.add(dimension);
      return this;
    }

    public Builder setDimensions(List<DimensionSpec> dimensions)
    {
      this.dimensions = Lists.newArrayList(dimensions);
      return this;
    }

    public Builder addAggregator(AggregatorFactory aggregator)
    {
      if (aggregatorSpecs == null) {
        aggregatorSpecs = Lists.newArrayList();
      }

      aggregatorSpecs.add(aggregator);
      return this;
    }

    public Builder setAggregatorSpecs(List<AggregatorFactory> aggregatorSpecs)
    {
      this.aggregatorSpecs = Lists.newArrayList(aggregatorSpecs);
      return this;
    }

    public Builder addPostAggregator(PostAggregator postAgg)
    {
      if (postAggregatorSpecs == null) {
        postAggregatorSpecs = Lists.newArrayList();
      }

      postAggregatorSpecs.add(postAgg);
      return this;
    }

    public Builder setPostAggregatorSpecs(List<PostAggregator> postAggregatorSpecs)
    {
      this.postAggregatorSpecs = Lists.newArrayList(postAggregatorSpecs);
      return this;
    }

    public Builder setContext(Map<String, String> context)
    {
      this.context = context;
      return this;
    }

    public Builder copy()
    {
      return new Builder(this);
    }

    public GroupByQuery build()
    {
      return new GroupByQuery(
          dataSource,
          querySegmentSpec,
          dimFilter,
          granularity,
          dimensions,
          aggregatorSpecs,
          postAggregatorSpecs,
          context
      );
    }
  }
}
