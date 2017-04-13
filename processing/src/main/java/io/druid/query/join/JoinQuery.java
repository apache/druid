/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.join;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.data.input.Row;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.DataSourceWithSegmentSpec;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.VirtualColumns;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class JoinQuery extends BaseQuery<Row>
{
  private final JoinSpec joinSpec;
  private final Granularity granularity;
  private final List<DimensionSpec> dimensions; // output dimensions
  private final List<String> metrics;           // output metrics
  private final VirtualColumns virtualColumns;
  private final DimFilter filter;

  public static Builder newBuilder()
  {
    return new Builder();
  }

  @JsonCreator
  public JoinQuery(
      @JsonProperty("joinSpec") JoinSpec joinSpec,
      @JsonProperty("granularity") Granularity granularity,
      @JsonProperty("dimensions") List<DimensionSpec> dimensions,
      @JsonProperty("metrics") List<String> metrics,
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("filter") DimFilter filter,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(false, context);
    this.joinSpec = Objects.requireNonNull(joinSpec);
    this.granularity = Objects.requireNonNull(granularity);
    this.dimensions = dimensions == null ? ImmutableList.of() : dimensions;
    this.metrics = metrics == null ? ImmutableList.of() : metrics;
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.filter = filter;
    Preconditions.checkState(dimensions != null || metrics != null, "At least one dimension or metric must be specified.");
  }

  @Override
  public List<DataSourceWithSegmentSpec> getDataSources()
  {
    final List<DataSourceWithSegmentSpec> found = new ArrayList<>();
    final JoinSpecVisitor visitor = new JoinSpecVisitor()
    {
      @Override
      public DataInput visit(DataInput dataInput)
      {
        found.add(new DataSourceWithSegmentSpec(dataInput.getDataSource(), dataInput.getQuerySegmentSpec()));
        return dataInput;
      }
    };

    joinSpec.accept(visitor);

    return found;
  }

  @Override
  public boolean hasFilters()
  {
    return filter != null;
  }

  @Override
  public DimFilter getFilter()
  {
    return filter;
  }

  @Override
  public String getType()
  {
    return Query.JOIN;
  }

  @Override
  public Sequence<Row> run(
      QuerySegmentWalker walker, Map<String, Object> context
  )
  {
    return run(getDistributionTarget().getQuerySegmentSpec().lookup(this, walker), context);
  }

  @Override
  public Duration getDuration(DataSource dataSource)
  {
    for (DataSourceWithSegmentSpec sourceWithSegmentSpec : getDataSources()) {
      if (sourceWithSegmentSpec.getDataSource().equals(dataSource)) {
        return getTotalDuration(sourceWithSegmentSpec.getQuerySegmentSpec());
      }
    }
    return null;
  }

  @JsonProperty
  public JoinSpec getJoinSpec()
  {
    return joinSpec;
  }

  @JsonProperty
  public Granularity getGranularity()
  {
    return granularity;
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<String> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @Override
  public Query<Row> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new JoinQuery(
        joinSpec,
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        filter,
        computeOverridenContext(contextOverride)
    );
  }

  @Override
  public Query<Row> withQuerySegmentSpec(DataSource dataSource, QuerySegmentSpec spec)
  {
    return withQuerySegmentSpec(dataSource.getFirstName(), spec);
  }

  @Override
  public Query<Row> withQuerySegmentSpec(String dataSource, QuerySegmentSpec spec)
  {
    final JoinSpecVisitor visitor = new JoinSpecVisitor()
    {
      @Override
      public JoinSpec visit(JoinSpec joinSpec)
      {
        final JoinInputSpec newLeft = joinSpec.getLeft().accept(this);
        final JoinInputSpec newRight = joinSpec.getRight().accept(this);
        return new JoinSpec(joinSpec.getJoinType(), joinSpec.getPredicate(), newLeft, newRight);
      }

      @Override
      public DataInput visit(DataInput dataInput)
      {
        if (dataInput.getDataSource().getFirstName().equals(dataSource)) {
          return new DataInput(dataInput.getDataSource(), spec);
        } else {
          return dataInput;
        }
      }
    };

    return new JoinQuery(
        joinSpec.accept(visitor),
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        filter,
        getContext()
    );
  }

  @Override
  public Query<Row> replaceDataSourceWith(DataSource src, DataSource dst)
  {
    final JoinSpecVisitor visitor = new JoinSpecVisitor()
    {
      @Override
      public JoinSpec visit(JoinSpec joinSpec)
      {
        final JoinInputSpec newLeft = joinSpec.getLeft().accept(this);
        final JoinInputSpec newRight = joinSpec.getRight().accept(this);
        return new JoinSpec(joinSpec.getJoinType(), joinSpec.getPredicate(), newLeft, newRight);
      }

      @Override
      public DataInput visit(DataInput dataInput)
      {
        if (dataInput.getDataSource().equals(src)) {
          return new DataInput(dst, dataInput.getQuerySegmentSpec());
        } else {
          return dataInput;
        }
      }
    };

    return new JoinQuery(
        joinSpec.accept(visitor),
        granularity,
        dimensions,
        metrics,
        virtualColumns,
        filter,
        getContext()
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final JoinQuery that = (JoinQuery) o;
    if (!joinSpec.equals(that.joinSpec)) {
      return false;
    }

    if (!granularity.equals(that.granularity)) {
      return false;
    }

    if (!dimensions.equals(that.dimensions)) {
      return false;
    }

    if (!metrics.equals(that.metrics)) {
      return false;
    }

    if (!virtualColumns.equals(that.virtualColumns)) {
      return false;
    }

    if (!Objects.equals(filter, that.filter)) {
      return false;
    }

    return Objects.equals(getContext(), that.getContext());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getType(), joinSpec, granularity, dimensions, metrics, virtualColumns, filter, getContext());
  }

  public static class Builder
  {
    private JoinSpec joinSpec;
    private Granularity granularity;
    private List<DimensionSpec> dimensions;
    private List<String> metrics;
    private VirtualColumns virtualColumns;
    private DimFilter filter;
    private Map<String, Object> context;

    public Builder() {}

    public Builder(JoinQuery query)
    {
      this(
          query.joinSpec,
          query.granularity,
          query.dimensions,
          query.metrics,
          query.virtualColumns,
          query.filter,
          query.getContext()
      );
    }

    public Builder(Builder builder)
    {
      this(
          builder.joinSpec,
          builder.granularity,
          builder.dimensions,
          builder.metrics,
          builder.virtualColumns,
          builder.filter,
          builder.context
      );
    }

    public Builder(
        JoinSpec joinSpec,
        Granularity granularity,
        List<DimensionSpec> dimensions,
        List<String> metrics,
        VirtualColumns virtualColumns,
        DimFilter filter,
        Map<String, Object> context
    )
    {
      this.joinSpec = joinSpec;
      this.granularity = granularity;
      this.dimensions = dimensions;
      this.metrics = metrics;
      this.virtualColumns = virtualColumns;
      this.filter = filter;
      this.context = context;
    }

    public Builder setJoinSpec(JoinSpec joinSpec)
    {
      this.joinSpec = joinSpec;
      return this;
    }

    public Builder setGranularity(Granularity granularity)
    {
      this.granularity = granularity;
      return this;
    }

    public Builder setDimensions(List<DimensionSpec> dimensions)
    {
      this.dimensions = dimensions;
      return this;
    }

    public Builder setMetrics(List<String> metrics)
    {
      this.metrics = metrics;
      return this;
    }

    public Builder setVirtualColumns(VirtualColumns virtualColumns)
    {
      this.virtualColumns = virtualColumns;
      return this;
    }

    public Builder setFilter(DimFilter filter)
    {
      this.filter = filter;
      return this;
    }

    public Builder setContext(Map<String, Object> context)
    {
      if (this.context == null) {
        this.context = new HashMap<>(context);
      } else {
        this.context.putAll(context);
      }
      return this;
    }

    public JoinQuery build()
    {
      return new JoinQuery(
          joinSpec,
          granularity,
          dimensions,
          metrics,
          virtualColumns,
          filter,
          context
      );
    }
  }
}
