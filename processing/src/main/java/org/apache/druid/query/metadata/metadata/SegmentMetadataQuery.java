/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnionDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SegmentMetadataQuery extends BaseQuery<SegmentAnalysis>
{
  private static final QuerySegmentSpec DEFAULT_SEGMENT_SPEC = new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY);

  public enum AnalysisType implements Cacheable
  {
    CARDINALITY,
    SIZE,
    INTERVAL,
    AGGREGATORS,
    MINMAX,
    TIMESTAMPSPEC,
    QUERYGRANULARITY,
    ROLLUP;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static AnalysisType fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }

    @Override
    public byte[] getCacheKey()
    {
      return new byte[] {(byte) this.ordinal()};
    }
  }

  private final ColumnIncluderator toInclude;
  private final boolean merge;
  private final boolean usingDefaultInterval;
  private final EnumSet<AnalysisType> analysisTypes;
  private final AggregatorMergeStrategy aggregatorMergeStrategy;

  @JsonCreator
  public SegmentMetadataQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("toInclude") ColumnIncluderator toInclude,
      @JsonProperty("merge") Boolean merge,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("analysisTypes") EnumSet<AnalysisType> analysisTypes,
      @JsonProperty("usingDefaultInterval") Boolean useDefaultInterval,
      @Deprecated @JsonProperty("lenientAggregatorMerge") Boolean lenientAggregatorMerge,
      @JsonProperty("aggregatorMergeStrategy") AggregatorMergeStrategy aggregatorMergeStrategy
  )
  {
    super(dataSource, querySegmentSpec == null ? DEFAULT_SEGMENT_SPEC : querySegmentSpec, context);

    if (querySegmentSpec == null) {
      this.usingDefaultInterval = true;
    } else {
      this.usingDefaultInterval = useDefaultInterval == null ? false : useDefaultInterval;
    }
    this.toInclude = toInclude == null ? new AllColumnIncluderator() : toInclude;
    this.merge = merge == null ? false : merge;
    this.analysisTypes = analysisTypes;
    if (!(dataSource instanceof TableDataSource || dataSource instanceof UnionDataSource)) {
      throw InvalidInput.exception("Invalid dataSource type [%s]. "
                                   + "SegmentMetadataQuery only supports table or union datasources.", dataSource);
    }
    // We validate that there's only one parameter specified by the user. While the deprecated property is still
    // supported in the API, we only set the new member variable either using old or new property, so we've a single source
    // of truth for consumers of this class variable. The defaults are to preserve backwards compatibility.
    // In a future release, 28.0+, we can remove the deprecated property lenientAggregatorMerge.
    if (lenientAggregatorMerge != null && aggregatorMergeStrategy != null) {
      throw InvalidInput.exception("Both lenientAggregatorMerge [%s] and aggregatorMergeStrategy [%s] parameters cannot be set."
                                   + " Consider using aggregatorMergeStrategy since lenientAggregatorMerge is deprecated.",
                                   lenientAggregatorMerge, aggregatorMergeStrategy);
    }
    if (lenientAggregatorMerge != null) {
      this.aggregatorMergeStrategy = lenientAggregatorMerge
                                     ? AggregatorMergeStrategy.LENIENT
                                     : AggregatorMergeStrategy.STRICT;
    } else if (aggregatorMergeStrategy != null) {
      this.aggregatorMergeStrategy = aggregatorMergeStrategy;
    } else {
      this.aggregatorMergeStrategy = AggregatorMergeStrategy.STRICT;
    }
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

  @JsonProperty
  public boolean isUsingDefaultInterval()
  {
    return usingDefaultInterval;
  }

  @Override
  public boolean hasFilters()
  {
    return false;
  }

  @Override
  public DimFilter getFilter()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return Query.SEGMENT_METADATA;
  }

  @JsonProperty
  public EnumSet<AnalysisType> getAnalysisTypes()
  {
    return analysisTypes;
  }

  @JsonProperty
  public AggregatorMergeStrategy getAggregatorMergeStrategy()
  {
    return aggregatorMergeStrategy;
  }

  public boolean analyzingInterval()
  {
    return analysisTypes.contains(AnalysisType.INTERVAL);
  }

  public boolean hasAggregators()
  {
    return analysisTypes.contains(AnalysisType.AGGREGATORS);
  }

  public boolean hasTimestampSpec()
  {
    return analysisTypes.contains(AnalysisType.TIMESTAMPSPEC);
  }

  public boolean hasQueryGranularity()
  {
    return analysisTypes.contains(AnalysisType.QUERYGRANULARITY);
  }

  public boolean hasRollup()
  {
    return analysisTypes.contains(AnalysisType.ROLLUP);
  }

  @Override
  public Query<SegmentAnalysis> withOverriddenContext(Map<String, Object> contextOverride)
  {
    Map<String, Object> newContext = computeOverriddenContext(getContext(), contextOverride);
    return Druids.SegmentMetadataQueryBuilder.copy(this).context(newContext).build();
  }

  @Override
  public Query<SegmentAnalysis> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return Druids.SegmentMetadataQueryBuilder.copy(this).intervals(spec).build();
  }

  @Override
  public Query<SegmentAnalysis> withDataSource(DataSource dataSource)
  {
    return Druids.SegmentMetadataQueryBuilder.copy(this).dataSource(dataSource).build();
  }

  public Query<SegmentAnalysis> withColumns(ColumnIncluderator includerator)
  {
    return Druids.SegmentMetadataQueryBuilder.copy(this).toInclude(includerator).build();
  }

  public SegmentMetadataQuery withFinalizedAnalysisTypes(SegmentMetadataQueryConfig config)
  {
    if (analysisTypes != null) {
      return this;
    }
    return Druids.SegmentMetadataQueryBuilder
        .copy(this)
        .analysisTypes(config.getDefaultAnalysisTypes())
        .build();
  }

  @Override
  public List<Interval> getIntervals()
  {
    return this.getQuerySegmentSpec().getIntervals();
  }

  @Override
  public String toString()
  {
    return "SegmentMetadataQuery{" +
        "dataSource='" + getDataSource() + '\'' +
        ", querySegmentSpec=" + getQuerySegmentSpec() +
        ", toInclude=" + toInclude +
        ", merge=" + merge +
        ", usingDefaultInterval=" + usingDefaultInterval +
        ", analysisTypes=" + analysisTypes +
        ", aggregatorMergeStrategy=" + aggregatorMergeStrategy +
        '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    SegmentMetadataQuery that = (SegmentMetadataQuery) o;
    return merge == that.merge &&
        usingDefaultInterval == that.usingDefaultInterval &&
        Objects.equals(toInclude, that.toInclude) &&
        Objects.equals(analysisTypes, that.analysisTypes) &&
        Objects.equals(aggregatorMergeStrategy, that.aggregatorMergeStrategy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        toInclude,
        merge,
        usingDefaultInterval,
        analysisTypes,
        aggregatorMergeStrategy
    );
  }
}
