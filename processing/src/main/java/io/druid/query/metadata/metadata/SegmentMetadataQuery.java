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

package io.druid.query.metadata.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.common.utils.JodaUtils;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.Query;
import io.druid.query.TableDataSource;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Map;

public class SegmentMetadataQuery extends BaseQuery<SegmentAnalysis>
{
  public static final Interval DEFAULT_INTERVAL = new Interval(
      JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT
  );

  private final ColumnIncluderator toInclude;
  private final boolean merge;
  private final boolean usingDefaultInterval;

  @JsonCreator
  public SegmentMetadataQuery(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec,
      @JsonProperty("toInclude") ColumnIncluderator toInclude,
      @JsonProperty("merge") Boolean merge,
      @JsonProperty("context") Map<String, Object> context,
      @JsonProperty("usingDefaultInterval") Boolean useDefaultInterval
  )
  {
    super(
        dataSource,
        (querySegmentSpec == null) ? new MultipleIntervalSegmentSpec(Arrays.asList(DEFAULT_INTERVAL))
                                   : querySegmentSpec,
        context
    );

    if (querySegmentSpec == null) {
      this.usingDefaultInterval = true;
    } else {
      this.usingDefaultInterval = useDefaultInterval == null ? false : useDefaultInterval;
    }

    this.toInclude = toInclude == null ? new AllColumnIncluderator() : toInclude;
    this.merge = merge == null ? false : merge;
    Preconditions.checkArgument(
        dataSource instanceof TableDataSource,
        "SegmentMetadataQuery only supports table datasource"
    );
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
  public String getType()
  {
    return Query.SEGMENT_METADATA;
  }

  @Override
  public Query<SegmentAnalysis> withOverriddenContext(Map<String, Object> contextOverride)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        toInclude,
        merge,
        computeOverridenContext(contextOverride),
        usingDefaultInterval
    );
  }

  @Override
  public Query<SegmentAnalysis> withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return new SegmentMetadataQuery(
        getDataSource(),
        spec,
        toInclude,
        merge,
        getContext(),
        usingDefaultInterval
    );
  }

  @Override
  public Query<SegmentAnalysis> withDataSource(DataSource dataSource)
  {
    return new SegmentMetadataQuery(
        dataSource,
        getQuerySegmentSpec(),
        toInclude,
        merge,
        getContext(),
        usingDefaultInterval
    );
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

    if (merge != that.merge) {
      return false;
    }
    if (usingDefaultInterval != that.usingDefaultInterval) {
      return false;
    }
    return !(toInclude != null ? !toInclude.equals(that.toInclude) : that.toInclude != null);

  }

  @Override
  public int hashCode()
  {
    int result = super.hashCode();
    result = 31 * result + (toInclude != null ? toInclude.hashCode() : 0);
    result = 31 * result + (merge ? 1 : 0);
    result = 31 * result + (usingDefaultInterval ? 1 : 0);
    return result;
  }
}
