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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.server.coordinator.UserCompactionTaskDimensionsConfig;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies a data schema for reindexing tasks to configure.
 * <p>
 * This rule allows users to specify dimensionsspec metricsspec, query granularity, rollup, and projections for reindexing tasks to apply
 * <p>
 * This is a non-additive rule. Multiple data schema rules cannot be applied to the same interval being reindexed.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *   "id": "change-query-granularity-30d",
 *   "olderThan": "P30D",
 *   "queryGranularity": "HOUR",
 *   "rollup": true
 * }</pre>
 */
public class ReindexingDataSchemaRule extends AbstractReindexingRule
{
  private final UserCompactionTaskDimensionsConfig dimensionsSpec;
  private final AggregatorFactory[] metricsSpec;
  private final Granularity queryGranularity;
  private final Boolean rollup;
  private final List<AggregateProjectionSpec> projections;

  public ReindexingDataSchemaRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("dimensionsSpec") @Nullable UserCompactionTaskDimensionsConfig dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable AggregatorFactory[] metricsSpec,
      @JsonProperty("queryGranularity") @Nullable Granularity queryGranularity,
      @JsonProperty("rollup") @Nullable Boolean rollup,
      @JsonProperty("projections") @Nullable List<AggregateProjectionSpec> projections
  )
  {
    super(id, description, olderThan);
    this.dimensionsSpec = dimensionsSpec;
    this.metricsSpec = metricsSpec;
    this.queryGranularity = queryGranularity;
    this.rollup = rollup;
    this.projections = projections;
  }

  @JsonProperty
  public UserCompactionTaskDimensionsConfig getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  public List<AggregateProjectionSpec> getProjections()
  {
    return projections;
  }

  @JsonProperty
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonProperty
  public Granularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public Boolean getRollup()
  {
    return rollup;
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
    ReindexingDataSchemaRule that = (ReindexingDataSchemaRule) o;
    return Objects.equals(getId(), that.getId())
           && Objects.equals(getDescription(), that.getDescription())
           && Objects.equals(getOlderThan(), that.getOlderThan())
           && Objects.equals(dimensionsSpec, that.dimensionsSpec)
           && Objects.deepEquals(metricsSpec, that.metricsSpec)
           && Objects.equals(queryGranularity, that.queryGranularity)
           && Objects.equals(rollup, that.rollup)
           && Objects.equals(projections, that.projections);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(
        getId(),
        getDescription(),
        getOlderThan(),
        dimensionsSpec,
        queryGranularity,
        rollup,
        projections
    );
    result = 31 * result + Arrays.hashCode(metricsSpec);
    return result;
  }

  @Override
  public String toString()
  {
    return "ReindexingDataSchemaRule{"
           + "id='" + getId() + '\''
           + ", description='" + getDescription() + '\''
           + ", olderThan=" + getOlderThan()
           + ", dimensionsSpec=" + dimensionsSpec
           + ", metricsSpec=" + Arrays.toString(metricsSpec)
           + ", queryGranularity=" + queryGranularity
           + ", rollup=" + rollup
           + ", projections=" + projections
           + '}';
  }
}
