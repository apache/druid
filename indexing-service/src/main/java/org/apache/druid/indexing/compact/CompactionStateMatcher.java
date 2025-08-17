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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.coordinator.UserCompactionTaskGranularityConfig;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Instances of this class are used to determine if an interval needs to be
 * compacted based on the current compaction state of the underlying segments.
 * <p>
 * This class is mostly a duplicate of {@code CompactionState} but allows null
 * values for all fields so that only non-null fields are used for matching.
 */
public class CompactionStateMatcher
{
  private final PartitionsSpec partitionsSpec;
  private final DimensionsSpec dimensionsSpec;
  private final CompactionTransformSpec transformSpec;
  private final IndexSpec indexSpec;
  private final UserCompactionTaskGranularityConfig granularitySpec;
  private final AggregatorFactory[] metricsSpec;
  private final List<AggregateProjectionSpec> projections;

  @JsonCreator
  public CompactionStateMatcher(
      @JsonProperty("partitionsSpec") @Nullable PartitionsSpec partitionsSpec,
      @JsonProperty("dimensionsSpec") @Nullable DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable AggregatorFactory[] metricsSpec,
      @JsonProperty("transformSpec") @Nullable CompactionTransformSpec transformSpec,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("granularitySpec") @Nullable UserCompactionTaskGranularityConfig granularitySpec,
      @JsonProperty("projections") @Nullable List<AggregateProjectionSpec> projections
  )
  {
    this.partitionsSpec = partitionsSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.metricsSpec = metricsSpec;
    this.transformSpec = transformSpec;
    this.indexSpec = indexSpec;
    this.granularitySpec = granularitySpec;
    this.projections = projections;
  }

  @Nullable
  @JsonProperty
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Nullable
  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @Nullable
  @JsonProperty
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @Nullable
  @JsonProperty
  public CompactionTransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  @Nullable
  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @Nullable
  @JsonProperty
  public UserCompactionTaskGranularityConfig getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public List<AggregateProjectionSpec> getProjections()
  {
    return projections;
  }

  @Nullable
  public Granularity getSegmentGranularity()
  {
    return granularitySpec == null ? null : granularitySpec.getSegmentGranularity();
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
    CompactionStateMatcher that = (CompactionStateMatcher) o;
    return Objects.equals(partitionsSpec, that.partitionsSpec) &&
           Objects.equals(dimensionsSpec, that.dimensionsSpec) &&
           Objects.equals(transformSpec, that.transformSpec) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(granularitySpec, that.granularitySpec) &&
           Arrays.equals(metricsSpec, that.metricsSpec) &&
           Objects.equals(projections, that.projections);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        partitionsSpec,
        dimensionsSpec,
        transformSpec,
        indexSpec,
        granularitySpec,
        Arrays.hashCode(metricsSpec),
        projections
    );
  }

  @Override
  public String toString()
  {
    return "CompactionState{" +
           "partitionsSpec=" + partitionsSpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", transformSpec=" + transformSpec +
           ", indexSpec=" + indexSpec +
           ", granularitySpec=" + granularitySpec +
           ", metricsSpec=" + Arrays.toString(metricsSpec) +
           ", projections=" + projections +
           '}';
  }
}
