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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.transform.CompactionTransformSpec;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class describes what compaction task spec was used to create a given segment.
 * The compaction task is a task that reads Druid segments and overwrites them with new ones. Since this task always
 * reads segments in the same order, the same task spec will always create the same set of segments
 * (not same segment ID, but same content).
 * <p>
 * Note that this class doesn't include all fields in the compaction task spec. Only the configurations that can
 * affect the content of segment should be included.
 *
 * @see DataSegment#lastCompactionState
 */
public class CompactionState
{
  private final PartitionsSpec partitionsSpec;
  private final DimensionsSpec dimensionsSpec;
  private final CompactionTransformSpec transformSpec;
  private final IndexSpec indexSpec;
  private final GranularitySpec granularitySpec;
  private final List<AggregatorFactory> metricsSpec;
  @Nullable
  private final List<AggregateProjectionSpec> projections;

  @JsonCreator
  public CompactionState(
      @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      @JsonProperty("dimensionsSpec") DimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") List<AggregatorFactory> metricsSpec,
      @JsonProperty("transformSpec") CompactionTransformSpec transformSpec,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
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

  @JsonProperty
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @JsonProperty
  public DimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  public List<AggregatorFactory> getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonProperty
  public CompactionTransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompactionState that = (CompactionState) o;
    return Objects.equals(partitionsSpec, that.partitionsSpec) &&
           Objects.equals(dimensionsSpec, that.dimensionsSpec) &&
           Objects.equals(transformSpec, that.transformSpec) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(granularitySpec, that.granularitySpec) &&
           Objects.equals(metricsSpec, that.metricsSpec) &&
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
        metricsSpec,
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
           ", metricsSpec=" + metricsSpec +
           ", projections=" + projections +
           '}';
  }

  public static Function<Set<DataSegment>, Set<DataSegment>> addCompactionStateToSegments(
      PartitionsSpec partitionsSpec,
      DimensionsSpec dimensionsSpec,
      List<AggregatorFactory> metricsSpec,
      CompactionTransformSpec transformSpec,
      IndexSpec indexSpec,
      GranularitySpec granularitySpec,
      @Nullable List<AggregateProjectionSpec> projections
  )
  {
    IndexSpec effectiveIndexSpec = indexSpec.getEffectiveSpec();
    DimensionsSpec effectiveDimensions =
        DimensionsSpec.builder(dimensionsSpec)
                      .setDimensions(
                          dimensionsSpec.getDimensions()
                                        .stream()
                                        .map(dim -> dim.getEffectiveSchema(effectiveIndexSpec))
                                        .collect(Collectors.toList())
                      )
                      .build();

    CompactionState compactionState = new CompactionState(
        partitionsSpec,
        effectiveDimensions,
        metricsSpec,
        transformSpec,
        effectiveIndexSpec,
        granularitySpec,
        projections
    );

    return segments -> segments
        .stream()
        .map(s -> s.withLastCompactionState(compactionState))
        .collect(Collectors.toSet());
  }


  public Builder toBuilder()
  {
    return new Builder(this);
  }

  public static class Builder
  {
    private PartitionsSpec partitionsSpec;
    private DimensionsSpec dimensionsSpec;
    private CompactionTransformSpec transformSpec;
    private IndexSpec indexSpec;
    private GranularitySpec granularitySpec;
    private List<AggregatorFactory> metricsSpec;
    @Nullable
    private List<AggregateProjectionSpec> projections;

    Builder()
    {
    }

    private Builder(CompactionState compactionState)
    {
      this.partitionsSpec = compactionState.partitionsSpec;
      this.dimensionsSpec = compactionState.dimensionsSpec;
      this.transformSpec = compactionState.transformSpec;
      this.indexSpec = compactionState.indexSpec;
      this.granularitySpec = compactionState.granularitySpec;
      this.metricsSpec = compactionState.metricsSpec;
      this.projections = compactionState.projections;
    }

    public Builder partitionsSpec(PartitionsSpec partitionsSpec)
    {
      this.partitionsSpec = partitionsSpec;
      return this;
    }

    public Builder dimensionsSpec(DimensionsSpec dimensionsSpec)
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder transformSpec(CompactionTransformSpec transformSpec)
    {
      this.transformSpec = transformSpec;
      return this;
    }

    public Builder indexSpec(IndexSpec indexSpec)
    {
      this.indexSpec = indexSpec;
      return this;
    }

    public Builder granularitySpec(GranularitySpec granularitySpec)
    {
      this.granularitySpec = granularitySpec;
      return this;
    }

    public Builder metricsSpec(List<AggregatorFactory> metricsSpec)
    {
      this.metricsSpec = metricsSpec;
      return this;
    }

    public Builder projections(@Nullable List<AggregateProjectionSpec> projections)
    {
      this.projections = projections;
      return this;
    }

    public CompactionState build()
    {
      return new CompactionState(
          partitionsSpec,
          dimensionsSpec,
          metricsSpec,
          transformSpec,
          indexSpec,
          granularitySpec,
          projections
      );
    }
  }
}
