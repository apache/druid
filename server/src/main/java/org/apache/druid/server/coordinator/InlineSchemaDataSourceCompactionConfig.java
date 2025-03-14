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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InlineSchemaDataSourceCompactionConfig implements DataSourceCompactionConfig
{
  private final String dataSource;
  private final int taskPriority;
  private final long inputSegmentSizeBytes;

  public static Builder builder()
  {
    return new Builder();
  }

  /**
   * The number of input segments is limited because the byte size of a serialized task spec is limited by
   * org.apache.druid.indexing.overlord.config.RemoteTaskRunnerConfig.maxZnodeBytes.
   */
  @Nullable
  private final Integer maxRowsPerSegment;
  private final Period skipOffsetFromLatest;
  @Nullable
  private final UserCompactionTaskQueryTuningConfig tuningConfig;
  @Nullable
  private final UserCompactionTaskGranularityConfig granularitySpec;
  @Nullable
  private final UserCompactionTaskDimensionsConfig dimensionsSpec;
  @Nullable
  private final AggregatorFactory[] metricsSpec;
  @Nullable
  private final CompactionTransformSpec transformSpec;
  @Nullable
  private final List<AggregateProjectionSpec> projections;
  @Nullable
  private final UserCompactionTaskIOConfig ioConfig;
  @Nullable
  private final Map<String, Object> taskContext;
  @Nullable
  private final CompactionEngine engine;

  @JsonCreator
  public InlineSchemaDataSourceCompactionConfig(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JsonProperty("maxRowsPerSegment") @Deprecated @Nullable Integer maxRowsPerSegment,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("tuningConfig") @Nullable UserCompactionTaskQueryTuningConfig tuningConfig,
      @JsonProperty("granularitySpec") @Nullable UserCompactionTaskGranularityConfig granularitySpec,
      @JsonProperty("dimensionsSpec") @Nullable UserCompactionTaskDimensionsConfig dimensionsSpec,
      @JsonProperty("metricsSpec") @Nullable AggregatorFactory[] metricsSpec,
      @JsonProperty("transformSpec") @Nullable CompactionTransformSpec transformSpec,
      @JsonProperty("projections") @Nullable List<AggregateProjectionSpec> projections,
      @JsonProperty("ioConfig") @Nullable UserCompactionTaskIOConfig ioConfig,
      @JsonProperty("engine") @Nullable CompactionEngine engine,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.taskPriority = taskPriority == null
                        ? DEFAULT_COMPACTION_TASK_PRIORITY
                        : taskPriority;
    this.inputSegmentSizeBytes = inputSegmentSizeBytes == null
                                 ? DEFAULT_INPUT_SEGMENT_SIZE_BYTES
                                 : inputSegmentSizeBytes;
    this.maxRowsPerSegment = maxRowsPerSegment;
    this.skipOffsetFromLatest = skipOffsetFromLatest == null ? DEFAULT_SKIP_OFFSET_FROM_LATEST : skipOffsetFromLatest;
    this.tuningConfig = tuningConfig;
    this.ioConfig = ioConfig;
    this.granularitySpec = granularitySpec;
    this.metricsSpec = metricsSpec;
    this.dimensionsSpec = dimensionsSpec;
    this.transformSpec = transformSpec;
    this.projections = projections;
    this.taskContext = taskContext;
    this.engine = engine;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Override
  public int getTaskPriority()
  {
    return taskPriority;
  }

  @JsonProperty
  @Override
  public long getInputSegmentSizeBytes()
  {
    return inputSegmentSizeBytes;
  }

  @Deprecated
  @JsonProperty
  @Nullable
  @Override
  public Integer getMaxRowsPerSegment()
  {
    return maxRowsPerSegment;
  }

  @JsonProperty
  @Override
  public Period getSkipOffsetFromLatest()
  {
    return skipOffsetFromLatest;
  }

  @JsonProperty
  @Nullable
  @Override
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  @Nullable
  @Override
  public UserCompactionTaskIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  @Nullable
  @Override
  public UserCompactionTaskGranularityConfig getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  @Nullable
  @Override
  public UserCompactionTaskDimensionsConfig getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  @Nullable
  @Override
  public CompactionTransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  @JsonProperty
  @Nullable
  @Override
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonProperty
  @Nullable
  @Override
  public List<AggregateProjectionSpec> getProjections()
  {
    return projections;
  }

  @JsonProperty
  @Nullable
  @Override
  public Map<String, Object> getTaskContext()
  {
    return taskContext;
  }

  @JsonProperty
  @Nullable
  @Override
  public CompactionEngine getEngine()
  {
    return engine;
  }

  @Nullable
  @JsonIgnore
  @Override
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
    InlineSchemaDataSourceCompactionConfig that = (InlineSchemaDataSourceCompactionConfig) o;
    return taskPriority == that.taskPriority &&
           inputSegmentSizeBytes == that.inputSegmentSizeBytes &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(maxRowsPerSegment, that.maxRowsPerSegment) &&
           Objects.equals(skipOffsetFromLatest, that.skipOffsetFromLatest) &&
           Objects.equals(tuningConfig, that.tuningConfig) &&
           Objects.equals(granularitySpec, that.granularitySpec) &&
           Objects.equals(dimensionsSpec, that.dimensionsSpec) &&
           Arrays.equals(metricsSpec, that.metricsSpec) &&
           Objects.equals(transformSpec, that.transformSpec) &&
           Objects.equals(projections, that.projections) &&
           Objects.equals(ioConfig, that.ioConfig) &&
           this.engine == that.engine &&
           Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(
        dataSource,
        taskPriority,
        inputSegmentSizeBytes,
        maxRowsPerSegment,
        skipOffsetFromLatest,
        tuningConfig,
        granularitySpec,
        dimensionsSpec,
        transformSpec,
        projections,
        ioConfig,
        taskContext,
        engine
    );
    result = 31 * result + Arrays.hashCode(metricsSpec);
    return result;
  }

  public static class Builder
  {
    private String dataSource;
    private Integer taskPriority;
    private Long inputSegmentSizeBytes;
    private Integer maxRowsPerSegment;
    private Period skipOffsetFromLatest;
    private UserCompactionTaskQueryTuningConfig tuningConfig;
    private UserCompactionTaskGranularityConfig granularitySpec;
    private UserCompactionTaskDimensionsConfig dimensionsSpec;
    private AggregatorFactory[] metricsSpec;
    private CompactionTransformSpec transformSpec;
    private List<AggregateProjectionSpec> projections;
    private UserCompactionTaskIOConfig ioConfig;
    private CompactionEngine engine;
    private Map<String, Object> taskContext;

    public InlineSchemaDataSourceCompactionConfig build()
    {
      return new InlineSchemaDataSourceCompactionConfig(
          dataSource,
          taskPriority,
          inputSegmentSizeBytes,
          maxRowsPerSegment,
          skipOffsetFromLatest,
          tuningConfig,
          granularitySpec,
          dimensionsSpec,
          metricsSpec,
          transformSpec,
          projections,
          ioConfig,
          engine,
          taskContext
      );
    }

    public Builder forDataSource(String dataSource)
    {
      this.dataSource = dataSource;
      return this;
    }

    public Builder withTaskPriority(Integer taskPriority)
    {
      this.taskPriority = taskPriority;
      return this;
    }

    public Builder withInputSegmentSizeBytes(Long inputSegmentSizeBytes)
    {
      this.inputSegmentSizeBytes = inputSegmentSizeBytes;
      return this;
    }

    @Deprecated
    public Builder withMaxRowsPerSegment(Integer maxRowsPerSegment)
    {
      this.maxRowsPerSegment = maxRowsPerSegment;
      return this;
    }

    public Builder withSkipOffsetFromLatest(Period skipOffsetFromLatest)
    {
      this.skipOffsetFromLatest = skipOffsetFromLatest;
      return this;
    }

    public Builder withTuningConfig(
        UserCompactionTaskQueryTuningConfig tuningConfig
    )
    {
      this.tuningConfig = tuningConfig;
      return this;
    }

    public Builder withGranularitySpec(
        UserCompactionTaskGranularityConfig granularitySpec
    )
    {
      this.granularitySpec = granularitySpec;
      return this;
    }

    public Builder withDimensionsSpec(
        UserCompactionTaskDimensionsConfig dimensionsSpec
    )
    {
      this.dimensionsSpec = dimensionsSpec;
      return this;
    }

    public Builder withMetricsSpec(AggregatorFactory[] metricsSpec)
    {
      this.metricsSpec = metricsSpec;
      return this;
    }

    public Builder withTransformSpec(
        CompactionTransformSpec transformSpec
    )
    {
      this.transformSpec = transformSpec;
      return this;
    }

    public Builder withProjections(List<AggregateProjectionSpec> projections)
    {
      this.projections = projections;
      return this;
    }

    public Builder withIoConfig(UserCompactionTaskIOConfig ioConfig)
    {
      this.ioConfig = ioConfig;
      return this;
    }

    public Builder withEngine(CompactionEngine engine)
    {
      this.engine = engine;
      return this;
    }

    public Builder withTaskContext(Map<String, Object> taskContext)
    {
      this.taskContext = taskContext;
      return this;
    }
  }
}
