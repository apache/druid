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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.granularity.GranularitySpec;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.server.compaction.CompactionStatus;
import org.apache.druid.timeline.CompactionState;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = InlineSchemaDataSourceCompactionConfig.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "inline", value = InlineSchemaDataSourceCompactionConfig.class),
    @JsonSubTypes.Type(name = "catalog", value = CatalogDataSourceCompactionConfig.class)
})
public interface DataSourceCompactionConfig
{
  /**
   * Must be synced with Tasks.DEFAULT_MERGE_TASK_PRIORITY
   */
  int DEFAULT_COMPACTION_TASK_PRIORITY = 25;

  // Approx. 100TB. Chosen instead of Long.MAX_VALUE to avoid overflow on web-console and other clients
  long DEFAULT_INPUT_SEGMENT_SIZE_BYTES = 100_000_000_000_000L;
  Period DEFAULT_SKIP_OFFSET_FROM_LATEST = new Period("P1D");

  String getDataSource();

  @Nullable
  CompactionEngine getEngine();

  int getTaskPriority();

  long getInputSegmentSizeBytes();

  @Deprecated
  @Nullable
  Integer getMaxRowsPerSegment();

  Period getSkipOffsetFromLatest();

  @Nullable
  UserCompactionTaskQueryTuningConfig getTuningConfig();

  @Nullable
  UserCompactionTaskIOConfig getIoConfig();

  @Nullable
  Map<String, Object> getTaskContext();

  @Nullable
  Granularity getSegmentGranularity();

  @Nullable
  UserCompactionTaskGranularityConfig getGranularitySpec();

  @Nullable
  List<AggregateProjectionSpec> getProjections();

  @Nullable
  CompactionTransformSpec getTransformSpec();

  @Nullable
  UserCompactionTaskDimensionsConfig getDimensionsSpec();

  @Nullable
  AggregatorFactory[] getMetricsSpec();

  /**
   * Converts this compaction config to a {@link CompactionState}.
   */
  default CompactionState toCompactionState()
  {
    ClientCompactionTaskQueryTuningConfig tuningConfig = ClientCompactionTaskQueryTuningConfig.from(this);

    // 1. PartitionsSpec - reuse existing method
    PartitionsSpec partitionsSpec = CompactionStatus.findPartitionsSpecFromConfig(tuningConfig);

    // 2. DimensionsSpec
    DimensionsSpec dimensionsSpec = null;
    if (getDimensionsSpec() != null && getDimensionsSpec().getDimensions() != null) {
      dimensionsSpec = new DimensionsSpec(getDimensionsSpec().getDimensions());
    }

    // 3. Metrics
    List<AggregatorFactory> metricsSpec = getMetricsSpec() == null
                                          ? null
                                          : Arrays.asList(getMetricsSpec());

    // 4. Transform
    CompactionTransformSpec transformSpec = getTransformSpec();

    // 5. IndexSpec
    IndexSpec indexSpec = tuningConfig.getIndexSpec() == null
                          ? IndexSpec.getDefault()
                          : tuningConfig.getIndexSpec();

    // 6. GranularitySpec
    GranularitySpec granularitySpec = null;
    if (getGranularitySpec() != null) {
      UserCompactionTaskGranularityConfig userGranularityConfig = getGranularitySpec();
      granularitySpec = new UniformGranularitySpec(
          userGranularityConfig.getSegmentGranularity(),
          userGranularityConfig.getQueryGranularity(),
          userGranularityConfig.isRollup(),
          null  // intervals
      );
    }

    // 7. Projections
    List<AggregateProjectionSpec> projections = getProjections();

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
