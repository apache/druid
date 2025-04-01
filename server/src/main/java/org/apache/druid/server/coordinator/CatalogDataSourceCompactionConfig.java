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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class CatalogDataSourceCompactionConfig implements DataSourceCompactionConfig
{
  private final String dataSource;
  @Nullable
  private final CompactionEngine engine;
  private final Period skipOffsetFromLatest;
  private final int taskPriority;
  @Nullable
  private final Map<String, Object> taskContext;
  private final long inputSegmentSizeBytes;
  private final MetadataCatalog catalog;
  private final TableId tableId;

  @JsonCreator
  public CatalogDataSourceCompactionConfig(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("engine") @Nullable CompactionEngine engine,
      @JsonProperty("skipOffsetFromLatest") @Nullable Period skipOffsetFromLatest,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JacksonInject MetadataCatalog metadataCatalog
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.engine = engine;
    this.skipOffsetFromLatest = skipOffsetFromLatest == null ? DEFAULT_SKIP_OFFSET_FROM_LATEST : skipOffsetFromLatest;
    this.inputSegmentSizeBytes = inputSegmentSizeBytes == null
                                 ? DEFAULT_INPUT_SEGMENT_SIZE_BYTES
                                 : inputSegmentSizeBytes;
    this.taskPriority = taskPriority == null ? DEFAULT_COMPACTION_TASK_PRIORITY : taskPriority;
    this.taskContext = taskContext;
    this.catalog = metadataCatalog;
    this.tableId = TableId.datasource(dataSource);
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  @Nullable
  @Override
  public CompactionEngine getEngine()
  {
    return engine;
  }

  @JsonProperty
  @Override
  public Period getSkipOffsetFromLatest()
  {
    return skipOffsetFromLatest;
  }

  @JsonProperty
  @Override
  public int getTaskPriority()
  {
    return taskPriority;
  }

  @JsonProperty
  @Nullable
  @Override
  public Map<String, Object> getTaskContext()
  {
    return taskContext;
  }

  @JsonProperty
  @Override
  public long getInputSegmentSizeBytes()
  {
    return inputSegmentSizeBytes;
  }

  @JsonIgnore
  @Nullable
  @Override
  public Integer getMaxRowsPerSegment()
  {
    return null;
  }

  @JsonIgnore
  @Nullable
  @Override
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return null;
  }

  @JsonIgnore
  @Nullable
  @Override
  public UserCompactionTaskIOConfig getIoConfig()
  {
    return null;
  }

  @JsonIgnore
  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    final ResolvedTable table = catalog.resolveTable(tableId);
    if (table == null) {
      return null;
    }
    return CatalogUtils.asDruidGranularity(
        table.decodeProperty(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY)
    );
  }

  @JsonIgnore
  @Nullable
  @Override
  public UserCompactionTaskGranularityConfig getGranularitySpec()
  {
    return new UserCompactionTaskGranularityConfig(
        getSegmentGranularity(),
        null,
        null
    );
  }

  @JsonIgnore
  @Nullable
  @Override
  public List<AggregateProjectionSpec> getProjections()
  {
    final ResolvedTable table = catalog.resolveTable(tableId);
    if (table == null) {
      return null;
    }
    List<DatasourceProjectionMetadata> projections = table.decodeProperty(DatasourceDefn.PROJECTIONS_KEYS_PROPERTY);
    if (projections == null) {
      return null;
    }
    return projections.stream().map(DatasourceProjectionMetadata::getSpec).collect(Collectors.toList());
  }

  @JsonIgnore
  @Nullable
  @Override
  public CompactionTransformSpec getTransformSpec()
  {
    return null;
  }

  @JsonIgnore
  @Nullable
  @Override
  public UserCompactionTaskDimensionsConfig getDimensionsSpec()
  {
    return null;
  }

  @JsonIgnore
  @Nullable
  @Override
  public AggregatorFactory[] getMetricsSpec()
  {
    return null;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CatalogDataSourceCompactionConfig that = (CatalogDataSourceCompactionConfig) o;
    return taskPriority == that.taskPriority
           && inputSegmentSizeBytes == that.inputSegmentSizeBytes
           && Objects.equals(dataSource, that.dataSource)
           && engine == that.engine
           && Objects.equals(skipOffsetFromLatest, that.skipOffsetFromLatest)
           && Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, engine, skipOffsetFromLatest, taskPriority, taskContext, inputSegmentSizeBytes);
  }
}
