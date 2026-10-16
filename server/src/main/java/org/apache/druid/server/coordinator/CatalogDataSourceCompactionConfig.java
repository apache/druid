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
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.DatasourceBaseTableMetadata;
import org.apache.druid.catalog.model.DatasourceProjectionMetadata;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.client.indexing.ClientCompactionRunnerInfo;
import org.apache.druid.common.config.Configs;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.BaseTableProjectionSpec;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.transform.CompactionTransformSpec;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.Interval;
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
  private final List<Interval> skipIntervals;
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
      @JsonProperty("skipIntervals") @Nullable List<Interval> skipIntervals,
      @JsonProperty("taskPriority") @Nullable Integer taskPriority,
      @JsonProperty("taskContext") @Nullable Map<String, Object> taskContext,
      @JsonProperty("inputSegmentSizeBytes") @Nullable Long inputSegmentSizeBytes,
      @JacksonInject MetadataCatalog metadataCatalog
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.engine = engine;
    this.skipOffsetFromLatest = skipOffsetFromLatest == null ? DEFAULT_SKIP_OFFSET_FROM_LATEST : skipOffsetFromLatest;
    this.skipIntervals = Configs.valueOrDefault(skipIntervals, List.of());
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
  public List<Interval> getSkipIntervals()
  {
    return skipIntervals;
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

  /**
   * Partitioning comes from the catalog: {@link DatasourceDefn#CLUSTER_KEYS_PROPERTY} becomes the range partition
   * dimensions and {@link DatasourceDefn#TARGET_SEGMENT_ROWS_PROPERTY} sizes the segments. A table that declares no
   * cluster keys falls back to dynamic partitioning.
   * <p>
   * A {@link #getBaseTable()} layout does not replace this: its clustering columns group rows <em>within</em> a
   * segment, while cluster keys range-partition <em>across</em> segments, so a table that declares both gets both.
   * <p>
   * This is not optional the way the other schema fields are: the MSQ engine rejects a compaction task with no
   * partitionsSpec, and a base table layout can only be compacted by MSQ.
   */
  @JsonIgnore
  @Nullable
  @Override
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    final ResolvedTable table = catalog.resolveTable(tableId);
    if (table == null) {
      return null;
    }
    final Integer targetSegmentRows = table.decodeProperty(DatasourceDefn.TARGET_SEGMENT_ROWS_PROPERTY);
    final List<ClusterKeySpec> clusterKeys = getClusterKeys(table);

    final PartitionsSpec partitionsSpec;
    if (CollectionUtils.isNullOrEmpty(clusterKeys)) {
      partitionsSpec = new DynamicPartitionsSpec(targetSegmentRows, null);
    } else {
      // Range partitioning is ascending only, DatasourceDefn.ClusterKeysDefn already rejects a descending cluster key,
      // so a catalog table cannot declare one.
      partitionsSpec = new DimensionRangePartitionsSpec(
          null,
          targetSegmentRows == null ? PartitionsSpec.DEFAULT_MAX_ROWS_PER_SEGMENT : targetSegmentRows,
          clusterKeys.stream().map(ClusterKeySpec::expr).collect(Collectors.toList()),
          false
      );
    }
    return UserCompactionTaskQueryTuningConfig.builder().partitionsSpec(partitionsSpec).build();
  }

  @Nullable
  private List<ClusterKeySpec> getClusterKeys(@Nullable ResolvedTable table)
  {
    return table == null ? null : table.decodeProperty(DatasourceDefn.CLUSTER_KEYS_PROPERTY);
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
    final String segmentGranularity = table.decodeProperty(DatasourceDefn.SEGMENT_GRANULARITY_PROPERTY);
    if (segmentGranularity == null) {
      return null;
    }
    return CatalogUtils.asDruidGranularity(segmentGranularity);
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

  /**
   * The projections declared by the catalog table's {@link DatasourceDefn#PROJECTIONS_KEYS_PROPERTY}.
   * <p>
   * A sealed table is the complete description of the datasource, so a table that declares no projections is taken to
   * have none: the property's absence becomes an empty list, which compaction treats as a declaration like any other
   * and so drops whatever projections the segments still carry. An unsealed table says nothing about the projections
   * it does not declare, so the absent property stays null and compaction leaves them alone.
   */
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
      return isSealed(table) ? List.of() : null;
    }
    return projections.stream().map(DatasourceProjectionMetadata::getSpec).collect(Collectors.toList());
  }

  /**
   * Derives the physical base table spec from the catalog table's {@link DatasourceDefn#BASE_TABLE_PROPERTY} layout,
   * combined with its declared columns via {@link DatasourceBaseTableMetadata#createSpec}, the same way
   * {@code MSQTaskQueryMaker} does for INSERT and REPLACE so that compaction lays segments out exactly as ingestion
   * does. A table that declares no layout has no spec, and compaction falls back to the legacy schema fields.
   */
  @JsonIgnore
  @Nullable
  @Override
  public BaseTableProjectionSpec getBaseTable()
  {
    final ResolvedTable table = catalog.resolveTable(tableId);
    if (table == null) {
      return null;
    }
    final DatasourceBaseTableMetadata baseTable = table.decodeProperty(DatasourceDefn.BASE_TABLE_PROPERTY);
    if (baseTable == null) {
      return null;
    }
    return baseTable.createSpec(table.spec().columns());
  }

  /**
   * A catalog table is sealed only when it says so: an absent property means the table accepts columns it does not
   * declare, which ingestion appends to the base table spec, so compaction must analyze the existing segments to find
   * and preserve them rather than treating the declaration as complete.
   */
  @JsonIgnore
  @Override
  public boolean isSealed()
  {
    return isSealed(catalog.resolveTable(tableId));
  }

  private static boolean isSealed(@Nullable ResolvedTable table)
  {
    if (table == null) {
      return true;
    }
    return Boolean.TRUE.equals(table.decodeProperty(DatasourceDefn.SEALED_PROPERTY));
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
  public CompactionConfigValidationResult validate(ClusterCompactionConfig clusterCompactionConfig)
  {
    final CompactionEngine effectiveEngine = engine == null ? clusterCompactionConfig.getEngine() : engine;
    if (effectiveEngine == CompactionEngine.MSQ) {
      // the validation below can only check partition column types when the config has a schema to read them from,
      // which a catalog table without a baseTable layout does not, so check the declared types here instead
      final CompactionConfigValidationResult clusterKeyTypes = validateClusterKeyTypes();
      if (!clusterKeyTypes.isValid()) {
        return clusterKeyTypes;
      }
    }
    return ClientCompactionRunnerInfo.validateCompactionConfig(this, clusterCompactionConfig.getEngine());
  }

  /**
   * Checks that every cluster key names a declared {@code VARCHAR} column, the only type the MSQ engine range
   * partitions by. A key naming a column the table does not declare is left alone: the table may still store it, and
   * its type is not knowable from the catalog.
   */
  private CompactionConfigValidationResult validateClusterKeyTypes()
  {
    final ResolvedTable table = catalog.resolveTable(tableId);
    final List<ClusterKeySpec> clusterKeys = getClusterKeys(table);
    if (CollectionUtils.isNullOrEmpty(clusterKeys)) {
      return CompactionConfigValidationResult.success();
    }
    final List<ColumnSpec> columns = table.spec().columns();
    if (CollectionUtils.isNullOrEmpty(columns)) {
      return CompactionConfigValidationResult.success();
    }
    for (ClusterKeySpec clusterKey : clusterKeys) {
      final int position = CatalogUtils.findColumn(columns, clusterKey.expr());
      if (position < 0) {
        continue;
      }
      final ColumnType type = Columns.druidType(columns.get(position));
      if (!ColumnType.STRING.equals(type)) {
        return CompactionConfigValidationResult.failure(
            "Cluster key[%s] of table[%s] is declared as[%s]; compaction partitions segments by range, which the MSQ"
            + " engine supports for string columns only. Remove it from 'clusterKeys'.",
            clusterKey.expr(),
            dataSource,
            type
        );
      }
    }
    return CompactionConfigValidationResult.success();
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
           && Objects.equals(skipIntervals, that.skipIntervals)
           && Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, engine, skipOffsetFromLatest, skipIntervals, taskPriority, taskContext, inputSegmentSizeBytes);
  }
}
