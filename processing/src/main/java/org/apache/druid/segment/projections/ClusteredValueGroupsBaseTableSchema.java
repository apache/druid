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

package org.apache.druid.segment.projections;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.AggregateProjectionMetadata;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Top-level summary for a clustered base table whose groups are identified by discrete clustering-value tuples. Each
 * tuple group is internally stored as a separate table without storing the cluster columns, which are pulled into this
 * metadata. This is optimizing for use cases which typically only need to read from a single group via filters present
 * on a query. Cluster groups nest inside as {@link #getClusterGroups()}; their column data live in the V10 segment
 * file under dictionary-id-tuple prefixes ({@code __base$<id0>_<id1>...<idK>/<col>}), where the ids index into
 * {@link #getClusteringDictionaries()}.
 */
public class ClusteredValueGroupsBaseTableSchema implements BaseTableProjectionSchema
{
  public static final String TYPE_NAME = "clustered-value-groups-base-table";

  private final VirtualColumns virtualColumns;
  private final List<String> columnNames;
  private final List<OrderBy> ordering;
  private final RowSignature clusteringColumns;
  private final List<String> sharedColumns;
  private final ClusteringDictionaries clusteringDictionaries;
  private final List<TableClusterGroupSpec> clusterGroups;

  // computed
  private final int timeColumnPosition;
  private final Granularity effectiveGranularity;

  @JsonCreator
  public ClusteredValueGroupsBaseTableSchema(
      @JsonProperty("virtualColumns") VirtualColumns virtualColumns,
      @JsonProperty("columns") List<String> columns,
      @JsonProperty("ordering") List<OrderBy> ordering,
      @JsonProperty("clusteringColumns") RowSignature clusteringColumns,
      @JsonProperty("sharedColumns") @Nullable List<String> sharedColumns,
      @JsonProperty("clusteringDictionaries") @Nullable ClusteringDictionaries clusteringDictionaries,
      @JsonProperty("groups") @Nullable List<TableClusterGroupSpec> clusterGroups
  )
  {
    if (CollectionUtils.isNullOrEmpty(columns)) {
      throw DruidException.defensive("clustered base table schema columns must not be null or empty");
    }
    if (ordering == null) {
      throw DruidException.defensive("clustered base table schema ordering must not be null");
    }
    if (clusteringColumns == null || clusteringColumns.size() == 0) {
      throw DruidException.defensive(
          "clustered base table schema clusteringColumns must not be null or empty"
      );
    }
    if (ordering.size() < clusteringColumns.size()) {
      throw DruidException.defensive(
          "ordering size [%s] must be at least clusteringColumns size [%s] (clustering columns must form a prefix"
          + " of the segment ordering)",
          ordering.size(),
          clusteringColumns.size()
      );
    }
    for (int i = 0; i < clusteringColumns.size(); i++) {
      final String clusteringColumn = clusteringColumns.getColumnName(i);
      if (!columns.contains(clusteringColumn)) {
        throw DruidException.defensive(
            "clusteringColumn [%s] must appear in columns of the clustered base table summary",
            clusteringColumn
        );
      }
      final ColumnType type = clusteringColumns.getColumnType(i).orElse(null);
      if (!Projections.isAllowedClusteringType(type)) {
        throw DruidException.defensive(
            "clustering column [%s] has unsupported type [%s]; allowed types are STRING, LONG, DOUBLE, FLOAT",
            clusteringColumn,
            type
        );
      }
      // Per-group ordering is derived by dropping this prefix; pruning + cursor concatenation rely on it.
      final String orderingColumn = ordering.get(i).getColumnName();
      if (!clusteringColumn.equals(orderingColumn)) {
        throw DruidException.defensive(
            "clustering column at position [%s] is [%s] but the segment ordering at the same position is [%s];"
            + " clustering columns must form a prefix of the segment ordering",
            i,
            clusteringColumn,
            orderingColumn
        );
      }
    }
    final List<String> resolvedSharedColumns = sharedColumns == null ? List.of() : sharedColumns;
    for (String shared : resolvedSharedColumns) {
      if (!columns.contains(shared)) {
        throw DruidException.defensive(
            "sharedColumn [%s] must appear in columns of the clustered base table summary",
            shared
        );
      }
    }
    this.virtualColumns = virtualColumns == null ? VirtualColumns.EMPTY : virtualColumns;
    this.columnNames = columns;
    this.ordering = ordering;
    this.clusteringColumns = clusteringColumns;
    this.sharedColumns = resolvedSharedColumns;
    this.clusterGroups = clusterGroups == null ? List.of() : List.copyOf(clusterGroups);
    this.clusteringDictionaries = clusteringDictionaries == null
                                  ? ClusteringDictionaries.EMPTY
                                  : clusteringDictionaries;

    int foundTimePosition = -1;
    Granularity granularity = null;
    for (int i = 0; i < ordering.size(); i++) {
      OrderBy orderBy = ordering.get(i);
      if (orderBy.getColumnName().equals(ColumnHolder.TIME_COLUMN_NAME)) {
        foundTimePosition = i;
        final VirtualColumn vc = this.virtualColumns.getVirtualColumn(Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME);
        if (vc != null) {
          granularity = Granularities.fromVirtualColumn(vc);
        } else {
          granularity = Granularities.NONE;
        }
      }
    }
    if (granularity == null) {
      throw DruidException.defensive(
          "clustered base table doesn't have a [%s] column?",
          ColumnHolder.TIME_COLUMN_NAME
      );
    }
    this.timeColumnPosition = foundTimePosition;
    this.effectiveGranularity = granularity;

    // Specs always start unwired: there's a chicken-and-egg between the summary and its specs, resolved by
    // deferring all summary-dependent state on the spec to setSummary, which we invoke here once the summary's
    // own state is populated.
    for (TableClusterGroupSpec spec : this.clusterGroups) {
      spec.setSummary(this);
    }
  }

  @JsonIgnore
  @Override
  public List<String> getColumnNames()
  {
    return new ArrayList<>(columnNames);
  }

  @JsonProperty
  @Override
  public VirtualColumns getVirtualColumns()
  {
    return virtualColumns;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columnNames;
  }

  @JsonProperty
  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @JsonProperty
  public RowSignature getClusteringColumns()
  {
    return clusteringColumns;
  }

  /**
   * Columns which have common data stored once under {@link Projections#BASE_TABLE_PROJECTION_NAME} and shared by
   * all cluster-group entries
   */
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<String> getSharedColumns()
  {
    return sharedColumns;
  }

  /**
   *  Per-type clustering value dictionaries; see {@link ClusteringDictionaries} for routing semantics.
   */
  @JsonProperty
  public ClusteringDictionaries getClusteringDictionaries()
  {
    return clusteringDictionaries;
  }

  /**
   * Materialize the typed value at clustering position {@code clusteringColumnIndex} with dictionary
   * {@code dictionaryId}.
   */
  Object lookupClusteringValue(int clusteringColumnIndex, int dictionaryId)
  {
    return clusteringDictionaries.lookupValue(
        clusteringColumns.getColumnType(clusteringColumnIndex).orElseThrow(),
        dictionaryId
    );
  }

  /**
   * The cluster groups nested in this summary, in clustering-value sort order. Walking groups back-to-back in
   * this order yields rows in the segment's full declared ordering (clustering values monotonically advance
   * across groups; within each group, rows follow the segment ordering with the clustering prefix dropped).
   */
  @JsonProperty("groups")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<TableClusterGroupSpec> getClusterGroups()
  {
    return clusterGroups;
  }

  /**
   *  Per-group sort order: the segment ordering with the clustering-column prefix dropped.
   */
  @JsonIgnore
  public List<OrderBy> getGroupOrdering()
  {
    return ordering.subList(clusteringColumns.size(), ordering.size());
  }

  /**
   * Per-group column names: this summary's full column list minus the clustering columns.
   */
  @JsonIgnore
  public List<String> getGroupColumnNames()
  {
    final Set<String> clusteringNames = new HashSet<>(clusteringColumns.getColumnNames());
    final List<String> all = getColumnNames();
    final List<String> result = new ArrayList<>(all.size());
    for (String c : all) {
      if (!clusteringNames.contains(c)) {
        result.add(c);
      }
    }
    return result;
  }

  /**
   * Per-group dimension names: {@link #getDimensionNames()} minus the clustering columns.
   */
  @JsonIgnore
  public List<String> getGroupDimensionNames()
  {
    final Set<String> clusteringNames = new HashSet<>(clusteringColumns.getColumnNames());
    final List<String> dims = getDimensionNames();
    final List<String> result = new ArrayList<>(dims.size());
    for (String d : dims) {
      if (!clusteringNames.contains(d)) {
        result.add(d);
      }
    }
    return result;
  }

  @JsonIgnore
  @Override
  public int getTimeColumnPosition()
  {
    return timeColumnPosition;
  }

  @JsonIgnore
  @Override
  public Granularity getEffectiveGranularity()
  {
    return effectiveGranularity;
  }

  @JsonIgnore
  @Override
  public List<String> getDimensionNames()
  {
    if (timeColumnPosition == 0) {
      return columnNames.subList(1, columnNames.size());
    }
    final List<String> dimsWithoutTime = Lists.newArrayListWithCapacity(columnNames.size() - 1);
    for (String column : columnNames) {
      if (ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
        continue;
      }
      dimsWithoutTime.add(column);
    }
    return dimsWithoutTime;
  }

  @Override
  public Metadata asMetadata(@Nullable List<AggregateProjectionMetadata> projections)
  {
    return new Metadata(
        null,
        null,
        null,
        effectiveGranularity,
        false,
        ordering,
        projections,
        this
    );
  }

  /**
   * Convert {@link #clusterGroups} to {@link ClusterGroupTuples} for
   * {@link org.apache.druid.timeline.DataSegment#getClusterGroups()}, to expose what cluster groups the segment
   * contains to coordinators and brokers.
   */
  public ClusterGroupTuples toClusterGroupTuples()
  {
    final List<List<Object>> tuples = new ArrayList<>(clusterGroups.size());
    for (TableClusterGroupSpec group : clusterGroups) {
      tuples.add(Arrays.asList(group.lookupClusteringValues()));
    }
    return new ClusterGroupTuples(
        clusteringColumns,
        virtualColumns.isEmpty() ? null : virtualColumns,
        tuples
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
    ClusteredValueGroupsBaseTableSchema that = (ClusteredValueGroupsBaseTableSchema) o;
    return Objects.equals(virtualColumns, that.virtualColumns)
           && Objects.equals(columnNames, that.columnNames)
           && Objects.equals(ordering, that.ordering)
           && Objects.equals(clusteringColumns, that.clusteringColumns)
           && Objects.equals(sharedColumns, that.sharedColumns)
           && Objects.equals(clusteringDictionaries, that.clusteringDictionaries)
           && Objects.equals(clusterGroups, that.clusterGroups);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        virtualColumns,
        columnNames,
        ordering,
        clusteringColumns,
        sharedColumns,
        clusteringDictionaries,
        clusterGroups
    );
  }

  @Override
  public String toString()
  {
    return "ClusteredValueGroupsBaseTableSchema{" +
           "virtualColumns=" + virtualColumns +
           ", columnNames=" + columnNames +
           ", ordering=" + ordering +
           ", clusteringColumns=" + clusteringColumns +
           ", sharedColumns=" + sharedColumns +
           ", clusteringDictionaries=" + clusteringDictionaries +
           ", clusterGroups=" + clusterGroups +
           '}';
  }
}
