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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ConstantColumns;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.file.SegmentFileMapper;
import org.apache.druid.segment.projections.ClusteredColumnInspector;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 *
 */
public abstract class SimpleQueryableIndex implements QueryableIndex
{
  public static final Interner<List<OrderBy>> ORDERING_INTERNER = Interners.newWeakInterner();

  private final Interval dataInterval;
  private final List<String> columnNames;
  private final Indexed<String> availableDimensions;
  private final BitmapFactory bitmapFactory;
  private final ColumnHolderTable columns;
  private final List<OrderBy> ordering;
  private final Map<String, AggregateProjectionMetadata> projectionsMap;
  private final SortedSet<AggregateProjectionMetadata> projections;
  private final Map<String, ColumnHolderTable> projectionColumns;
  @Nullable
  private final ClusteredValueGroupsBaseTableSchema clusteredBaseSummary;
  @Nullable
  private final ClusteredColumnInspector clusteredColumnInspector;
  private final List<ColumnHolderTable> clusterGroupColumns;
  private final SegmentFileMapper fileMapper;
  private final Supplier<Map<String, DimensionHandler>> dimensionHandlers;

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<BaseColumnHolder>> columns,
      SegmentFileMapper fileMapper
  )
  {
    this(
        dataInterval,
        dimNames,
        bitmapFactory,
        ColumnHolderTable.fromSupplierMap(columns),
        fileMapper,
        null,
        null,
        null,
        null,
        null
    );
  }

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<BaseColumnHolder>> columns,
      SegmentFileMapper fileMapper,
      @Nullable Metadata metadata,
      @Nullable Map<String, Map<String, Supplier<BaseColumnHolder>>> projectionColumns
  )
  {
    this(
        dataInterval,
        dimNames,
        bitmapFactory,
        ColumnHolderTable.fromSupplierMap(columns),
        fileMapper,
        metadata,
        projectionTablesFromMaps(projectionColumns),
        null,
        null,
        null
    );
  }

  /**
   * Creates an index from {@link ColumnHolderTable}s.
   */
  SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      ColumnHolderTable columns,
      SegmentFileMapper fileMapper,
      @Nullable Metadata metadata,
      @Nullable Map<String, ColumnHolderTable> projectionColumns,
      @Nullable ClusteredValueGroupsBaseTableSchema clusteredBaseSummary,
      @Nullable List<ColumnHolderTable> clusterGroupColumns,
      @Nullable ClusteredColumnInspector clusteredColumnInspector
  )
  {
    // For clustered base tables, the top-level columns map is empty; all column data lives under per-cluster-group
    // entries in clusterGroupColumns. For all other schema shapes, __time must be present in the top-level columns map
    Preconditions.checkState(
        columns.isEmpty() || columns.contains(ColumnHolder.TIME_COLUMN_NAME),
        "Missing column[%s]",
        ColumnHolder.TIME_COLUMN_NAME
    );
    this.dataInterval = Preconditions.checkNotNull(dataInterval, "dataInterval");
    LinkedHashSet<String> dimsFirst = new LinkedHashSet<>();
    for (String dimName : dimNames) {
      dimsFirst.add(dimName);
    }
    for (String columnName : columns.getColumnNames()) {
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
        dimsFirst.add(columnName);
      }
    }
    this.columnNames = ImmutableList.copyOf(dimsFirst);
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;

    this.projectionColumns = projectionColumns == null || projectionColumns.isEmpty()
                             ? Collections.emptyMap()
                             : projectionColumns;
    DruidException.conditionalDefensive(
        (clusteredBaseSummary == null) == (clusteredColumnInspector == null),
        "Clustered column capabilities must be provided exactly when the clustered summary is"
    );
    this.clusteredBaseSummary = clusteredBaseSummary;
    this.clusteredColumnInspector = clusteredColumnInspector;
    this.clusterGroupColumns = clusterGroupColumns == null
                               ? Collections.emptyList()
                               : List.copyOf(clusterGroupColumns);
    this.dimensionHandlers = Suppliers.memoize(() -> initDimensionHandlers(availableDimensions));

    if (metadata != null) {
      if (metadata.getOrdering() != null) {
        this.ordering = ORDERING_INTERNER.intern(metadata.getOrdering());
      } else {
        this.ordering = Cursors.ascendingTimeOrder();
      }
      if (metadata.getProjections() != null && !metadata.getProjections().isEmpty()) {
        this.projectionsMap = Maps.newHashMapWithExpectedSize(metadata.getProjections().size());
        this.projections = new ObjectAVLTreeSet<>(AggregateProjectionMetadata.COMPARATOR);
        for (AggregateProjectionMetadata projection : metadata.getProjections()) {
          projections.add(projection);
          projectionsMap.put(projection.getSchema().getName(), projection);
        }
      } else {
        this.projectionsMap = Collections.emptyMap();
        this.projections = Collections.emptySortedSet();
      }
    } else {
      // When sort order isn't available from metadata, assume the segment is sorted by __time.
      this.ordering = Cursors.ascendingTimeOrder();
      this.projections = Collections.emptySortedSet();
      this.projectionsMap = Collections.emptyMap();
    }
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return columns.get(ColumnHolder.TIME_COLUMN_NAME).getLength();
  }

  @Override
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return availableDimensions;
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    return bitmapFactory;
  }

  @Nullable
  @Override
  public BaseColumnHolder getColumnHolder(String columnName)
  {
    return columns.get(columnName);
  }

  /**
   * Clustered segments store no top-level columns, so the default holder-based lookup would report null for every
   * logical column. Resolve instead from {@link ClusteredColumnInspector}, which combines every cluster group's
   * column descriptors without reading column data. Group sub-indexes have a null summary and fall through to the
   * default holder-based path, as do non-clustered segments.
   */
  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (clusteredColumnInspector != null) {
      return clusteredColumnInspector.getColumnCapabilities(column);
    } else {
      return QueryableIndex.super.getColumnCapabilities(column);
    }
  }

  @VisibleForTesting
  public Map<String, Supplier<BaseColumnHolder>> getColumns()
  {
    return columns.toSupplierMap();
  }

  /**
   * Returns the {@link ClusteredValueGroupsBaseTableSchema} summary entry if this index is for a clustered segment, or null for
   * a non-clustered segment. The summary owns segment-wide clustering config (clustering column signature, shared-
   * column markers, naming-scheme version)
   */
  @Override
  @Nullable
  public ClusteredValueGroupsBaseTableSchema getClusteredBaseSummary()
  {
    return clusteredBaseSummary;
  }

  /**
   * Returns the cluster groups nested in this index's summary, in their original order. Empty for a non-clustered
   * segment. Used by query-time dispatch to enumerate groups and feed {@link Projections#pruneClusterGroups}.
   */
  @Override
  public List<TableClusterGroupSpec> getClusterGroupSchemas()
  {
    return clusteredBaseSummary == null ? Collections.emptyList() : clusteredBaseSummary.getClusterGroups();
  }

  /**
   * Returns a {@link QueryableIndex} sub-view scoped to a single cluster group's column data. Mirrors
   * {@link #getProjectionQueryableIndex(String)} but for cluster groups: addressed by reference to the spec, not by
   * name. The returned index's columns are the group's per-group columns; clustering columns are NOT present in
   * the returned index, they're injected at the cursor-factory level via {@code ClusteringColumnSelectorFactory}.
   */
  @Override
  public QueryableIndex getClusterGroupQueryableIndex(TableClusterGroupSpec groupSpec, boolean withClusteringColumns)
  {
    if (clusteredBaseSummary == null) {
      throw DruidException.defensive("getClusterGroupQueryableIndex called on a non-clustered segment");
    }
    final List<TableClusterGroupSpec> groups = clusteredBaseSummary.getClusterGroups();
    final int index = groups.indexOf(groupSpec);
    if (index < 0) {
      throw DruidException.defensive("Cluster group spec is not part of this segment");
    }
    // add clustering columns are constants for query paths
    final ColumnHolderTable groupColumns;
    if (withClusteringColumns) {
      final ColumnHolderTable.Builder groupColumnsBuilder = clusterGroupColumns.get(index).toBuilder();
      ConstantColumns.addConstantClusteringColumns(
          groupColumnsBuilder::put,
          clusteredBaseSummary.getClusteringColumns(),
          groupSpec.lookupClusteringValues(),
          groupSpec.getNumRows(),
          bitmapFactory
      );
      groupColumns = groupColumnsBuilder.build();
    } else {
      groupColumns = clusterGroupColumns.get(index);
    }
    final Metadata groupMetadata = new Metadata(
        null,
        null,
        null,
        clusteredBaseSummary.getEffectiveGranularity(),
        false,
        clusteredBaseSummary.getGroupOrdering(),
        null,
        null
    );
    return new SimpleQueryableIndex(
        dataInterval,
        new ListIndexed<>(clusteredBaseSummary.getGroupDimensionNames()),
        bitmapFactory,
        groupColumns,
        fileMapper,
        groupMetadata,
        null,
        null,
        null,
        null
    )
    {
      @Override
      public Metadata getMetadata()
      {
        return groupMetadata;
      }
    };
  }

  @VisibleForTesting
  public SegmentFileMapper getFileMapper()
  {
    return fileMapper;
  }

  @Override
  public void close()
  {
    if (fileMapper != null) {
      fileMapper.close();
    }
  }

  @Override
  public abstract Metadata getMetadata();

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return dimensionHandlers.get();
  }

  private Map<String, DimensionHandler> initDimensionHandlers(Indexed<String> availableDimensions)
  {
    Map<String, DimensionHandler> dimensionHandlerMap = Maps.newLinkedHashMap();
    for (String dim : availableDimensions) {
      final ColumnHolder columnHolder = getColumnHolder(dim);
      final DimensionHandler handler = columnHolder.getColumnFormat().getColumnHandler(dim);
      dimensionHandlerMap.put(dim, handler);
    }
    return dimensionHandlerMap;
  }

  @Nullable
  @Override
  public QueryableProjection<QueryableIndex> getProjection(CursorBuildSpec cursorBuildSpec)
  {
    return Projections.findMatchingProjection(
        cursorBuildSpec,
        projections,
        dataInterval,
        (projectionName, columnName) ->
            projectionColumns.get(projectionName).contains(columnName) || getColumnCapabilities(columnName) == null,
        this::getProjectionQueryableIndex
    );
  }

  @Override
  public QueryableIndex getProjectionQueryableIndex(String name)
  {
    final AggregateProjectionMetadata projectionSpec = projectionsMap.get(name);
    final Metadata projectionMetadata = new Metadata(
        null,
        projectionSpec.getSchema().getAggregators(),
        null,
        null,
        true,
        projectionSpec.getSchema().getOrderingWithTimeColumnSubstitution(),
        null,
        null
    );
    return new SimpleQueryableIndex(
        dataInterval,
        new ListIndexed<>(
            projectionSpec.getSchema()
                          .getGroupingColumns()
                          .stream()
                          .filter(x -> !x.equals(projectionSpec.getSchema().getTimeColumnName()))
                          .collect(Collectors.toList())
        ),
        bitmapFactory,
        projectionColumns.get(name),
        fileMapper,
        projectionMetadata,
        null,
        null,
        null,
        null
    )
    {
      @Override
      public Metadata getMetadata()
      {
        return projectionMetadata;
      }

      @Override
      public int getNumRows()
      {
        return projectionSpec.getNumRows();
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        // return ordering with projection time column substituted with __time so query engines can treat it equivalently
        return projectionSpec.getSchema().getOrderingWithTimeColumnSubstitution();
      }
    };
  }

  /**
   * Converts per-projection column maps to {@link ColumnHolderTable}s.
   */
  @Nullable
  private static Map<String, ColumnHolderTable> projectionTablesFromMaps(
      @Nullable Map<String, Map<String, Supplier<BaseColumnHolder>>> projectionColumns
  )
  {
    if (projectionColumns == null) {
      return null;
    }
    final Map<String, ColumnHolderTable> tables = new LinkedHashMap<>();
    for (Map.Entry<String, Map<String, Supplier<BaseColumnHolder>>> entry : projectionColumns.entrySet()) {
      tables.put(entry.getKey(), ColumnHolderTable.fromSupplierMap(entry.getValue()));
    }
    return tables;
  }
}
