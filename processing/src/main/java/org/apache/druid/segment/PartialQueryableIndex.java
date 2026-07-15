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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ConstantColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileMapper;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.BaseTableProjectionSchema;
import org.apache.druid.segment.projections.ClusterGroupQueryPlan;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ConstantTimeColumn;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.ProjectionSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A {@link QueryableIndex} that loads projection and base table columns on demand from a
 * {@link PartialSegmentFileMapperV10}. Schema queries (column names, types, intervals, metadata) are answered from
 * the {@link SegmentFileMetadata} alone without triggering any downloads. Column data is only downloaded when a column
 * is accessed via {@link #getColumnHolder(String)} or {@link #getProjection(CursorBuildSpec)}.
 * <p>
 * Projection matching uses only metadata ({@link SegmentFileMetadata#getColumnDescriptors()} keys) to determine if
 * a projection can satisfy a query, avoiding downloads of projection data that won't be used.
 *
 * @see PartialSegmentFileMapperV10
 */
public class PartialQueryableIndex implements QueryableIndex
{
  private final Interval dataInterval;
  private final int baseNumRows;
  private final ProjectionMetadata baseProjectionMetadata;
  private final Indexed<String> availableDimensions;
  private final List<String> columnNames;
  private final BitmapFactory bitmapFactory;
  private final PartialSegmentFileMapperV10 fileMapper;
  private final SegmentFileMetadata metadata;
  private final ColumnConfig columnConfig;
  private final Metadata reconstructedMetadata;
  private final List<OrderBy> ordering;

  // projection metadata for matching
  private final SortedSet<AggregateProjectionMetadata> projections;
  private final Map<String, AggregateProjectionMetadata> projectionsMap;
  private final Map<String, ProjectionMetadata> projectionSpecs;

  // segment-internal file prefix for the base table projection, used to translate column names to descriptor keys
  private final String baseProjectionPrefix;

  // base table columns, built at construction time. each entry's supplier defers both mapFile() and column
  // deserialization until the column is actually accessed, so queries only trigger downloads for the specific
  // columns they use.
  private final Map<String, Supplier<BaseColumnHolder>> baseColumns;

  // projection columns, keyed by projection name. built on demand (per-projection) when the projection is matched.
  // within each projection, per-column suppliers defer both mapFile() and deserialization.
  private final ConcurrentHashMap<String, Map<String, Supplier<BaseColumnHolder>>> projectionColumnsByName =
      new ConcurrentHashMap<>();

  // clustered base summary when this segment is a clustered base table, else null. when non-null, the base table has
  // no top-level columns
  @Nullable
  private final ClusteredValueGroupsBaseTableSchema clusteredBaseSummary;

  // per-cluster-group column suppliers, keyed by group index (into the summary's group list). built on demand like
  // projectionColumnsByName; each supplier defers both mapFile() and deserialization until the column is read.
  private final ConcurrentHashMap<Integer, Map<String, Supplier<BaseColumnHolder>>> clusterGroupColumnsByIndex =
      new ConcurrentHashMap<>();

  // lazy dimension handlers
  private final Supplier<Map<String, DimensionHandler>> dimensionHandlers;

  public PartialQueryableIndex(
      SegmentFileMetadata metadata,
      PartialSegmentFileMapperV10 fileMapper,
      ColumnConfig columnConfig
  )
  {
    this.metadata = metadata;
    this.fileMapper = fileMapper;
    this.columnConfig = columnConfig;

    // base table projection is always first
    final ProjectionMetadata baseProjection = metadata.getProjections().get(0);
    DruidException.conditionalDefensive(
        Projections.BASE_TABLE_PROJECTION_NAME.equals(baseProjection.getSchema().getName()),
        "Expected base table projection with name[%s], but got projection with name[%s] instead",
        Projections.BASE_TABLE_PROJECTION_NAME,
        baseProjection.getSchema().getName()
    );
    final BaseTableProjectionSchema baseSchema = (BaseTableProjectionSchema) baseProjection.getSchema();
    this.clusteredBaseSummary = baseSchema instanceof ClusteredValueGroupsBaseTableSchema
                                ? (ClusteredValueGroupsBaseTableSchema) baseSchema
                                : null;
    if (clusteredBaseSummary != null && !clusteredBaseSummary.getSharedColumns().isEmpty()) {
      // Shared base columns aren't wired into partial loading yet
      throw DruidException.defensive(
          "Clustered V10 segments with shared columns%s are not yet supported for partial loading (interval[%s])",
          clusteredBaseSummary.getSharedColumns(),
          metadata.getInterval()
      );
    }
    this.baseProjectionMetadata = baseProjection;
    this.baseNumRows = baseProjection.getNumRows();
    this.baseProjectionPrefix = Projections.getProjectionSegmentInternalFilePrefix(baseSchema);
    this.dataInterval = Intervals.of(metadata.getInterval());
    this.bitmapFactory = metadata.getBitmapEncoding().getBitmapFactory();

    // A clustered base table has no top-level columns (its data lives in per-cluster-group bundles), so its available
    // dimensions / column names are empty; logical columns are resolved via getColumnCapabilities + cluster-group
    // dispatch (matching the eager SimpleQueryableIndex). A regular base table lists dimensions first, then the
    // remaining columns (excluding __time).
    if (clusteredBaseSummary != null) {
      this.availableDimensions = new ListIndexed<>(List.of());
      this.columnNames = List.of();
    } else {
      this.availableDimensions = new ListIndexed<>(baseSchema.getDimensionNames());
      final LinkedHashSet<String> dimsFirst = new LinkedHashSet<>(baseSchema.getDimensionNames());
      for (String columnName : baseSchema.getColumnNames()) {
        if (!ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
          dimsFirst.add(columnName);
        }
      }
      this.columnNames = List.copyOf(dimsFirst);
    }

    // build aggregate projection metadata for matching
    final List<AggregateProjectionMetadata> aggProjections = new ArrayList<>();
    this.projectionSpecs = new ConcurrentHashMap<>();
    boolean first = true;
    for (ProjectionMetadata projectionSpec : metadata.getProjections()) {
      if (first) {
        first = false;
        continue;
      }
      if (projectionSpec.getSchema() instanceof AggregateProjectionSchema) {
        aggProjections.add(
            new AggregateProjectionMetadata(
                (AggregateProjectionSchema) projectionSpec.getSchema(),
                projectionSpec.getNumRows()
            )
        );
        projectionSpecs.put(projectionSpec.getSchema().getName(), projectionSpec);
      } else {
        throw DruidException.defensive(
            "Unexpected projection[%s] with type[%s]",
            projectionSpec.getSchema().getName(),
            projectionSpec.getSchema().getClass()
        );
      }
    }

    this.reconstructedMetadata = baseSchema.asMetadata(aggProjections);
    if (reconstructedMetadata.getOrdering() != null) {
      this.ordering = SimpleQueryableIndex.ORDERING_INTERNER.intern(reconstructedMetadata.getOrdering());
    } else {
      this.ordering = Cursors.ascendingTimeOrder();
    }

    this.projectionsMap = Maps.newHashMapWithExpectedSize(aggProjections.size());
    this.projections = new ObjectAVLTreeSet<>(AggregateProjectionMetadata.COMPARATOR);
    for (AggregateProjectionMetadata projection : aggProjections) {
      projections.add(projection);
      projectionsMap.put(projection.getSchema().getName(), projection);
    }

    // build per-column suppliers for the base table. each supplier is memoized and defers both mapFile() and
    // deserialization until the column is accessed. A clustered base has no top-level columns (its data lives in
    // per-cluster-group bundles), so its base column map is empty.
    this.baseColumns = clusteredBaseSummary != null
                       ? Map.of()
                       : buildProjectionColumnSuppliers(baseProjection, Map.of());

    this.dimensionHandlers = Suppliers.memoize(this::initDimensionHandlers);
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return baseNumRows;
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return availableDimensions;
  }

  @Override
  public BitmapFactory getBitmapFactoryForDimensions()
  {
    return bitmapFactory;
  }

  @Override
  public Metadata getMetadata()
  {
    return reconstructedMetadata;
  }

  /**
   * Returns the {@link ProjectionMetadata} for the base table projection.
   */
  public ProjectionMetadata getBaseProjectionMetadata()
  {
    return baseProjectionMetadata;
  }

  /**
   * Whether every internal file referenced by this index's segment metadata (including any attached external file
   * mappers) is already downloaded. Used by partial loaded segments to gate sync paths to confirm the segment
   * is fully loaded before performing operations. Only async paths are allowed to perform on-demand downloads.
   */
  public boolean isFullyDownloaded()
  {
    return fileMapper.isFullyDownloaded();
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return dimensionHandlers.get();
  }

  @Override
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public List<OrderBy> getOrdering()
  {
    return ordering;
  }

  @Nullable
  @Override
  public BaseColumnHolder getColumnHolder(String columnName)
  {
    final Supplier<BaseColumnHolder> supplier = baseColumns.get(columnName);
    return supplier == null ? null : supplier.get();
  }

  /**
   * Answers from metadata without triggering column downloads. The default implementation in {@link QueryableIndex}
   * calls {@link #getColumnHolder(String)}, which would force a base table load.
   */
  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (clusteredBaseSummary != null) {
      return getClusteredColumnCapabilities(column);
    }
    // look up the column in the base table projection's namespace
    final String smooshName = baseProjectionPrefix + column;
    return capabilitiesFromDescriptor(metadata.getColumnDescriptors().get(smooshName));
  }

  /**
   * Column capabilities for a clustered base table, answered from metadata only (no downloads): clustering columns
   * resolve from the summary's typed clustering signature; data columns (and {@code __time}) resolve from the first
   * cluster group's {@link ColumnDescriptor} (all groups share the same per-group shape). Mirrors the eager
   * {@link SimpleQueryableIndex} clustered branch, but reads the descriptor directly rather than routing through a
   * group sub-index's {@code getColumnHolder} (which would trigger a download).
   */
  @Nullable
  private ColumnCapabilities getClusteredColumnCapabilities(String column)
  {
    final ColumnType clusteringType = clusteredBaseSummary.getClusteringColumns().getColumnType(column).orElse(null);
    if (clusteringType != null) {
      return clusteringType.is(ValueType.STRING)
             ? ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
             : ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(clusteringType);
    }
    final List<TableClusterGroupSpec> groups = clusteredBaseSummary.getClusterGroups();
    if (groups.isEmpty()) {
      return null;
    }
    final String smooshName =
        Projections.getClusterGroupSegmentInternalFileName(groups.getFirst().getClusteringValueIds(), column);
    return capabilitiesFromDescriptor(metadata.getColumnDescriptors().get(smooshName));
  }

  @Nullable
  private static ColumnCapabilities capabilitiesFromDescriptor(@Nullable ColumnDescriptor descriptor)
  {
    if (descriptor == null) {
      return null;
    }
    return ColumnCapabilitiesImpl.createDefault()
                                 .setType(descriptor.toColumnType())
                                 .setHasMultipleValues(descriptor.isHasMultipleValues());
  }

  @Nullable
  @Override
  public QueryableProjection<QueryableIndex> getProjection(CursorBuildSpec cursorBuildSpec)
  {
    return Projections.findMatchingProjection(
        cursorBuildSpec,
        projections,
        dataInterval,
        this::checkProjectionHasColumn,
        this::getProjectionQueryableIndex
    );
  }

  private boolean checkProjectionHasColumn(String projectionName, String columnName)
  {
    final ProjectionMetadata meta = projectionSpecs.get(projectionName);
    if (meta == null) {
      return false;
    }
    final String file = Projections.getProjectionSegmentInternalFileName(meta.getSchema(), columnName);
    return metadata.getColumnDescriptors().containsKey(file) || getColumnCapabilities(columnName) == null;
  }

  @Nullable
  @Override
  public QueryableIndex getProjectionQueryableIndex(String name)
  {
    final AggregateProjectionMetadata projectionMeta = projectionsMap.get(name);
    if (projectionMeta == null) {
      return null;
    }

    // build per-column suppliers for this projection on first access. the suppliers themselves still defer download
    // and deserialization until individual columns are read.
    final Map<String, Supplier<BaseColumnHolder>> projColumns = projectionColumnsByName.computeIfAbsent(
        name,
        projName -> buildProjectionColumnSuppliers(projectionSpecs.get(projName), baseColumns)
    );

    final Metadata projectionMetadata = new Metadata(
        null,
        projectionMeta.getSchema().getAggregators(),
        null,
        null,
        true,
        projectionMeta.getSchema().getOrderingWithTimeColumnSubstitution(),
        null,
        null
    );

    return new SimpleQueryableIndex(
        dataInterval,
        new ListIndexed<>(
            projectionMeta.getSchema()
                          .getGroupingColumns()
                          .stream()
                          .filter(x -> !x.equals(projectionMeta.getSchema().getTimeColumnName()))
                          .collect(Collectors.toList())
        ),
        bitmapFactory,
        projColumns,
        fileMapper,
        projectionMetadata,
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
        return projectionMeta.getNumRows();
      }

      @Override
      public List<OrderBy> getOrdering()
      {
        return projectionMeta.getSchema().getOrderingWithTimeColumnSubstitution();
      }
    };
  }

  @Nullable
  @Override
  public ClusteredValueGroupsBaseTableSchema getClusteredBaseSummary()
  {
    return clusteredBaseSummary;
  }

  @Override
  public List<TableClusterGroupSpec> getClusterGroupSchemas()
  {
    return clusteredBaseSummary == null ? List.of() : clusteredBaseSummary.getClusterGroups();
  }

  /**
   * Returns a {@link QueryableIndex} sub-view scoped to a single cluster group's column data, The per-group column
   * suppliers are memoized by group index so a pre-fetch (async path) and the later cursor build share the same
   * already-downloaded columns. Clustering columns are NOT present in the returned index; they are injected at the
   * cursor-factory level via {@code ClusteringColumnSelectorFactory}.
   */
  @Override
  public QueryableIndex getClusterGroupQueryableIndex(TableClusterGroupSpec groupSpec, boolean withClusteringColumns)
  {
    if (clusteredBaseSummary == null) {
      throw DruidException.defensive("getClusterGroupQueryableIndex called on a non-clustered segment");
    }
    final List<TableClusterGroupSpec> groups = clusteredBaseSummary.getClusterGroups();
    final int groupIndex = groups.indexOf(groupSpec);
    if (groupIndex < 0) {
      throw DruidException.defensive("Cluster group spec is not part of this segment");
    }
    final Map<String, Supplier<BaseColumnHolder>> baseColumns = clusterGroupColumnsByIndex.computeIfAbsent(
        groupIndex,
        i -> buildColumnSuppliers(
            clusteredBaseSummary.getTimeColumnName(),
            groupSpec.getNumRows(),
            clusteredBaseSummary.getGroupColumnNames(),
            column -> Projections.getClusterGroupSegmentInternalFileName(groupSpec.getClusteringValueIds(), column),
            Map.of()
        )
    );

    final Map<String, Supplier<BaseColumnHolder>> groupColumns;
    if (withClusteringColumns) {
      groupColumns = new HashMap<>(baseColumns);
      ConstantColumns.addConstantClusteringColumns(
          groupColumns,
          clusteredBaseSummary.getClusteringColumns(),
          groupSpec.lookupClusteringValues(),
          groupSpec.getNumRows(),
          bitmapFactory
      );
    } else {
      groupColumns = baseColumns;
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

  /**
   * Plan the downloads a {@link CursorBuildSpec} needs: resolve which row selector serves the spec (matched
   * aggregate projection, surviving cluster groups, or the base table), compute each selector's required columns,
   * and plan the coalesced range reads that make them resident. The returned plan carries the dispatch result so the
   * caller can build the eventual cursor holder against the same match without re-running it.
   */
  CursorPrefetchPlan planCursorPrefetch(CursorBuildSpec spec)
  {
    final QueryableProjection<QueryableIndex> matched = getProjection(spec);
    if (matched == null && clusteredBaseSummary != null) {
      // Clustered base table: metadata-only group resolution; only the groups whose clustering tuples survive the
      // query's filters plan any downloads, one bundle per surviving group.
      final ClusterGroupQueryPlan clusterGroupPlan = Projections.planClusterGroupQuery(
          new ArrayList<>(getClusterGroupSchemas()),
          spec
      );
      final List<PrefetchBundle> bundles = new ArrayList<>(clusterGroupPlan.survivingGroups().size());
      for (TableClusterGroupSpec group : clusterGroupPlan.survivingGroups()) {
        final QueryableIndex groupIndex = getClusterGroupQueryableIndex(group, true);
        final CursorBuildSpec groupSpec = clusterGroupPlan.rebuildCursorBuildSpec(spec, group);
        final Set<String> required = requiredColumns(groupIndex, null, groupSpec);
        bundles.add(
            new PrefetchBundle(
                Projections.getClusterGroupBundleName(group.getClusteringValueIds()),
                groupIndex,
                required,
                planClusterGroupPrefetch(group, required)
            )
        );
      }
      return new CursorPrefetchPlan(null, clusterGroupPlan, bundles);
    }

    // Aggregate-projection match, or the plain base table: the selector's bundle with the query's required columns,
    // plus (for a matched projection) the base-table bundle when required projection columns read through base
    // parents.
    final QueryableIndex rowSelector = matched != null ? matched.getRowSelector() : this;
    final String bundleName = matched != null ? matched.getName() : Projections.BASE_TABLE_PROJECTION_NAME;
    final Set<String> required = requiredColumns(rowSelector, matched, spec);
    final List<PartialSegmentFileMapperV10.PlannedFetch> fetches =
        matched != null ? planProjectionPrefetch(matched.getName(), required) : planBaseTablePrefetch(required);
    final List<PrefetchBundle> bundles = new ArrayList<>(2);
    bundles.add(new PrefetchBundle(bundleName, rowSelector, required, fetches));
    if (matched != null) {
      final Set<String> parents = projectionParentColumns(matched.getName(), required);
      if (!parents.isEmpty()) {
        // Materializing a projection column also materializes its same-named base column when one exists
        // (buildColumnSuppliers reads through the parent, e.g. for dictionary reuse), so those parents are part of
        // this query's working set: plan their files and hold the base bundle so the parent mmaps are
        // eviction-excluded for the holder's whole lifetime. When the metadata predates recorded column file lists
        // the parent files can't be enumerated; the bundle is then hold-only (no planned fetches) and the parents
        // download lazily during materialization, under the hold, on the download executor.
        bundles.add(
            new PrefetchBundle(
                Projections.BASE_TABLE_PROJECTION_NAME,
                this,
                parents,
                metadata.getColumnFiles() == null ? List.of() : planBaseTablePrefetch(parents)
            )
        );
      }
    }
    return new CursorPrefetchPlan(matched, null, bundles);
  }

  /**
   * The base-table parent columns that materializing {@code requiredProjectionColumns} on {@code projectionName}
   * pulls in: {@link #buildColumnSuppliers} materializes a projection column's same-named base column whenever the
   * base table has one (the descriptor's serde decides whether it actually reads through the parent, but the parent
   * holder is materialized regardless), so this mirrors that same-name rule instead of inspecting serdes, applied to
   * each required column's {@link #resolvePhysicalColumn physical resolution}. Always empty for clustered base
   * tables, whose base column map is empty.
   */
  private Set<String> projectionParentColumns(String projectionName, Set<String> requiredProjectionColumns)
  {
    if (baseColumns.isEmpty()) {
      return Set.of();
    }
    final ProjectionSchema schema = projectionSpecs.get(projectionName).getSchema();
    final Function<String, String> fileNameFn =
        column -> Projections.getProjectionSegmentInternalFileName(schema, column);
    final Set<String> parents = new LinkedHashSet<>();
    for (String column : requiredProjectionColumns) {
      // the parent pull only happens through an existing projection column's supplier, so a column that resolves to
      // no descriptor-backed physical column (constant time, virtual, unknown) has no parent either
      final String physical = resolvePhysicalColumn(schema.getTimeColumnName(), fileNameFn, column);
      if (physical != null && baseColumns.containsKey(physical)) {
        parents.add(physical);
      }
    }
    return parents;
  }

  @Override
  public void close()
  {
    fileMapper.close();
  }

  private Map<String, DimensionHandler> initDimensionHandlers()
  {
    final Map<String, DimensionHandler> handlers = Maps.newLinkedHashMap();
    for (String dim : availableDimensions) {
      final ColumnHolder columnHolder = getColumnHolder(dim);
      if (columnHolder != null) {
        handlers.put(dim, columnHolder.getColumnFormat().getColumnHandler(dim));
      }
    }
    return handlers;
  }

  /**
   * Build a map of column name to per-column supplier for the given projection. Each supplier defers both
   * {@link SegmentFileMapper#mapFile} and {@link ColumnDescriptor#read} until the column is actually accessed, so
   * queries only trigger downloads for the specific columns they use.
   */
  private Map<String, Supplier<BaseColumnHolder>> buildProjectionColumnSuppliers(
      ProjectionMetadata projectionSpec,
      Map<String, Supplier<BaseColumnHolder>> parentColumns
  )
  {
    return buildColumnSuppliers(
        projectionSpec.getSchema().getTimeColumnName(),
        projectionSpec.getNumRows(),
        projectionSpec.getSchema().getColumnNames(),
        column -> Projections.getProjectionSegmentInternalFileName(projectionSpec.getSchema(), column),
        parentColumns
    );
  }

  /**
   * Shared builder for lazy per-column suppliers. Each supplier is memoized and defers both
   * {@link SegmentFileMapper#mapFile} and {@link ColumnDescriptor#read} until the column is actually accessed, so
   * queries only trigger downloads for the specific columns they use. {@code fileNameFn} maps a logical column name to
   * its segment-internal (smoosh) file name in the right bundle namespace.
   */
  private Map<String, Supplier<BaseColumnHolder>> buildColumnSuppliers(
      @Nullable String timeColumnName,
      int numRows,
      List<String> columnNames,
      Function<String, String> fileNameFn,
      Map<String, Supplier<BaseColumnHolder>> parentColumns
  )
  {
    final boolean renameTime = !ColumnHolder.TIME_COLUMN_NAME.equals(timeColumnName);
    final Map<String, Supplier<BaseColumnHolder>> columns = new LinkedHashMap<>();

    for (String column : columnNames) {
      final String smooshName = fileNameFn.apply(column);
      final ColumnDescriptor columnDescriptor = metadata.getColumnDescriptors().get(smooshName);
      if (columnDescriptor == null) {
        continue;
      }

      final String internedColumnName = SmooshedFileMapper.STRING_INTERNER.intern(column);
      final Supplier<BaseColumnHolder> columnSupplier = Suppliers.memoize(() -> {
        try {
          final ByteBuffer colBuffer = fileMapper.mapFile(smooshName);
          final BaseColumnHolder parentColumn =
              parentColumns.containsKey(column) ? parentColumns.get(column).get() : null;
          return columnDescriptor.read(colBuffer, columnConfig, fileMapper, parentColumn);
        }
        catch (IOException e) {
          throw DruidException.defensive(e, "Failed to load column[%s]", smooshName);
        }
      });

      columns.put(internedColumnName, columnSupplier);

      if (column.equals(timeColumnName) && renameTime) {
        columns.put(ColumnHolder.TIME_COLUMN_NAME, columns.get(column));
        columns.remove(column);
      }
    }

    if (timeColumnName == null) {
      columns.put(
          ColumnHolder.TIME_COLUMN_NAME,
          ConstantTimeColumn.makeConstantTimeSupplier(numRows, dataInterval.getStartMillis())
      );
    }

    return columns;
  }

  /**
   * Shared planning for the {@code plan*Prefetch} methods: resolve the columns to their recorded internal-file sets
   * and plan coalesced range reads for them ({@link PartialSegmentFileMapperV10#planParallelFetch}, since the cursor
   * factory executes the runs concurrently). A column's serde may have written some of its files into external
   * segment files; each external is its own {@link PartialSegmentFileMapperV10} recording those files in its own
   * metadata under the same column key, so every mapper plans (and later executes) its own runs, paired via
   * {@link PartialSegmentFileMapperV10.PlannedFetch}. When this segment's metadata predates per-column file lists
   * (see {@link SegmentFileMetadata#getColumnFiles()}), falls back to planning the whole bundle across the main
   * mapper and every external, coarser (no column-level pruning within the bundle) but still one range read per
   * planned span (contiguous, capped at the parallel run-size limit) rather than one per internal file, and it
   * covers serde sub-files that can't be enumerated without the recorded lists.
   */
  private List<PartialSegmentFileMapperV10.PlannedFetch> planPrefetch(
      String bundleName,
      @Nullable String timeColumnName,
      Function<String, String> fileNameFn,
      Set<String> columns
  )
  {
    final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
    if (columnFiles == null) {
      // whole-bundle fallback; the mapper spans its externals itself
      return fileMapper.planParallelFetchBundle(bundleName);
    }

    final Set<String> smooshNames = resolveSmooshNames(timeColumnName, fileNameFn, columns);

    final List<PartialSegmentFileMapperV10.PlannedFetch> fetches = new ArrayList<>();
    addPlannedFetches(fetches, fileMapper, fileMapper.planParallelFetch(recordedFiles(columnFiles, smooshNames)));

    for (String externalName : fileMapper.getExternalFilenames()) {
      final PartialSegmentFileMapperV10 external = fileMapper.getExternalMapper(externalName);
      final Map<String, List<String>> externalColumnFiles = external.getSegmentFileMetadata().getColumnFiles();
      if (externalColumnFiles == null) {
        // this external holds no column-attributed files and there is nothing to plan from it.
        continue;
      }
      final Set<String> externalFiles = recordedFiles(externalColumnFiles, smooshNames);
      if (!externalFiles.isEmpty()) {
        addPlannedFetches(fetches, external, external.planParallelFetch(externalFiles));
      }
    }
    return fetches;
  }

  /**
   * Union of the recorded file lists of every smoosh name that has an entry in {@code columnFiles}.
   */
  private static Set<String> recordedFiles(Map<String, List<String>> columnFiles, Set<String> smooshNames)
  {
    final Set<String> files = new LinkedHashSet<>();
    for (String smooshName : smooshNames) {
      final List<String> recorded = columnFiles.get(smooshName);
      if (recorded != null) {
        files.addAll(recorded);
      }
    }
    return files;
  }

  private static void addPlannedFetches(
      List<PartialSegmentFileMapperV10.PlannedFetch> out,
      PartialSegmentFileMapperV10 mapper,
      List<PartialSegmentFileMapperV10.FetchRun> runs
  )
  {
    for (PartialSegmentFileMapperV10.FetchRun run : runs) {
      out.add(new PartialSegmentFileMapperV10.PlannedFetch(mapper, run));
    }
  }

  /**
   * File-name resolution for {@link #planPrefetch}: map each logical column to its bundle-namespaced smoosh name via
   * {@link #resolvePhysicalColumn}, dropping columns that resolve to nothing.
   */
  private Set<String> resolveSmooshNames(
      @Nullable String timeColumnName,
      Function<String, String> fileNameFn,
      Set<String> columns
  )
  {
    final Set<String> smooshNames = new LinkedHashSet<>();
    for (String column : columns) {
      final String physical = resolvePhysicalColumn(timeColumnName, fileNameFn, column);
      if (physical != null) {
        smooshNames.add(fileNameFn.apply(physical));
      }
    }
    return smooshNames;
  }

  /**
   * The single definition of logical-to-physical column resolution for plan-time helpers, mirroring
   * {@link #buildColumnSuppliers}: {@code __time} maps back to the bundle's raw time column name (the pre-rename
   * name the column supplier captured; {@code null} for a constant-time bundle means no physical column at all),
   * and only names with a registered descriptor count; columns with no descriptor (virtual, clustering constants,
   * unknown) resolve to {@code null}.
   */
  @Nullable
  private String resolvePhysicalColumn(
      @Nullable String timeColumnName,
      Function<String, String> fileNameFn,
      String column
  )
  {
    String physical = column;
    if (ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
      if (timeColumnName == null) {
        // constant time column, no physical files
        return null;
      }
      physical = timeColumnName;
    }
    return metadata.getColumnDescriptors().containsKey(fileNameFn.apply(physical)) ? physical : null;
  }

  /**
   * Plan the coalesced range reads needed to materialize {@code columns} on the base table row selector. See
   * {@link #planPrefetch} for the plan semantics and fallback behavior.
   */
  private List<PartialSegmentFileMapperV10.PlannedFetch> planBaseTablePrefetch(Set<String> columns)
  {
    final ProjectionSchema schema = baseProjectionMetadata.getSchema();
    return planPrefetch(
        Projections.BASE_TABLE_PROJECTION_NAME,
        schema.getTimeColumnName(),
        column -> Projections.getProjectionSegmentInternalFileName(schema, column),
        columns
    );
  }

  /**
   * Plan the coalesced range reads needed to materialize {@code columns} on the named aggregate projection's row
   * selector. See {@link #planPrefetch} for the plan semantics and fallback behavior.
   */
  private List<PartialSegmentFileMapperV10.PlannedFetch> planProjectionPrefetch(String projectionName, Set<String> columns)
  {
    final ProjectionMetadata projectionSpec = projectionSpecs.get(projectionName);
    if (projectionSpec == null) {
      throw DruidException.defensive("Unknown projection[%s]", projectionName);
    }
    final ProjectionSchema schema = projectionSpec.getSchema();
    return planPrefetch(
        projectionName,
        schema.getTimeColumnName(),
        column -> Projections.getProjectionSegmentInternalFileName(schema, column),
        columns
    );
  }

  /**
   * Plan the coalesced range reads needed to materialize {@code columns} on a cluster group's row selector. See
   * {@link #planPrefetch} for the plan semantics and fallback behavior.
   */
  private List<PartialSegmentFileMapperV10.PlannedFetch> planClusterGroupPrefetch(
      TableClusterGroupSpec groupSpec,
      Set<String> columns
  )
  {
    if (clusteredBaseSummary == null) {
      throw DruidException.defensive("planClusterGroupPrefetch called on a non-clustered segment");
    }
    return planPrefetch(
        Projections.getClusterGroupBundleName(groupSpec.getClusteringValueIds()),
        clusteredBaseSummary.getTimeColumnName(),
        column -> Projections.getClusterGroupSegmentInternalFileName(groupSpec.getClusteringValueIds(), column),
        columns
    );
  }

  /**
   * Determine the set of physical column names required from the chosen row selector given a {@link CursorBuildSpec}.
   */
  private static Set<String> requiredColumns(
      QueryableIndex rowSelector,
      @Nullable QueryableProjection<QueryableIndex> matched,
      CursorBuildSpec originalSpec
  )
  {
    final CursorBuildSpec effective = matched != null ? matched.getCursorBuildSpec() : originalSpec;
    if (effective.getPhysicalColumns() != null) {
      final Set<String> required = new LinkedHashSet<>(effective.getPhysicalColumns());
      // physicalColumns enumerates the selected columns, but QueryableIndexCursorHolder also reads __time while
      // building the cursor, independent of physicalColumns: unconditionally for a time-ordered index (its
      // interval-checking offset reads timestamps), and via a synthesized __time range filter for a non-time-ordered
      // index whose data extends past the query interval. That read happens after the cursor holder is handed back,
      // so __time must be pre-fetched on the async path or it becomes a lazy deep-storage download on a processing
      // thread. Predicting exactly when the holder reads __time would mean replicating its internals (fragile, and it
      // reads __time in the common cases anyway), so always include it: __time is cheap, and it resolves to a
      // no-download constant column for projections that don't carry a real time column.
      required.add(ColumnHolder.TIME_COLUMN_NAME);
      return required;
    }
    // Conservative fallback when physicalColumns isn't declared, fetch every column on the chosen row selector
    // plus __time (which is special-cased and not enumerated by getColumnNames()).
    final Set<String> all = new LinkedHashSet<>(rowSelector.getColumnNames());
    all.add(ColumnHolder.TIME_COLUMN_NAME);
    return all;
  }

  /**
   * The result of {@link #planCursorPrefetch}: the dispatch outcome (at most one of {@code matchedProjection} /
   * {@code clusterGroupPlan} is non-null; both null means the plain base table) plus one {@link PrefetchBundle} per
   * cache-layer bundle the cursor needs. A clustered plan whose filters rule out every group has no bundles.
   */
  record CursorPrefetchPlan(
      @Nullable QueryableProjection<QueryableIndex> matchedProjection,
      @Nullable ClusterGroupQueryPlan clusterGroupPlan,
      List<PrefetchBundle> bundles
  )
  {
    CursorPrefetchPlan
    {
      // enforce the at-most-one invariant: the factory dispatches on clusterGroupPlan first, so a plan carrying both
      // would silently drop the matched projection rather than failing
      if (matchedProjection != null && clusterGroupPlan != null) {
        throw DruidException.defensive("Cursor prefetch plan cannot match both a projection and a cluster-group plan");
      }
    }
  }

  /**
   * One bundle's worth of planned download work: the cache-layer {@code bundleName} to mount, the row selector whose
   * {@code getColumnHolder} materializes the columns, the columns to materialize once resident, and the planned
   * coalesced range reads that make them resident, each paired with the (main or external) mapper that executes it
   * and each executable concurrently via {@link PartialSegmentFileMapperV10.PlannedFetch#fetch()}.
   */
  record PrefetchBundle(
      String bundleName,
      QueryableIndex rowSelector,
      Set<String> requiredColumns,
      List<PartialSegmentFileMapperV10.PlannedFetch> fetches
  )
  {
  }
}
