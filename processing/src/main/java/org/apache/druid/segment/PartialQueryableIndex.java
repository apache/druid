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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileMapper;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.BaseTableProjectionSchema;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ConstantTimeColumn;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

  /**
   * Logical column name -> primary segment-internal (smoosh) file name for the base table namespace, including the
   * {@code __time} rename and the same column-descriptor filtering as {@link #buildColumnSuppliers}. Columns that
   * resolve to a constant/synthesized column with no downloadable file (e.g. a constant {@code __time}) are omitted.
   * Used by {@link PartialQueryableIndexCursorFactory} to coalesce the on-demand pre-fetch of a query's columns.
   */
  public Map<String, String> getBaseColumnFileNames()
  {
    return columnFileNames(
        baseProjectionMetadata.getSchema().getTimeColumnName(),
        baseProjectionMetadata.getSchema().getColumnNames(),
        column -> Projections.getProjectionSegmentInternalFileName(baseProjectionMetadata.getSchema(), column)
    );
  }

  /**
   * Logical column name -> primary smoosh file name for an aggregate projection's namespace. Empty for an unknown
   * projection. See {@link #getBaseColumnFileNames}.
   */
  public Map<String, String> getProjectionColumnFileNames(String projectionName)
  {
    final ProjectionMetadata spec = projectionSpecs.get(projectionName);
    if (spec == null) {
      return Map.of();
    }
    return columnFileNames(
        spec.getSchema().getTimeColumnName(),
        spec.getSchema().getColumnNames(),
        column -> Projections.getProjectionSegmentInternalFileName(spec.getSchema(), column)
    );
  }

  /**
   * Logical column name -> primary smoosh file name for a single cluster group's namespace. Empty for a non-clustered
   * segment. See {@link #getBaseColumnFileNames}.
   */
  public Map<String, String> getClusterGroupColumnFileNames(TableClusterGroupSpec group)
  {
    if (clusteredBaseSummary == null) {
      return Map.of();
    }
    return columnFileNames(
        clusteredBaseSummary.getTimeColumnName(),
        clusteredBaseSummary.getGroupColumnNames(),
        column -> Projections.getClusterGroupSegmentInternalFileName(group.getClusteringValueIds(), column)
    );
  }

  /**
   * Registered column name -> primary smoosh file name, built from {@link #resolveColumnFiles}. The key is the name the
   * column is exposed under (the {@code __time} rename applied), which is what query callers look it up by.
   */
  private Map<String, String> columnFileNames(
      @Nullable String timeColumnName,
      List<String> columnNames,
      Function<String, String> fileNameFn
  )
  {
    final Map<String, String> returnNames = new LinkedHashMap<>();
    for (ColumnFile cf : resolveColumnFiles(timeColumnName, columnNames, fileNameFn)) {
      returnNames.put(cf.registeredName(), cf.smooshName());
    }
    return returnNames;
  }

  /**
   * Resolve a namespace's logical columns to their primary segment-internal (smoosh) files: skip columns with no
   * {@link ColumnDescriptor}, and register the time column under {@code __time} when the schema names it differently.
   * This is the single source of truth shared by {@link #buildColumnSuppliers} (which builds one lazy supplier per
   * entry) and {@link #columnFileNames} (which the cursor factory uses to coalesce the on-demand pre-fetch), so the
   * mapping a pre-fetch plans against cannot drift from the file a later {@code getColumnHolder} maps.
   */
  private List<ColumnFile> resolveColumnFiles(
      @Nullable String timeColumnName,
      List<String> columnNames,
      Function<String, String> fileNameFn
  )
  {
    final boolean renameTime = !ColumnHolder.TIME_COLUMN_NAME.equals(timeColumnName);
    final List<ColumnFile> out = new ArrayList<>();
    for (String column : columnNames) {
      final String smooshName = fileNameFn.apply(column);
      if (!metadata.getColumnDescriptors().containsKey(smooshName)) {
        continue;
      }
      final String registeredName = (column.equals(timeColumnName) && renameTime)
                                     ? ColumnHolder.TIME_COLUMN_NAME
                                     : column;
      out.add(new ColumnFile(column, registeredName, smooshName));
    }
    return out;
  }

  /**
   * A resolved column: its logical {@code column} name, the {@code registeredName} it is exposed under (after the
   * {@code __time} rename), and its primary {@code smooshName} file.
   */
  private record ColumnFile(String column, String registeredName, String smooshName)
  {
  }

  /**
   * Plan coalesced range reads for a set of segment-internal (smoosh) file names; see
   * {@link PartialSegmentFileMapperV10#planDownloadRuns}.
   */
  public List<PartialSegmentFileMapperV10.DownloadRun> planDownloadRuns(Set<String> smooshNames)
  {
    return fileMapper.planDownloadRuns(smooshNames);
  }

  /**
   * Fetch one coalesced range read; see {@link PartialSegmentFileMapperV10#fetchRun}.
   */
  public void fetchDownloadRun(PartialSegmentFileMapperV10.DownloadRun run) throws IOException
  {
    fileMapper.fetchRun(run);
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
  public QueryableIndex getClusterGroupQueryableIndex(TableClusterGroupSpec groupSpec)
  {
    if (clusteredBaseSummary == null) {
      throw DruidException.defensive("getClusterGroupQueryableIndex called on a non-clustered segment");
    }
    final List<TableClusterGroupSpec> groups = clusteredBaseSummary.getClusterGroups();
    final int groupIndex = groups.indexOf(groupSpec);
    if (groupIndex < 0) {
      throw DruidException.defensive("Cluster group spec is not part of this segment");
    }
    final Map<String, Supplier<BaseColumnHolder>> groupColumns = clusterGroupColumnsByIndex.computeIfAbsent(
        groupIndex,
        i -> buildColumnSuppliers(
            clusteredBaseSummary.getTimeColumnName(),
            groupSpec.getNumRows(),
            clusteredBaseSummary.getGroupColumnNames(),
            column -> Projections.getClusterGroupSegmentInternalFileName(groupSpec.getClusteringValueIds(), column),
            Map.of()
        )
    );
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
    final Map<String, Supplier<BaseColumnHolder>> columns = new LinkedHashMap<>();

    for (ColumnFile cf : resolveColumnFiles(timeColumnName, columnNames, fileNameFn)) {
      final ColumnDescriptor columnDescriptor = metadata.getColumnDescriptors().get(cf.smooshName());
      final Supplier<BaseColumnHolder> columnSupplier = Suppliers.memoize(() -> {
        try {
          final ByteBuffer colBuffer = fileMapper.mapFile(cf.smooshName());
          final BaseColumnHolder parentColumn =
              parentColumns.containsKey(cf.column()) ? parentColumns.get(cf.column()).get() : null;
          return columnDescriptor.read(colBuffer, columnConfig, fileMapper, parentColumn);
        }
        catch (IOException e) {
          throw DruidException.defensive(e, "Failed to load column[%s]", cf.smooshName());
        }
      });

      // register under the exposed name (the __time rename already applied by resolveColumnFiles)
      columns.put(SmooshedFileMapper.STRING_INTERNER.intern(cf.registeredName()), columnSupplier);
    }

    if (timeColumnName == null) {
      columns.put(
          ColumnHolder.TIME_COLUMN_NAME,
          ConstantTimeColumn.makeConstantTimeSupplier(numRows, dataInterval.getStartMillis())
      );
    }

    return columns;
  }
}
