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
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileMapper;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.BaseTableProjectionSchema;
import org.apache.druid.segment.projections.ConstantTimeColumn;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
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
    this.baseNumRows = baseProjection.getNumRows();
    this.baseProjectionPrefix = Projections.getProjectionSegmentInternalFilePrefix(baseSchema);
    this.dataInterval = Intervals.of(metadata.getInterval());
    this.bitmapFactory = metadata.getBitmapEncoding().getBitmapFactory();
    this.availableDimensions = new ListIndexed<>(baseSchema.getDimensionNames());

    // build column names (dimensions first, then other columns, excluding __time)
    final LinkedHashSet<String> dimsFirst = new LinkedHashSet<>(baseSchema.getDimensionNames());
    for (String columnName : baseSchema.getColumnNames()) {
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
        dimsFirst.add(columnName);
      }
    }
    this.columnNames = List.copyOf(dimsFirst);

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
    // deserialization until the column is accessed.
    this.baseColumns = buildProjectionColumnSuppliers(baseProjection, Map.of());

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
    // look up the column in the base table projection's namespace
    final String smooshName = baseProjectionPrefix + column;
    final ColumnDescriptor descriptor = metadata.getColumnDescriptors().get(smooshName);
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
        (projectionName, columnName) -> {
          // check if the projection has this column using metadata column descriptors
          final ProjectionMetadata projSpec = projectionSpecs.get(projectionName);
          if (projSpec == null) {
            return false;
          }
          final String smooshName = Projections.getProjectionSegmentInternalFileName(projSpec.getSchema(), columnName);
          return metadata.getColumnDescriptors().containsKey(smooshName)
                 || getColumnCapabilities(columnName) == null;
        },
        this::getProjectionQueryableIndex
    );
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
    final String timeColumnName = projectionSpec.getSchema().getTimeColumnName();
    final boolean renameTime = !ColumnHolder.TIME_COLUMN_NAME.equals(timeColumnName);
    final Map<String, Supplier<BaseColumnHolder>> projectionColumns = new LinkedHashMap<>();

    for (String column : projectionSpec.getSchema().getColumnNames()) {
      final String smooshName = Projections.getProjectionSegmentInternalFileName(projectionSpec.getSchema(), column);
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

      projectionColumns.put(internedColumnName, columnSupplier);

      if (column.equals(timeColumnName) && renameTime) {
        projectionColumns.put(ColumnHolder.TIME_COLUMN_NAME, projectionColumns.get(column));
        projectionColumns.remove(column);
      }
    }

    if (timeColumnName == null) {
      projectionColumns.put(
          ColumnHolder.TIME_COLUMN_NAME,
          ConstantTimeColumn.makeConstantTimeSupplier(projectionSpec.getNumRows(), dataInterval.getStartMillis())
      );
    }

    return projectionColumns;
  }
}
