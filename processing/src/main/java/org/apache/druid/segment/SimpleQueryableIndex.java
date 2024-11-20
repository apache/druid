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
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.QueryableProjection;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collections;
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
  private final Map<String, Supplier<ColumnHolder>> columns;
  private final List<OrderBy> ordering;
  private final Map<String, AggregateProjectionMetadata> projectionsMap;
  private final SortedSet<AggregateProjectionMetadata> projections;
  private final Map<String, Map<String, Supplier<ColumnHolder>>> projectionColumns;
  private final SmooshedFileMapper fileMapper;
  private final Supplier<Map<String, DimensionHandler>> dimensionHandlers;

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<ColumnHolder>> columns,
      SmooshedFileMapper fileMapper,
      boolean lazy
  )
  {
    this(dataInterval, dimNames, bitmapFactory, columns, fileMapper, lazy, null, null);
  }

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<ColumnHolder>> columns,
      SmooshedFileMapper fileMapper,
      boolean lazy,
      @Nullable Metadata metadata,
      @Nullable Map<String, Map<String, Supplier<ColumnHolder>>> projectionColumns
  )
  {
    Preconditions.checkNotNull(columns.get(ColumnHolder.TIME_COLUMN_NAME));
    this.dataInterval = Preconditions.checkNotNull(dataInterval, "dataInterval");
    ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
    LinkedHashSet<String> dimsFirst = new LinkedHashSet<>();
    for (String dimName : dimNames) {
      dimsFirst.add(dimName);
    }
    for (String columnName : columns.keySet()) {
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(columnName)) {
        dimsFirst.add(columnName);
      }
    }
    columnNamesBuilder.addAll(dimsFirst);
    this.columnNames = columnNamesBuilder.build();
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;

    this.projectionColumns = projectionColumns == null ? Collections.emptyMap() : projectionColumns;

    if (lazy) {
      this.dimensionHandlers = Suppliers.memoize(() -> initDimensionHandlers(availableDimensions));
    } else {
      this.dimensionHandlers = () -> initDimensionHandlers(availableDimensions);
    }
    if (metadata != null) {
      if (metadata.getOrdering() != null) {
        this.ordering = ORDERING_INTERNER.intern(metadata.getOrdering());
      } else {
        this.ordering = Cursors.ascendingTimeOrder();
      }
      if (metadata.getProjections() != null) {
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
      // When sort order isn't set in metadata.drd, assume the segment is sorted by __time.
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
    return columns.get(ColumnHolder.TIME_COLUMN_NAME).get().getLength();
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
  public ColumnHolder getColumnHolder(String columnName)
  {
    Supplier<ColumnHolder> columnHolderSupplier = columns.get(columnName);
    return columnHolderSupplier == null ? null : columnHolderSupplier.get();
  }

  @VisibleForTesting
  public Map<String, Supplier<ColumnHolder>> getColumns()
  {
    return columns;
  }

  @VisibleForTesting
  public SmooshedFileMapper getFileMapper()
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
        (projectionName, columnName) ->
            projectionColumns.get(projectionName).containsKey(columnName) || getColumnCapabilities(columnName) == null,
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
        true,
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
}

