/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 */
public class SimpleQueryableIndex extends AbstractIndex implements QueryableIndex
{
  private final Interval dataInterval;
  private final List<String> columnNames;
  private final Indexed<String> availableDimensions;
  private final BitmapFactory bitmapFactory;
  private final Map<String, Column> columns;
  private final SmooshedFileMapper fileMapper;
  @Nullable
  private final Metadata metadata;
  private final Map<String, DimensionHandler> dimensionHandlers;

  public SimpleQueryableIndex(
      Interval dataInterval, Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Column> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata
  )
  {
    Preconditions.checkNotNull(columns.get(Column.TIME_COLUMN_NAME));
    this.dataInterval = Preconditions.checkNotNull(dataInterval, "dataInterval");
    ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
    for (String column : columns.keySet()) {
      if (!Column.TIME_COLUMN_NAME.equals(column)) {
        columnNamesBuilder.add(column);
      }
    }
    this.columnNames = columnNamesBuilder.build();
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;
    this.dimensionHandlers = Maps.newLinkedHashMap();
    initDimensionHandlers();
  }

  @VisibleForTesting
  public SimpleQueryableIndex(
      Interval interval,
      List<String> columnNames,
      Indexed<String> availableDimensions,
      BitmapFactory bitmapFactory,
      Map<String, Column> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata,
      Map<String, DimensionHandler> dimensionHandlers
  )
  {
    this.dataInterval = interval;
    this.columnNames = columnNames;
    this.availableDimensions = availableDimensions;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;
    this.dimensionHandlers = dimensionHandlers;
  }

  @Override
  public Interval getDataInterval()
  {
    return dataInterval;
  }

  @Override
  public int getNumRows()
  {
    return columns.get(Column.TIME_COLUMN_NAME).getLength();
  }

  @Override
  public List<String> getColumnNames()
  {
    return columnNames;
  }

  @Override
  public StorageAdapter toStorageAdapter()
  {
    return new QueryableIndexStorageAdapter(this);
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

  @Nullable
  @Override
  public Column getColumn(String columnName)
  {
    return columns.get(columnName);
  }

  @VisibleForTesting
  public Map<String, Column> getColumns()
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
    fileMapper.close();
  }

  @Override
  public Metadata getMetadata()
  {
    return metadata;
  }

  @Override
  public Map<String, DimensionHandler> getDimensionHandlers()
  {
    return dimensionHandlers;
  }

  private void initDimensionHandlers()
  {
    for (String dim : availableDimensions) {
      ColumnCapabilities capabilities = getColumn(dim).getCapabilities();
      DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
      dimensionHandlers.put(dim, handler);
    }
  }
}
