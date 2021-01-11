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
import com.google.common.collect.Maps;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.Indexed;
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
  private final Map<String, Supplier<ColumnHolder>> columns;
  private final SmooshedFileMapper fileMapper;
  @Nullable
  private final Metadata metadata;
  private final Supplier<Map<String, DimensionHandler>> dimensionHandlers;

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<ColumnHolder>> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata,
      boolean lazy
  )
  {
    Preconditions.checkNotNull(columns.get(ColumnHolder.TIME_COLUMN_NAME));
    this.dataInterval = Preconditions.checkNotNull(dataInterval, "dataInterval");
    ImmutableList.Builder<String> columnNamesBuilder = ImmutableList.builder();
    for (String column : columns.keySet()) {
      if (!ColumnHolder.TIME_COLUMN_NAME.equals(column)) {
        columnNamesBuilder.add(column);
      }
    }
    this.columnNames = columnNamesBuilder.build();
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;

    if (lazy) {
      this.dimensionHandlers = Suppliers.memoize(() -> {
            Map<String, DimensionHandler> dimensionHandlerMap = Maps.newLinkedHashMap();
            for (String dim : availableDimensions) {
              ColumnCapabilities capabilities = getColumnHolder(dim).getCapabilities();
              DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
              dimensionHandlerMap.put(dim, handler);
            }
            return dimensionHandlerMap;
          }
      );
    } else {
      Map<String, DimensionHandler> dimensionHandlerMap = Maps.newLinkedHashMap();
      for (String dim : availableDimensions) {
        ColumnCapabilities capabilities = getColumnHolder(dim).getCapabilities();
        DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dim, capabilities, null);
        dimensionHandlerMap.put(dim, handler);
      }
      this.dimensionHandlers = () -> dimensionHandlerMap;
    }
  }

  @VisibleForTesting
  public SimpleQueryableIndex(
      Interval interval,
      List<String> columnNames,
      Indexed<String> availableDimensions,
      BitmapFactory bitmapFactory,
      Map<String, Supplier<ColumnHolder>> columns,
      SmooshedFileMapper fileMapper,
      @Nullable Metadata metadata,
      Supplier<Map<String, DimensionHandler>> dimensionHandlers
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
    return columns.get(ColumnHolder.TIME_COLUMN_NAME).get().getLength();
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
    return dimensionHandlers.get();
  }

}
