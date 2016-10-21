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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.metamx.collections.bitmap.BitmapFactory;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.Indexed;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;

/**
 */
public class SimpleQueryableIndex implements QueryableIndex
{
  private final Interval dataInterval;
  private final Indexed<String> columnNames;
  private final Indexed<String> availableDimensions;
  private final BitmapFactory bitmapFactory;
  private final Map<String, Column> columns;
  private final SmooshedFileMapper fileMapper;
  private final Metadata metadata;
  private final Map<String, DimensionHandler> dimensionHandlers;

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> columnNames,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Column> columns,
      SmooshedFileMapper fileMapper,
      Metadata metadata
  )
  {
    Preconditions.checkNotNull(columns.get(Column.TIME_COLUMN_NAME));
    this.dataInterval = dataInterval;
    this.columnNames = columnNames;
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
    this.metadata = metadata;
    this.dimensionHandlers = Maps.newLinkedHashMap();
    initDimensionHandlers();
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
  public Indexed<String> getColumnNames()
  {
    return columnNames;
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
  public Column getColumn(String columnName)
  {
    return columns.get(columnName);
  }

  @Override
  public void close() throws IOException
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
      DimensionHandler handler = DimensionHandlerUtil.getHandlerFromCapabilities(dim, capabilities);
      dimensionHandlers.put(dim, handler);
    }
  }
}
