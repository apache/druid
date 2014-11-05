/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment;

import com.google.common.base.Preconditions;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.column.Column;
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

  public SimpleQueryableIndex(
      Interval dataInterval,
      Indexed<String> columnNames,
      Indexed<String> dimNames,
      BitmapFactory bitmapFactory,
      Map<String, Column> columns,
      SmooshedFileMapper fileMapper
  )
  {
    Preconditions.checkNotNull(columns.get(Column.TIME_COLUMN_NAME));
    this.dataInterval = dataInterval;
    this.columnNames = columnNames;
    this.availableDimensions = dimNames;
    this.bitmapFactory = bitmapFactory;
    this.columns = columns;
    this.fileMapper = fileMapper;
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
}
