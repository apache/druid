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

package org.apache.druid.frame.read.columnar;

import org.apache.druid.frame.Frame;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * Dummy reader for unsupported types. Throws {@link UnsupportedColumnTypeException} if we try to call any method of the
 * frame reader
 */
public class UnsupportedColumnTypeFrameColumnReader implements FrameColumnReader
{

  private final String columnName;
  @Nullable
  private final ColumnType columnType;

  UnsupportedColumnTypeFrameColumnReader(String columnName, @Nullable ColumnType columnType)
  {
    this.columnName = columnName;
    this.columnType = columnType;
  }

  @Override
  public Column readRACColumn(Frame frame)
  {
    throw new UnsupportedColumnTypeException(columnName, columnType);
  }

  @Override
  public ColumnPlus readColumn(Frame frame)
  {
    throw new UnsupportedColumnTypeException(columnName, columnType);
  }
}
