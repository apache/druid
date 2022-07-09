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

package org.apache.druid.frame.write;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

public class UnsupportedColumnTypeException extends RuntimeException
{
  private final String columnName;
  @Nullable
  private final ColumnType columnType;

  public UnsupportedColumnTypeException(final String columnName, @Nullable final ColumnType columnType)
  {
    super(message(columnName, columnType));
    this.columnName = columnName;
    this.columnType = columnType;
  }

  public static String message(final String columnName, @Nullable final ColumnType columnType)
  {
    return columnType == null
           ? StringUtils.format("Cannot handle column [%s] with unknown type", columnName)
           : StringUtils.format("Cannot handle column [%s] with type [%s]", columnName, columnType);
  }

  public String getColumnName()
  {
    return columnName;
  }

  @Nullable
  public ColumnType getColumnType()
  {
    return columnType;
  }
}
