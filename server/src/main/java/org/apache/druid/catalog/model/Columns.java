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

package org.apache.druid.catalog.model;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.List;
import java.util.Map;

public class Columns
{
  public static final String TIME_COLUMN = "__time";

  public static final String STRING = ValueType.STRING.name();
  public static final String LONG = ValueType.LONG.name();
  public static final String FLOAT = ValueType.FLOAT.name();
  public static final String DOUBLE = ValueType.DOUBLE.name();

  public static final String SQL_VARCHAR = "VARCHAR";
  public static final String SQL_BIGINT = "BIGINT";
  public static final String SQL_FLOAT = "FLOAT";
  public static final String SQL_DOUBLE = "DOUBLE";
  public static final String SQL_VARCHAR_ARRAY = "VARCHAR ARRAY";
  public static final String SQL_BIGINT_ARRAY = "BIGINT ARRAY";
  public static final String SQL_FLOAT_ARRAY = "FLOAT ARRAY";
  public static final String SQL_DOUBLE_ARRAY = "DOUBLE ARRAY";
  public static final String SQL_TIMESTAMP = "TIMESTAMP";

  public static final Map<String, ColumnType> SQL_TO_DRUID_TYPES =
      new ImmutableMap.Builder<String, ColumnType>()
        .put(SQL_TIMESTAMP, ColumnType.LONG)
        .put(SQL_BIGINT, ColumnType.LONG)
        .put(SQL_FLOAT, ColumnType.FLOAT)
        .put(SQL_DOUBLE, ColumnType.DOUBLE)
        .put(SQL_VARCHAR, ColumnType.STRING)
        .put(SQL_VARCHAR_ARRAY, ColumnType.STRING_ARRAY)
        .put(SQL_BIGINT_ARRAY, ColumnType.LONG_ARRAY)
        .put(SQL_FLOAT_ARRAY, ColumnType.FLOAT_ARRAY)
        .put(SQL_DOUBLE_ARRAY, ColumnType.DOUBLE_ARRAY)
        .build();

  public static final Map<ColumnType, String> DRUID_TO_SQL_TYPES =
      new ImmutableMap.Builder<ColumnType, String>()
      .put(ColumnType.LONG, SQL_BIGINT)
      .put(ColumnType.FLOAT, FLOAT)
      .put(ColumnType.DOUBLE, DOUBLE)
      .put(ColumnType.STRING, SQL_VARCHAR)
      .put(ColumnType.STRING_ARRAY, SQL_VARCHAR_ARRAY)
      .put(ColumnType.LONG_ARRAY, SQL_BIGINT_ARRAY)
      .put(ColumnType.FLOAT_ARRAY, SQL_FLOAT_ARRAY)
      .put(ColumnType.DOUBLE_ARRAY, SQL_DOUBLE_ARRAY)
      .build();

  private Columns()
  {
  }

  public static ColumnType druidType(ColumnSpec spec)
  {
    if (isTimeColumn(spec.name())) {
      return ColumnType.LONG;
    }
    String dataType = spec.dataType();
    if (dataType == null) {
      return null;
    }
    ColumnType druidType = SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(dataType));
    if (druidType != null) {
      return druidType;
    }
    return ColumnType.fromString(dataType);
  }

  public static String sqlType(ColumnSpec spec)
  {
    if (isTimeColumn(spec.name())) {
      return SQL_TIMESTAMP;
    }
    ColumnType druidType = druidType(spec);
    if (druidType == null) {
      return null;
    }
    String sqlType = DRUID_TO_SQL_TYPES.get(druidType);
    return sqlType == null ? druidType.asTypeString() : sqlType;
  }

  public static boolean isTimeColumn(String name)
  {
    return TIME_COLUMN.equals(name);
  }

  public static RowSignature convertSignature(List<ColumnSpec> columns)
  {
    RowSignature.Builder builder = RowSignature.builder();
    for (ColumnSpec col : columns) {
      ColumnType druidType = druidType(col);
      if (druidType == null) {
        druidType = ColumnType.STRING;
      }
      builder.add(col.name(), druidType);
    }
    return builder.build();
  }

  public static String sqlType(ColumnType druidType)
  {
    return DRUID_TO_SQL_TYPES.get(druidType);
  }
}
