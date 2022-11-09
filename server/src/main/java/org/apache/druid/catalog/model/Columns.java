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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Columns
{
  public static final String TIME_COLUMN = "__time";

  public static final String VARCHAR = "VARCHAR";
  public static final String BIGINT = "BIGINT";
  public static final String FLOAT = "FLOAT";
  public static final String DOUBLE = "DOUBLE";
  public static final String TIMESTAMP = "TIMESTAMP";

  public static final Set<String> NUMERIC_TYPES =
      ImmutableSet.of(BIGINT, FLOAT, DOUBLE);
  public static final Set<String> SCALAR_TYPES =
      ImmutableSet.of(TIMESTAMP, VARCHAR, BIGINT, FLOAT, DOUBLE);

  public static final Map<String, ColumnType> SQL_TO_DRUID_TYPES =
      new ImmutableMap.Builder<String, ColumnType>()
        .put(TIMESTAMP, ColumnType.LONG)
        .put(BIGINT, ColumnType.LONG)
        .put(FLOAT, ColumnType.FLOAT)
        .put(DOUBLE, ColumnType.DOUBLE)
        .put(VARCHAR, ColumnType.STRING)
        .build();

  private Columns()
  {
  }

  public static boolean isTimestamp(String type)
  {
    return TIMESTAMP.equalsIgnoreCase(type.trim());
  }

  public static boolean isScalar(String type)
  {
    return SCALAR_TYPES.contains(StringUtils.toUpperCase(type.trim()));
  }

  public static ColumnType druidType(String sqlType)
  {
    if (sqlType == null) {
      return null;
    }
    return SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(sqlType));
  }

  public static void validateScalarColumn(String name, String type)
  {
    if (type == null) {
      return;
    }
    if (!Columns.isScalar(type)) {
      throw new IAE("Not a supported SQL type: " + type);
    }
  }

  public static boolean isTimeColumn(String name)
  {
    return TIME_COLUMN.equals(name);
  }

  public static RowSignature convertSignature(TableSpec spec)
  {
    List<ColumnSpec> columns = spec.columns();
    RowSignature.Builder builder = RowSignature.builder();
    for (ColumnSpec col : columns) {
      ColumnType druidType = Columns.SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(col.sqlType()));
      if (druidType == null) {
        druidType = ColumnType.STRING;
      }
      builder.add(col.name(), druidType);
    }
    return builder.build();
  }
}
