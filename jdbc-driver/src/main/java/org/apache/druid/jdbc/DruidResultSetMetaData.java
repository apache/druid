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

package org.apache.druid.jdbc;

import org.apache.druid.jdbc.http.ColumnMetadata;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

/**
 * Our implementation of JDBC {@link ResultSetMetaData}.
 */
public class DruidResultSetMetaData implements ResultSetMetaData
{
  private final List<ColumnMetadata> columns;

  public DruidResultSetMetaData(final List<ColumnMetadata> columns)
  {
    this.columns = columns;
  }

  @Override
  public int getColumnCount()
  {
    return columns.size();
  }

  @Override
  public boolean isAutoIncrement(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return false;
  }

  @Override
  public boolean isCaseSensitive(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return getSqlType(column) == Types.VARCHAR || getSqlType(column) == Types.CHAR;
  }

  @Override
  public boolean isSearchable(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return true;
  }

  @Override
  public boolean isCurrency(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return false;
  }

  @Override
  public int isNullable(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // We can't tell from ColumnMetadata if a result column is nullable, so report all as nullable to be safe.
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isSigned(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // All numeric types are signed.
    final int sqlType = getSqlType(column);
    return sqlType == Types.TINYINT || sqlType == Types.SMALLINT ||
           sqlType == Types.INTEGER || sqlType == Types.BIGINT ||
           sqlType == Types.REAL || sqlType == Types.FLOAT ||
           sqlType == Types.DOUBLE || sqlType == Types.DECIMAL ||
           sqlType == Types.NUMERIC;
  }

  @Override
  public int getColumnDisplaySize(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // Some reasonable guesses at display size.
    return switch (getSqlType(column)) {
      case Types.TINYINT -> 4;
      case Types.SMALLINT -> 6;
      case Types.INTEGER -> 11;
      case Types.BIGINT -> 20;
      case Types.REAL, Types.FLOAT -> 15;
      case Types.DOUBLE -> 25;
      case Types.TIMESTAMP -> 29;
      case Types.DATE -> 10;
      case Types.TIME -> 8;
      default -> 512;
    };
  }

  @Override
  public String getColumnLabel(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return columns.get(column - 1).name();
  }

  @Override
  public String getColumnName(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return columns.get(column - 1).name();
  }

  @Override
  public String getSchemaName(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // Druid SQL API does not return schema name.
    return "";
  }

  @Override
  public int getPrecision(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // Druid SQL API does not return precision information.
    return 0;
  }

  @Override
  public int getScale(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // Druid SQL API does not return scale information.
    return 0;
  }

  @Override
  public String getTableName(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // Druid SQL API does not return table name.
    return "";
  }

  @Override
  public String getCatalogName(final int column) throws SQLException
  {
    validateColumnIndex(column);

    // Druid SQL API does not return catalog name.
    return "";
  }

  @Override
  public int getColumnType(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return columns.get(column - 1).jdbcType();
  }

  @Override
  public String getColumnTypeName(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return columns.get(column - 1).type().getName();
  }

  @Override
  public boolean isReadOnly(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return true;
  }

  @Override
  public boolean isWritable(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return false;
  }

  @Override
  public String getColumnClassName(final int column) throws SQLException
  {
    validateColumnIndex(column);

    return switch (getSqlType(column)) {
      case Types.BIT, Types.TINYINT, Types.SMALLINT, Types.INTEGER -> Integer.class.getName();
      case Types.DECIMAL, Types.NUMERIC -> BigDecimal.class.getName();
      case Types.BIGINT -> Long.class.getName();
      case Types.REAL -> Float.class.getName();
      case Types.FLOAT, Types.DOUBLE -> Double.class.getName();
      case Types.BOOLEAN -> Boolean.class.getName();
      case Types.TIMESTAMP -> Timestamp.class.getName();
      case Types.DATE -> Date.class.getName();
      case Types.TIME -> Time.class.getName();
      case Types.VARCHAR, Types.CHAR -> String.class.getName();
      case Types.ARRAY -> Array.class.getName();
      default -> Object.class.getName();
    };
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException
  {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new DruidJdbcException("Cannot unwrap to class[%s]", iface.getName());
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface)
  {
    return iface.isAssignableFrom(getClass());
  }

  private void validateColumnIndex(final int columnIndex) throws SQLException
  {
    if (columnIndex < 1 || columnIndex > columns.size()) {
      throw new DruidJdbcException("Invalid column index[%s]. Valid range[1-%s]", columnIndex, columns.size());
    }
  }

  private int getSqlType(final int column) throws SQLException
  {
    validateColumnIndex(column);
    return columns.get(column - 1).jdbcType();
  }
}
