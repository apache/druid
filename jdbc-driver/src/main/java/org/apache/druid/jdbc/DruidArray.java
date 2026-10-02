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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Our implementation of JDBC {@link Array}.
 */
public class DruidArray implements Array
{
  private final int baseType;
  private final String baseTypeName;
  private final Object[] arr;

  public DruidArray(final int baseType, final String baseTypeName, final Object[] arr)
  {
    this.baseType = baseType;
    this.baseTypeName = baseTypeName;
    this.arr = arr;
  }

  @Override
  public String getBaseTypeName()
  {
    return baseTypeName;
  }

  @Override
  public int getBaseType()
  {
    return baseType;
  }

  @Override
  public Object getArray()
  {
    return arr;
  }

  @Override
  public Object getArray(final Map<String, Class<?>> map) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getArray with a type map is not supported for DruidArray");
  }

  @Override
  public Object getArray(final long index, final int count)
  {
    final int startIndex = (int) index - 1;
    final Object[] slice = new Object[count];
    System.arraycopy(arr, startIndex, slice, 0, count);
    return slice;
  }

  @Override
  public Object getArray(final long index, final int count, final Map<String, Class<?>> map) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getArray with a type map is not supported for DruidArray");
  }

  @Override
  public ResultSet getResultSet() throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getResultSet not supported for DruidArray");
  }

  @Override
  public ResultSet getResultSet(final Map<String, Class<?>> map) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getResultSet not supported for DruidArray");
  }

  @Override
  public ResultSet getResultSet(final long index, final int count) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getResultSet not supported for DruidArray");
  }

  @Override
  public ResultSet getResultSet(final long index, final int count, final Map<String, Class<?>> map) throws SQLException
  {
    throw new DruidJdbcFeatureNotSupportedException("getResultSet not supported for DruidArray");
  }

  @Override
  public void free()
  {
    // Nothing to free.
  }
}
