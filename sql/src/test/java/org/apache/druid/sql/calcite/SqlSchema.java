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

package org.apache.druid.sql.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.druid.java.util.common.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Simple representation of an SQL schema used to validate the row type of
 * a SQL query against the SQL types. See {@code RowSignature} when the goal
 * is to validate the Druid native signature.
 */
public class SqlSchema
{
  public static class ColumnSignature
  {
    private final String name;
    private final String type;

    public ColumnSignature(final String name, final String type)
    {
      this.name = name;
      this.type = type;
    }

    public String name()
    {
      return name;
    }

    public String type()
    {
      return type;
    }

    @Override
    public String toString()
    {
      return StringUtils.format("%s %s", name, type);
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || o.getClass() != getClass()) {
        return false;
      }
      ColumnSignature other = (ColumnSignature) o;
      return Objects.equals(name, other.name)
          && Objects.equals(type, other.type);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, type);
    }
  }

  public static class Builder
  {
    private final List<ColumnSignature> columns = new ArrayList<>();

    public Builder column(String name, String type)
    {
      columns.add(new ColumnSignature(name, type));
      return this;
    }

    public SqlSchema build()
    {
      return new SqlSchema(columns);
    }
  }

  private final List<ColumnSignature> columns;

  public SqlSchema(final List<ColumnSignature> columns)
  {
    this.columns = columns;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static SqlSchema of(RelDataType rowType)
  {
    final Builder builder = new Builder();
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (RelDataTypeField field : fields) {
      builder.column(field.getName(), field.getType().getFullTypeString());
    }
    return builder.build();
  }

  @Override
  public String toString()
  {
    return "(" +
        columns.stream().map(
            c -> c.toString()).collect(Collectors.joining(", ")) +
        ")";
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    final SqlSchema other = (SqlSchema) o;
    return Objects.equals(columns, other.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(columns);
  }
}
