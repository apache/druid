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

package org.apache.druid.catalog;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;

import java.util.Map;

/**
 * Base class for table columns. Columns have multiple types
 * represented as subclasses.
 */
@PublicApi
public abstract class ColumnSpec
{
  enum ColumnKind
  {
    DETAIL,
    DIMENSION,
    MEASURE,
    INPUT
  }

  public static final Map<String, ColumnType> VALID_SQL_TYPES =
      new ImmutableMap.Builder<String, ColumnType>()
        .put("BIGINT", ColumnType.LONG)
        .put("FLOAT", ColumnType.FLOAT)
        .put("DOUBLE", ColumnType.DOUBLE)
        .put("VARCHAR", ColumnType.STRING)
        .build();

  protected final String name;
  protected final String sqlType;

  public ColumnSpec(
      String name,
      String sqlType
  )
  {
    this.name = name;
    this.sqlType = sqlType;
  }

  protected abstract ColumnKind kind();

  @JsonProperty("name")
  public String name()
  {
    return name;
  }

  @JsonProperty("sqlType")
  public String sqlType()
  {
    return sqlType;
  }

  public void validate()
  {
    if (Strings.isNullOrEmpty(name)) {
      throw new IAE("Column name is required");
    }
  }

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return CatalogSpecs.toBytes(jsonMapper, this);
  }

  public static ColumnSpec fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return CatalogSpecs.fromBytes(jsonMapper, bytes, ColumnSpec.class);
  }

  @Override
  public String toString()
  {
    return CatalogSpecs.toString(this);
  }
}
