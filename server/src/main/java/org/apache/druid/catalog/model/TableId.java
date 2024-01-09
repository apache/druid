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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import org.apache.druid.java.util.common.StringUtils;

/**
 * SQL-like compound table ID with schema and table name.
 */
public class TableId
{
  // Well-known Druid schemas
  public static final String DRUID_SCHEMA = "druid";
  public static final String LOOKUP_SCHEMA = "lookups";
  public static final String SYSTEM_SCHEMA = "sys";
  public static final String CATALOG_SCHEMA = "INFORMATION_SCHEMA";

  // Extra for MSQE
  public static final String EXTERNAL_SCHEMA = "ext";

  // Extra for views
  public static final String VIEW_SCHEMA = "view";

  private final String schema;
  private final String name;

  @JsonCreator
  public TableId(
      @JsonProperty("schema") String schema,
      @JsonProperty("name") String name)
  {
    this.schema = schema;
    this.name = name;
  }

  public static TableId datasource(String name)
  {
    return new TableId(DRUID_SCHEMA, name);
  }

  public static TableId external(String name)
  {
    return new TableId(EXTERNAL_SCHEMA, name);
  }

  public static TableId of(String schema, String table)
  {
    return new TableId(schema, table);
  }

  @JsonProperty("schema")
  public String schema()
  {
    return schema;
  }

  @JsonProperty("name")
  public String name()
  {
    return name;
  }

  public String sqlName()
  {
    return StringUtils.format("\"%s\".\"%s\"", schema, name);
  }

  public String unquoted()
  {
    return StringUtils.format("%s.%s", schema, name);
  }

  @Override
  public String toString()
  {
    return sqlName();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    TableId other = (TableId) o;
    return Objects.equal(this.schema, other.schema)
        && Objects.equal(this.name, other.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(schema, name);
  }
}
