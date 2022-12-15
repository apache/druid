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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Specification of table columns. Columns have multiple types
 * represented via the type field.
 */
@UnstableApi
public class ColumnSpec
{
  /**
   * The type of column spec (not the column data type). For example, in a
   * cube, dimensions and measures have distinct sets of properties.
   */
  private final String type;

  /**
   * The name of the column as known to the SQL layer. At present, there is no
   * support for column aliases, so this is also the column name as physically
   * stored in segments.
   */
  private final String name;

  /**
   * The data type of the column expressed as a supported SQL type. The data type here must
   * directly match a Druid storage type. So, {@code BIGINT} for {code long}, say.
   * This usage does not support Druid's usual "fudging": one cannot use {@code INTEGER}
   * to mean {@code long}. The type will likely encode complex and aggregation types
   * in the future, though that is not yet supported. The set of valid mappings is
   * defined in the {@link Columns} class.
   */
  private final String sqlType;

  /**
   * Properties for the column. At present, these are all user and application defined.
   * For example, a UI layer might want to store a display format. Druid may define
   * properties in the future. Candidates would be indexing options if/when there are
   * choices available per-column.
   */
  private final Map<String, Object> properties;

  @JsonCreator
  public ColumnSpec(
      @JsonProperty("type")final String type,
      @JsonProperty("name")final String name,
      @JsonProperty("sqlType") @Nullable final String sqlType,
      @JsonProperty("properties") @Nullable final Map<String, Object> properties
  )
  {
    this.type = type;
    this.name = name;
    this.sqlType = sqlType;
    this.properties = properties == null ? Collections.emptyMap() : properties;
  }

  @JsonProperty("type")
  public String type()
  {
    return type;
  }

  @JsonProperty("name")
  public String name()
  {
    return name;
  }

  @JsonProperty("sqlType")
  @JsonInclude(Include.NON_NULL)
  public String sqlType()
  {
    return sqlType;
  }

  @JsonProperty("properties")
  @JsonInclude(Include.NON_EMPTY)
  public Map<String, Object> properties()
  {
    return properties;
  }

  public void validate()
  {
    if (Strings.isNullOrEmpty(type)) {
      throw new IAE("Column type is required");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IAE("Column name is required");
    }
  }

  @Override
  public String toString()
  {
    return CatalogUtils.toString(this);
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
    ColumnSpec other = (ColumnSpec) o;
    return Objects.equals(this.type, other.type)
        && Objects.equals(this.name, other.name)
        && Objects.equals(this.sqlType, other.sqlType)
        && Objects.equals(this.properties, other.properties);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        type,
        name,
        sqlType,
        properties
    );
  }
}
