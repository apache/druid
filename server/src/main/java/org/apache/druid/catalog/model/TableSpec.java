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
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Definition of a table "hint" in the metastore, between client and
 * Druid, and between Druid nodes.
 * <p>
 * This class is a simple holder of values. Semantics on top of the values
 * are provided separately: by the catalog object model for writes, and
 * by various Java classes for reads.
 */
public class TableSpec
{
  private final String type;
  private final Map<String, Object> properties;
  private final List<ColumnSpec> columns;

  @JsonCreator
  public TableSpec(
      @JsonProperty("type") final String type,
      @JsonProperty("properties") @Nullable final Map<String, Object> properties,
      @JsonProperty("columns") @Nullable final List<ColumnSpec> columns
  )
  {
    this.type = type;
    this.properties = properties == null ? Collections.emptyMap() : properties;
    this.columns = columns == null ? Collections.emptyList() : columns;

    // Note: no validation here. If a bad definition got into the
    // DB, don't prevent deserialization.
  }

  public TableSpec withProperties(final Map<String, Object> properties)
  {
    return new TableSpec(type, properties, columns);
  }

  public TableSpec withColumns(final List<ColumnSpec> columns)
  {
    return new TableSpec(type, properties, columns);
  }

  @JsonProperty("type")
  public String type()
  {
    return type;
  }

  @JsonProperty("properties")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> properties()
  {
    return properties;
  }

  @JsonProperty("columns")
  @JsonInclude(Include.NON_NULL)
  public List<ColumnSpec> columns()
  {
    return columns;
  }

  /**
   * Validate the final spec. Updates use this same class, but allow
   * the spec to be partial (and thus inconsistent). Validation should
   * be done on the merged result, not on the updates themselves.
   */
  public void validate()
  {
    if (Strings.isNullOrEmpty(type)) {
      throw new IAE("Table type is required");
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
    TableSpec other = (TableSpec) o;
    return Objects.equals(this.type, other.type)
        && Objects.equals(this.columns, other.columns)
        && Objects.equals(this.properties, other.properties);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        type,
        columns,
        properties
    );
  }
}
