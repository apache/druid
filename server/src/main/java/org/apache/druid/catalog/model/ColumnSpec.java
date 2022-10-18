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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;

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
  private final String type;
  private final String name;
  private final String sqlType;
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

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return JacksonUtils.toBytes(jsonMapper, this);
  }

  public static ColumnSpec fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return JacksonUtils.fromBytes(jsonMapper, bytes, ColumnSpec.class);
  }

  @Override
  public String toString()
  {
    return JacksonUtils.toString(this);
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
