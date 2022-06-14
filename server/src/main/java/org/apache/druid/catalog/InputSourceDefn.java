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
import com.google.common.base.Strings;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.IAE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Definition of an external input source, primarily for ingestion.
 * The components are derived from those for Druid ingestion: an
 * input source, a format and a set of columns. Also provides
 * properties, as do all table definitions.
 */
public class InputSourceDefn extends TableDefn
{
  private final InputSource inputSource;
  private final InputFormat format;
  private final List<InputColumnDefn> columns;

  public InputSourceDefn(
      @JsonProperty("inputSource") InputSource inputSource,
      @JsonProperty("format") InputFormat format,
      @JsonProperty("columns") List<InputColumnDefn> columns,
      @JsonProperty("properties") Map<String, Object> properties
  )
  {
    super(properties);
    this.inputSource = inputSource;
    this.format = format;
    this.columns = columns;
  }

  @JsonProperty("inputSource")
  public InputSource inputSource()
  {
    return inputSource;
  }

  @JsonProperty("format")
  public InputFormat format()
  {
    return format;
  }

  @JsonProperty("columns")
  public List<InputColumnDefn> columns()
  {
    return columns;
  }

  @Override
  public void validate()
  {
    super.validate();
    if (inputSource == null) {
      throw new IAE("The input source is required");
    }
    if (format == null) {
      throw new IAE("The format is required");
    }
    if (columns == null || columns.isEmpty()) {
      throw new IAE("An input source must specify one or more columns");
    }
    Set<String> names = new HashSet<>();
    for (ColumnDefn col : columns) {
      if (!names.add(col.name())) {
        throw new IAE("Duplicate column name: " + col.name());
      }
      col.validate();
    }
  }

  @Override
  public String defaultSchema()
  {
    return TableId.INPUT_SCHEMA;
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public Builder toBuilder()
  {
    return new Builder(this);
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
    InputSourceDefn other = (InputSourceDefn) o;
    return Objects.equals(this.inputSource, other.inputSource)
        && Objects.equals(this.format, other.format)
        && Objects.equals(this.columns, other.columns)
        && Objects.equals(this.properties(), other.properties());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        inputSource,
        format,
        columns,
        properties());
  }

  public static class Builder
  {
    private InputSource inputSource;
    private InputFormat format;
    private List<InputColumnDefn> columns;
    private Map<String, Object> properties;

    public Builder()
    {
      this.columns = new ArrayList<>();
      this.properties = new HashMap<>();
    }

    public Builder(InputSourceDefn defn)
    {
      this.inputSource = defn.inputSource;
      this.format = defn.format;
      this.columns = new ArrayList<>(defn.columns);
      this.properties = new HashMap<>(defn.properties());
    }

    public Builder source(InputSource inputSource)
    {
      this.inputSource = inputSource;
      return this;
    }

    public Builder format(InputFormat format)
    {
      this.format = format;
      return this;
    }

    public List<InputColumnDefn> columns()
    {
      return columns;
    }

    public Builder column(InputColumnDefn column)
    {
      if (Strings.isNullOrEmpty(column.name())) {
        throw new IAE("Column name is required");
      }
      columns.add(column);
      return this;
    }

    public Builder column(String name, String sqlType)
    {
      return column(new InputColumnDefn(name, sqlType));
    }

    public Builder properties(Map<String, Object> properties)
    {
      this.properties = properties;
      return this;
    }

    public Builder property(String key, Object value)
    {
      if (properties == null) {
        properties = new HashMap<>();
      }
      properties.put(key, value);
      return this;
    }

    public Map<String, Object> properties()
    {
      return properties;
    }

    public InputSourceDefn build()
    {
      return new InputSourceDefn(
          inputSource,
          format,
          columns,
          properties
          );
    }
  }
}
