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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.jackson.JacksonUtils;

import java.util.Objects;

/**
 * REST API level description of a table. Tables have multiple types
 * as described by subclasses. Stores the operational aspects of a
 * table, such as its name, creation time, state and spec.
 *
 * @see {@link ResolvedTable} for the semantic representation.
 */
@PublicApi
public class TableMetadata
{
  public enum TableState
  {
    ACTIVE("A"),
    DELETING("D");

    private final String code;

    TableState(String code)
    {
      this.code = code;
    }

    public String code()
    {
      return code;
    }

    public static TableState fromCode(String code)
    {
      for (TableState state : values()) {
        if (state.code.equals(code)) {
          return state;
        }
      }
      throw new ISE("Unknown TableState code: " + code);
    }
  }

  private final TableId id;
  private final long creationTime;
  private final long updateTime;
  private final TableState state;
  private final TableSpec spec;

  public TableMetadata(
      @JsonProperty("id") TableId tableId,
      @JsonProperty("creationTime") long creationTime,
      @JsonProperty("updateTime") long updateTime,
      @JsonProperty("state") TableState state,
      @JsonProperty("spec") TableSpec spec)
  {
    this.id = tableId;
    this.creationTime = creationTime;
    this.updateTime = updateTime;
    this.state = state;
    this.spec = spec;
  }

  public static TableMetadata newTable(
      TableId id,
      TableSpec defn
  )
  {
    return new TableMetadata(
        id,
        0,
        0,
        TableState.ACTIVE,
        defn
    );
  }

  public static TableMetadata newSegmentTable(
      String name,
      TableSpec defn
  )
  {
    return newTable(
        TableId.datasource(name),
        defn);
  }

  public TableMetadata fromInsert(long updateTime)
  {
    return new TableMetadata(
        id,
        updateTime,
        updateTime,
        state,
        spec
    );
  }

  public TableMetadata asUpdate(long updateTime)
  {
    return new TableMetadata(
        id,
        creationTime,
        updateTime,
        state,
        spec);
  }

  public TableMetadata withSpec(TableSpec spec)
  {
    return new TableMetadata(
        id,
        creationTime,
        updateTime,
        state,
        spec
    );
  }

  @JsonProperty("id")
  public TableId id()
  {
    return id;
  }

  public String sqlName()
  {
    return id.sqlName();
  }

  @JsonProperty("state")
  public TableState state()
  {
    return state;
  }

  @JsonProperty("creationTime")
  public long creationTime()
  {
    return creationTime;
  }

  @JsonProperty("updateTime")
  public long updateTime()
  {
    return updateTime;
  }

  @JsonProperty("spec")
  public TableSpec spec()
  {
    return spec;
  }

  /**
   * Syntactic validation of a table object. Validates only that which
   * can be checked from this table object.
   */
  public void validate()
  {
    if (Strings.isNullOrEmpty(id.schema())) {
      throw new IAE("Database schema is required");
    }
    if (Strings.isNullOrEmpty(id.name())) {
      throw new IAE("Table name is required");
    }
    if (spec == null) {
      throw new IAE("A table definition must include a table spec.");
    }
  }

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return JacksonUtils.toBytes(jsonMapper, this);
  }

  public static TableMetadata fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return JacksonUtils.fromBytes(jsonMapper, bytes, TableMetadata.class);
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
    TableMetadata other = (TableMetadata) o;
    return Objects.equals(id, other.id)
        && creationTime == other.creationTime
        && updateTime == other.updateTime
        && state == other.state
        && Objects.equals(spec, other.spec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        id,
        creationTime,
        updateTime,
        state,
        spec
    );
  }
}
