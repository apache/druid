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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.catalog.CatalogManager.TableState;

import java.util.Objects;

/**
 * REST API level description of a table. Tables have multiple types
 * as described by subclasses.
 */
@PublicApi
public class TableSpec
{
  private final String dbSchema;
  private final String name;
  private final String owner;
  private final long creationTime;
  private final long updateTime;
  private final TableState state;
  private final TableDefn defn;

  public TableSpec(
      @JsonProperty("dbSchema") String dbSchema,
      @JsonProperty("name") String name,
      @JsonProperty("owner") String owner,
      @JsonProperty("creationTime") long creationTime,
      @JsonProperty("updateTime") long updateTime,
      @JsonProperty("state") TableState state,
      @JsonProperty("defn") TableDefn defn)
  {
    this.dbSchema = dbSchema;
    this.name = name;
    this.owner = owner;
    this.creationTime = creationTime;
    this.updateTime = updateTime;
    this.state = state;
    this.defn = defn;
  }

  public static TableSpec newTable(
      TableId id,
      TableDefn defn
  )
  {
    return newTable(id.schema(), id.name(), defn);
  }

  public static TableSpec newTable(
      String dbSchema,
      String name,
      TableDefn defn
  )
  {
    return new TableSpec(
        dbSchema,
        name,
        null,
        0,
        0,
        TableState.ACTIVE,
        defn);
  }

  public static TableSpec newSegmentTable(
      String name,
      TableDefn defn
  )
  {
    return newTable(
        TableId.DRUID_SCHEMA,
        name,
        defn);
  }

  public TableSpec fromInsert(String dbSchema, long updateTime)
  {
    return new TableSpec(
        dbSchema,
        name,
        owner,
        updateTime,
        updateTime,
        state,
        defn);
  }

  public TableSpec asUpdate(long updateTime)
  {
    return new TableSpec(
        dbSchema,
        name,
        owner,
        creationTime,
        updateTime,
        state,
        defn);
  }

  public TableSpec withSchema(String dbSchema)
  {
    if (dbSchema.equals(this.dbSchema)) {
      return this;
    }
    return new TableSpec(
        dbSchema,
        name,
        owner,
        creationTime,
        updateTime,
        state,
        defn);
  }

  public TableId id()
  {
    return new TableId(resolveDbSchema(), name);
  }

  @JsonProperty("dbSchema")
  public String dbSchema()
  {
    return dbSchema;
  }

  @JsonProperty("name")
  public String name()
  {
    return name;
  }

  public String sqlName()
  {
    return StringUtils.format("\"%s\".\"%s\"", dbSchema, name);
  }

  @JsonProperty("owner")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String owner()
  {
    return owner;
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

  @JsonProperty("defn")
  public TableDefn defn()
  {
    return defn;
  }

  /**
   * Syntactic validation of a table object. Validates only that which
   * can be checked from this table object.
   */
  public void validate()
  {
    if (Strings.isNullOrEmpty(dbSchema)) {
      throw new IAE("Database schema is required");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IAE("Table name is required");
    }
    if (defn != null) {
      defn.validate();
    }
  }

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return CatalogDefns.toBytes(jsonMapper, this);
  }

  public static TableSpec fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return CatalogDefns.fromBytes(jsonMapper, bytes, TableSpec.class);
  }

  @Override
  public String toString()
  {
    return CatalogDefns.toString(this);
  }

  public String resolveDbSchema()
  {
    if (!Strings.isNullOrEmpty(dbSchema)) {
      return dbSchema;
    } else if (defn != null) {
      return defn.defaultSchema();
    } else {
      return null;
    }
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
    return Objects.equals(dbSchema, other.dbSchema)
        && Objects.equals(name, other.name)
        && Objects.equals(owner, other.owner)
        && creationTime == other.creationTime
        && updateTime == other.updateTime
        && state == other.state
        && Objects.equals(defn, other.defn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dbSchema,
        name,
        owner,
        creationTime,
        updateTime,
        state,
        defn);
  }
}
