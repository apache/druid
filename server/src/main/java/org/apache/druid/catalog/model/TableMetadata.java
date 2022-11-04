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
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.Map;
import java.util.Objects;

/**
 * REST API level description of a table. Tables have multiple types
 * as described by subclasses. Stores the operational aspects of a
 * table, such as its name, creation time, state and spec.
 * <p>
 * Doubles as a "holder" of partial collections of table metadata
 * internally. For example, in the "edit" operation, the id and
 * a partial spec will be available, the other fields are implicitly
 * unset. The set of provided fields is implicit in the code that uses
 * the object. (Providing partial information avoids the need to query
 * the DB for information that won't actually be used.)
 *
 * @see {@link ResolvedTable} for the semantic representation.
 */
@PublicApi
public class TableMetadata
{
  /**
   * State of the metadata table entry (not necessarily of the underlying
   * datasource.) A table entry will be Active normally. The Deleting state
   * is provided to handle one very specific case: a request to delete a
   * datasource. Since datasources are large, and consist of a large number of
   * segments, it takes time to unload segments from data nodes, then physically
   * delete those segments. The Deleting state says that this process has started.
   * It tell the Broker to act as if the table no longer exists in queries, but
   * not to allow creation of a new table of the same name until the deletion
   * process completes and the table metadata entry is deleted.
   */
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
      TableSpec spec
  )
  {
    return new TableMetadata(
        id,
        0,
        0,
        TableState.ACTIVE,
        spec
    );
  }

  public static TableMetadata of(
      TableId id,
      TableSpec spec
  )
  {
    return new TableMetadata(
        id,
        0,
        0,
        TableState.ACTIVE,
        spec
    );
  }

  public static TableMetadata forUpdate(TableId id, long updateTime, TableSpec spec)
  {
    return new TableMetadata(
        id,
        0,
        updateTime,
        TableState.ACTIVE,
        spec
    );
  }

  public static TableMetadata empty(TableId id)
  {
    return new TableMetadata(
        id,
        0,
        0,
        null,
        null
    );
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

  public TableMetadata withColumns(TableMetadata update)
  {
    return new TableMetadata(
        id,
        creationTime,
        update.updateTime(),
        state,
        spec.withColumns(update.spec().columns())
    );
  }

  public TableMetadata withProperties(TableMetadata update)
  {
    return new TableMetadata(
        id,
        creationTime,
        update.updateTime(),
        state,
        spec.withProperties(update.spec().properties())
    );
  }

  public TableMetadata withProperties(Map<String, Object> props)
  {
    return new TableMetadata(
        id,
        creationTime,
        updateTime,
        state,
        spec.withProperties(props)
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
