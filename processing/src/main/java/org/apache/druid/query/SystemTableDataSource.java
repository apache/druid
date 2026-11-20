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

package org.apache.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.query.policy.PolicyEnforcer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Identifies a Druid system table whose rows are supplied by a service rather than by segments. Each service reads its
 * own rows of the table: its {@link Scope} is {@link Scope#LOCAL}. The SQL layer on the Broker gives the tables it
 * plans the {@link Scope#CLUSTER} scope, and the Broker then gathers their rows from every service that serves them.
 *
 * <p>The server that authorizes a query attaches a {@link SystemTableAccess} with {@link #withAccess}, which says which
 * rows the caller may read. The table can't be read without it.</p>
 *
 * <p>Neither the access nor the scope is serialized, so a client can't supply them, and a query that is sent to another
 * service reads only that service's rows, under the access that service attaches.</p>
 */
public class SystemTableDataSource extends LeafDataSource
{
  /**
   * Which services' rows of a system table a query reads.
   */
  public enum Scope
  {
    /**
     * The rows of the service that runs the query.
     */
    LOCAL,

    /**
     * The rows of every service that serves the table, which only the Broker gathers.
     */
    CLUSTER
  }

  public static final String NODE_QUERY_ID_PREFIX = "native-system-node-";

  private final String table;

  /**
   * Name reported by {@link #getTableNames()}, for metrics, thread names and query blocklist rules. It isn't used for
   * authorization: system tables are authorized as SYSTEM_TABLE resources named {@link #table}.
   */
  private final String qualifiedTableName;

  @Nullable
  private final SystemTableAccess access;

  private final Scope scope;

  @JsonCreator
  public SystemTableDataSource(@JsonProperty("table") final String table)
  {
    this(table, null, Scope.LOCAL);
  }

  private SystemTableDataSource(final String table, @Nullable final SystemTableAccess access, final Scope scope)
  {
    this.table = Preconditions.checkNotNull(table, "table");
    this.qualifiedTableName = "sys." + table;
    this.access = access;
    this.scope = Preconditions.checkNotNull(scope, "scope");
  }

  @JsonProperty
  public String getTable()
  {
    return table;
  }

  /**
   * Whether the datasource, or one of the datasources it reads, is a system table.
   */
  public static boolean readsSystemTable(final DataSource dataSource)
  {
    if (dataSource instanceof SystemTableDataSource) {
      return true;
    }
    for (final DataSource child : dataSource.getChildren()) {
      if (readsSystemTable(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns a copy of the datasource in which every system table has the given scope.
   */
  public static DataSource withScope(final DataSource dataSource, final Scope scope)
  {
    if (dataSource instanceof SystemTableDataSource systemTable) {
      return systemTable.withScope(scope);
    }
    final List<DataSource> children = dataSource.getChildren();
    if (children.isEmpty()) {
      return dataSource;
    }
    final List<DataSource> scopedChildren = new ArrayList<>(children.size());
    for (final DataSource child : children) {
      scopedChildren.add(withScope(child, scope));
    }
    return dataSource.withChildren(scopedChildren);
  }

  /**
   * Returns the rows the caller may read, or null if the query hasn't been authorized on this server.
   */
  @JsonIgnore
  @Nullable
  public SystemTableAccess getAccess()
  {
    return access;
  }

  /**
   * Returns a copy of this datasource that carries the given access.
   */
  public SystemTableDataSource withAccess(final SystemTableAccess access)
  {
    return new SystemTableDataSource(table, Preconditions.checkNotNull(access, "access"), scope);
  }

  /**
   * Returns which services' rows the query reads. Only the Broker reads {@link Scope#CLUSTER} tables, for the queries
   * that the SQL layer plans.
   */
  @JsonIgnore
  public Scope getScope()
  {
    return scope;
  }

  /**
   * Returns a copy of this datasource with the given scope.
   */
  public SystemTableDataSource withScope(final Scope scope)
  {
    return new SystemTableDataSource(table, access, scope);
  }

  @Override
  public Set<String> getTableNames()
  {
    return Collections.singleton(qualifiedTableName);
  }

  /**
   * System tables have no row or column policies: their rows are authorized by the table's own row authorizer. Fails
   * rather than ignoring a policy that an authorizer returned for this table.
   */
  @Override
  public DataSource withPolicies(final Map<String, Optional<Policy>> policyMap, final PolicyEnforcer policyEnforcer)
  {
    if (policyMap.getOrDefault(qualifiedTableName, Optional.empty()).isPresent()) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.UNSUPPORTED)
                          .build("System table[%s] doesn't support policies", table);
    }
    return this;
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  /**
   * A system table can be processed once a server has authorized the query and attached the caller's access. Until
   * then, as when the SQL layer plans a query, it's a datasource that something else must resolve.
   */
  @Override
  public boolean isProcessable()
  {
    return access != null;
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SystemTableDataSource that = (SystemTableDataSource) o;
    return scope == that.scope
           && Objects.equals(table, that.table)
           && Objects.equals(access, that.access);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(table, access, scope);
  }

  @Override
  public String toString()
  {
    return "SystemTableDataSource{" +
           "table='" + table + '\'' +
           ", authorized=" + (access != null) +
           ", scope=" + scope +
           '}';
  }
}
