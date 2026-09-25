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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Identifies a Druid system table whose rows are supplied by a service rather than by segments.
 */
public class SystemTableDataSource extends LeafDataSource
{
  public static final String NODE_QUERY_ID_PREFIX = "native-system-node-";

  private final String table;

  @JsonCreator
  public SystemTableDataSource(@JsonProperty("table") final String table)
  {
    this.table = Preconditions.checkNotNull(table, "table");
  }

  @JsonProperty
  public String getTable()
  {
    return table;
  }

  @Override
  public Set<String> getTableNames()
  {
    // QueryScheduler uses table names as cancellation authorization resources. Namespace the synthetic datasource
    // name so a system table cannot collide with a regular Druid datasource that has the same unqualified name.
    return Collections.singleton("sys." + table);
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

  @Override
  public boolean isProcessable()
  {
    // The owning service must first resolve this datasource to an InlineDataSource.
    return false;
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
    return Objects.equals(table, that.table);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(table);
  }

  @Override
  public String toString()
  {
    return "SystemTableDataSource{" +
           "table='" + table + '\'' +
           '}';
  }
}
