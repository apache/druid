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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.IAE;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UnionDataSource implements DataSource
{
  @JsonProperty
  private final List<TableDataSource> dataSources;

  @JsonCreator
  public UnionDataSource(@JsonProperty("dataSources") List<TableDataSource> dataSources)
  {
    Preconditions.checkNotNull(dataSources, "dataSources cannot be null for unionDataSource");
    this.dataSources = dataSources;
  }

  @Override
  public Set<String> getTableNames()
  {
    return dataSources.stream()
                      .map(input -> Iterables.getOnlyElement(input.getTableNames()))
                      .collect(Collectors.toSet());
  }

  @JsonProperty
  public List<TableDataSource> getDataSources()
  {
    return dataSources;
  }

  @Override
  public List<DataSource> getChildren()
  {
    return ImmutableList.copyOf(dataSources);
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (children.size() != dataSources.size()) {
      throw new IAE("Expected [%d] children, got [%d]", dataSources.size(), children.size());
    }

    if (!children.stream().allMatch(dataSource -> dataSource instanceof TableDataSource)) {
      throw new IAE("All children must be tables");
    }

    return new UnionDataSource(
        children.stream().map(dataSource -> (TableDataSource) dataSource).collect(Collectors.toList())
    );
  }

  @Override
  public boolean isCacheable()
  {
    // Disables result-level caching for 'union' datasources, which doesn't work currently.
    // See https://github.com/apache/druid/issues/8713 for reference.
    //
    // Note that per-segment caching is still effective, since at the time the per-segment cache evaluates a query
    // for cacheability, it would have already been rewritten to a query on a single table.
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return dataSources.stream().allMatch(DataSource::isGlobal);
  }

  @Override
  public boolean isConcrete()
  {
    return dataSources.stream().allMatch(DataSource::isConcrete);
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

    UnionDataSource that = (UnionDataSource) o;

    if (!dataSources.equals(that.dataSources)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return dataSources.hashCode();
  }

  @Override
  public String toString()
  {
    return "UnionDataSource{" +
           "dataSources=" + dataSources +
           '}';
  }
}
