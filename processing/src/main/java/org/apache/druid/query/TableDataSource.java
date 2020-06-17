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
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@JsonTypeName("table")
public class TableDataSource implements DataSource
{
  private final String name;

  @JsonCreator
  public TableDataSource(@JsonProperty("name") String name)
  {
    this.name = Preconditions.checkNotNull(name, "'name' must be nonnull");
  }

  @JsonCreator
  public static TableDataSource create(final String name)
  {
    return new TableDataSource(name);
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public Set<String> getTableNames()
  {
    return Collections.singleton(name);
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public boolean isCacheable()
  {
    return true;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isConcrete()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return name;
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
    TableDataSource that = (TableDataSource) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name);
  }
}
