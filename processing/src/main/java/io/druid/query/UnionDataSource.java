/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

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
  public List<String> getNames()
  {
    return Lists.transform(
        dataSources,
        new Function<TableDataSource, String>()
        {
          @Override
          public String apply(TableDataSource input)
          {
            return Iterables.getOnlyElement(input.getNames());
          }
        }
    );
  }

  @JsonProperty
  public List<TableDataSource> getDataSources()
  {
    return dataSources;
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
