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
import com.google.common.base.Preconditions;

import java.util.List;

public class RegexDataSource implements DataSource
{
  @JsonProperty
  private final List<String> dataSources;

  @JsonCreator
  public RegexDataSource(@JsonProperty("dataSources") List<String> dataSources)
  {
    Preconditions.checkNotNull(dataSources, "dataSources cannot be null for RegexDataSource");
    this.dataSources = dataSources;
  }

  @Override
  public List<String> getNames()
  {
    return dataSources;
  }

  @JsonProperty
  public List<String> getDataSources()
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

    RegexDataSource that = (RegexDataSource) o;

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
    return "RegexDataSource{" +
           "dataSources=" + dataSources +
           '}';
  }
}
