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
import io.druid.query.spec.QuerySegmentSpec;

import java.util.Objects;

public class DataSourceWithSegmentSpec
{
  private final DataSource dataSource;
  private final QuerySegmentSpec querySegmentSpec;

  @JsonCreator
  public DataSourceWithSegmentSpec(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec)
  {
    this.dataSource = dataSource;
    this.querySegmentSpec = querySegmentSpec;
  }

  @JsonProperty
  public DataSource getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, querySegmentSpec);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }

    if (o == null || o.getClass() != getClass()) {
      return false;
    }

    DataSourceWithSegmentSpec that = (DataSourceWithSegmentSpec) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }

    return querySegmentSpec.equals(that.querySegmentSpec);
  }

}
