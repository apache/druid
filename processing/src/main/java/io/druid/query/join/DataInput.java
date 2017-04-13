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

package io.druid.query.join;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Iterables;
import io.druid.query.BaseQuery;
import io.druid.query.DataSource;
import io.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Duration;

import java.util.Objects;

public class DataInput implements JoinInputSpec
{
  private final DataSource dataSource;
  private final QuerySegmentSpec querySegmentSpec;
  private volatile Duration duration;

  @JsonCreator
  public DataInput(
      @JsonProperty("dataSource") DataSource dataSource,
      @JsonProperty("intervals") QuerySegmentSpec querySegmentSpec
  )
  {
    this.dataSource = Objects.requireNonNull(dataSource);
    this.querySegmentSpec = Objects.requireNonNull(querySegmentSpec);
  }

  public DataInput(
      DataSource dataSource,
      QuerySegmentSpec querySegmentSpec,
      Duration duration
  )
  {
    this.dataSource = Objects.requireNonNull(dataSource);
    this.querySegmentSpec = Objects.requireNonNull(querySegmentSpec);
    this.duration = Objects.requireNonNull(duration);
  }

  @JsonProperty("dataSource")
  public DataSource getDataSource()
  {
    return dataSource;
  }

  public String getName()
  {
    return Iterables.getOnlyElement(dataSource.getNames());
  }

  @JsonProperty("intervals")
  public QuerySegmentSpec getQuerySegmentSpec()
  {
    return querySegmentSpec;
  }

  public Duration getDuration()
  {
    if (duration == null) {
      this.duration = BaseQuery.getTotalDuration(querySegmentSpec);
    }
    return duration;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataInput that = (DataInput) o;
    if (!dataSource.equals(that.dataSource)) {
      return false;
    }

    return querySegmentSpec.equals(that.querySegmentSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, querySegmentSpec);
  }

  @Override
  public DataInput accept(JoinSpecVisitor visitor)
  {
    return visitor.visit(this);
  }
}
