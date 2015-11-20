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

package io.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.List;

/**
 */
public class CoordinatorHadoopMergeSpec
{

  private final String dataSource;
  private final QueryGranularity queryGranularity;
  private final List<String> dimensions;
  private final AggregatorFactory[] metricsSpec;

  public CoordinatorHadoopMergeSpec(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("queryGranularity") QueryGranularity queryGranularity,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metricsSpec") AggregatorFactory[] metricsSpec
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.queryGranularity = queryGranularity;
    this.dimensions = dimensions;
    this.metricsSpec = metricsSpec;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public QueryGranularity getQueryGranularity()
  {
    return queryGranularity;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @Override
  public String toString()
  {
    return "CoordinatorHadoopMergeSpec{" +
           "dataSource='" + dataSource + '\'' +
           ", queryGranularity=" + queryGranularity +
           ", dimensions=" + dimensions +
           ", metricsSpec=" + Arrays.toString(metricsSpec) +
           '}';
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

    CoordinatorHadoopMergeSpec that = (CoordinatorHadoopMergeSpec) o;

    if (dataSource != null ? !dataSource.equals(that.dataSource) : that.dataSource != null) {
      return false;
    }
    if (queryGranularity != null ? !queryGranularity.equals(that.queryGranularity) : that.queryGranularity != null) {
      return false;
    }
    if (dimensions != null ? !dimensions.equals(that.dimensions) : that.dimensions != null) {
      return false;
    }
    // Probably incorrect - comparing Object[] arrays with Arrays.equals
    return Arrays.equals(metricsSpec, that.metricsSpec);

  }

  @Override
  public int hashCode()
  {
    int result = dataSource != null ? dataSource.hashCode() : 0;
    result = 31 * result + (queryGranularity != null ? queryGranularity.hashCode() : 0);
    result = 31 * result + (dimensions != null ? dimensions.hashCode() : 0);
    result = 31 * result + Arrays.hashCode(metricsSpec);
    return result;
  }
}
