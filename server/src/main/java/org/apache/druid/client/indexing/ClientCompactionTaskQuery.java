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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * Client representation of org.apache.druid.indexing.common.task.CompactionTask. JSON serialization fields of
 * this class must correspond to those of org.apache.druid.indexing.common.task.CompactionTask.
 */
public class ClientCompactionTaskQuery implements ClientTaskQuery
{
  static final String TYPE = "compact";

  private final String id;
  private final String dataSource;
  private final ClientCompactionIOConfig ioConfig;
  private final ClientCompactionTaskQueryTuningConfig tuningConfig;
  private final ClientCompactionTaskGranularitySpec granularitySpec;
  private final ClientCompactionTaskDimensionsSpec dimensionsSpec;
  private final AggregatorFactory[] metricsSpec;
  private final ClientCompactionTaskTransformSpec transformSpec;
  private final Map<String, Object> context;

  @JsonCreator
  public ClientCompactionTaskQuery(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("ioConfig") ClientCompactionIOConfig ioConfig,
      @JsonProperty("tuningConfig") ClientCompactionTaskQueryTuningConfig tuningConfig,
      @JsonProperty("granularitySpec") ClientCompactionTaskGranularitySpec granularitySpec,
      @JsonProperty("dimensionsSpec") ClientCompactionTaskDimensionsSpec dimensionsSpec,
      @JsonProperty("metricsSpec") AggregatorFactory[] metrics,
      @JsonProperty("transformSpec") ClientCompactionTaskTransformSpec transformSpec,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.dataSource = dataSource;
    this.ioConfig = ioConfig;
    this.tuningConfig = tuningConfig;
    this.granularitySpec = granularitySpec;
    this.dimensionsSpec = dimensionsSpec;
    this.metricsSpec = metrics;
    this.transformSpec = transformSpec;
    this.context = context;
  }

  @JsonProperty
  @Override
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public ClientCompactionIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  public ClientCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @JsonProperty
  public ClientCompactionTaskGranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  public ClientCompactionTaskDimensionsSpec getDimensionsSpec()
  {
    return dimensionsSpec;
  }

  @JsonProperty
  @Nullable
  public AggregatorFactory[] getMetricsSpec()
  {
    return metricsSpec;
  }

  @JsonProperty
  public ClientCompactionTaskTransformSpec getTransformSpec()
  {
    return transformSpec;
  }

  @JsonProperty
  public Map<String, Object> getContext()
  {
    return context;
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
    ClientCompactionTaskQuery that = (ClientCompactionTaskQuery) o;
    return Objects.equals(id, that.id) &&
           Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(ioConfig, that.ioConfig) &&
           Objects.equals(tuningConfig, that.tuningConfig) &&
           Objects.equals(granularitySpec, that.granularitySpec) &&
           Objects.equals(dimensionsSpec, that.dimensionsSpec) &&
           Arrays.equals(metricsSpec, that.metricsSpec) &&
           Objects.equals(transformSpec, that.transformSpec) &&
           Objects.equals(context, that.context);
  }

  @Override
  public int hashCode()
  {
    int result = Objects.hash(
        id,
        dataSource,
        ioConfig,
        tuningConfig,
        granularitySpec,
        dimensionsSpec,
        transformSpec,
        context
    );
    result = 31 * result + Arrays.hashCode(metricsSpec);
    return result;
  }

  @Override
  public String toString()
  {
    return "ClientCompactionTaskQuery{" +
           "id='" + id + '\'' +
           ", dataSource='" + dataSource + '\'' +
           ", ioConfig=" + ioConfig +
           ", tuningConfig=" + tuningConfig +
           ", granularitySpec=" + granularitySpec +
           ", dimensionsSpec=" + dimensionsSpec +
           ", metricsSpec=" + Arrays.toString(metricsSpec) +
           ", transformSpec=" + transformSpec +
           ", context=" + context +
           '}';
  }
}
