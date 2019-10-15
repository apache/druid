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

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * Client representation of org.apache.druid.indexing.common.task.CompactionTask. JSON serialization fields of
 * this class must correspond to those of org.apache.druid.indexing.common.task.CompactionTask.
 */
public class ClientCompactQuery implements ClientQuery
{
  private final String dataSource;
  private final ClientCompactionIOConfig ioConfig;
  @Nullable
  private final Long targetCompactionSizeBytes;
  private final ClientCompactQueryTuningConfig tuningConfig;
  private final Map<String, Object> context;

  @JsonCreator
  public ClientCompactQuery(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("ioConfig") ClientCompactionIOConfig ioConfig,
      @JsonProperty("targetCompactionSizeBytes") @Nullable Long targetCompactionSizeBytes,
      @JsonProperty("tuningConfig") ClientCompactQueryTuningConfig tuningConfig,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this.dataSource = dataSource;
    this.ioConfig = ioConfig;
    this.targetCompactionSizeBytes = targetCompactionSizeBytes;
    this.tuningConfig = tuningConfig;
    this.context = context;
  }

  @JsonProperty
  @Override
  public String getType()
  {
    return "compact";
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
  @Nullable
  public Long getTargetCompactionSizeBytes()
  {
    return targetCompactionSizeBytes;
  }

  @JsonProperty
  public ClientCompactQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
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
    ClientCompactQuery that = (ClientCompactQuery) o;
    return Objects.equals(dataSource, that.dataSource) &&
           Objects.equals(ioConfig, that.ioConfig) &&
           Objects.equals(targetCompactionSizeBytes, that.targetCompactionSizeBytes) &&
           Objects.equals(tuningConfig, that.tuningConfig) &&
           Objects.equals(context, that.context);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, ioConfig, targetCompactionSizeBytes, tuningConfig, context);
  }

  @Override
  public String toString()
  {
    return "ClientCompactQuery{" +
           "dataSource='" + dataSource + '\'' +
           ", ioConfig=" + ioConfig +
           ", targetCompactionSizeBytes=" + targetCompactionSizeBytes +
           ", tuningConfig=" + tuningConfig +
           ", context=" + context +
           '}';
  }
}
