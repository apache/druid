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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Response object for returning the configuration of a running {@link SeekableStreamIndexTask}.
 */
public class TaskConfigResponse<PartitionIdType, SequenceOffsetType>
{
  private final SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig;
  private final String supervisorSpecVersion;

  @JsonCreator
  public TaskConfigResponse(
      @JsonProperty("ioConfig") @Nullable SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> ioConfig,
      @JsonProperty("supervisorSpecVersion") String supervisorSpecVersion
  )
  {
    this.ioConfig = ioConfig;
    this.supervisorSpecVersion = supervisorSpecVersion;
  }

  @JsonProperty
  public SeekableStreamIndexTaskIOConfig<PartitionIdType, SequenceOffsetType> getIoConfig()
  {
    return ioConfig;
  }

  @JsonProperty
  public String getSupervisorSpecVersion()
  {
    return supervisorSpecVersion;
  }

  @Override
  public String toString()
  {
    return "TaskConfigResponse{" +
           "ioConfig=" + ioConfig +
           ", supervisorSpecVersion='" + supervisorSpecVersion + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object object)
  {
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    TaskConfigResponse<?, ?> that = (TaskConfigResponse<?, ?>) object;
    return Objects.equals(ioConfig, that.ioConfig) && Objects.equals(
        supervisorSpecVersion,
        that.supervisorSpecVersion
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(ioConfig, supervisorSpecVersion);
  }

}
