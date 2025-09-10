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
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Objects;

/**
 * Request object for updating the configuration of a running {@link SeekableStreamIndexTask}.
 */
public class TaskConfigUpdateRequest
{
  private final SeekableStreamIndexTaskIOConfig ioConfig;

  @JsonCreator
  public TaskConfigUpdateRequest(
      @JsonProperty("ioConfig") @Nullable SeekableStreamIndexTaskIOConfig ioConfig
  )
  {
    this.ioConfig = ioConfig;
  }

  @JsonProperty
  public SeekableStreamIndexTaskIOConfig getIoConfig()
  {
    return ioConfig;
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
    TaskConfigUpdateRequest that = (TaskConfigUpdateRequest) o;
    return Objects.equals(ioConfig, that.ioConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(ioConfig);
  }

  @Override
  public String toString()
  {
    return "TaskConfigUpdateRequest{" +
           "ioConfig=" + ioConfig +
           '}';
  }
}
