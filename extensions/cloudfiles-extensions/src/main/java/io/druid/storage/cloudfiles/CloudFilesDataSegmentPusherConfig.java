/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.storage.cloudfiles;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;

/**
 */
public class CloudFilesDataSegmentPusherConfig
{

  @JsonProperty
  @NotNull
  private String region;

  @JsonProperty
  @NotNull
  private String container;

  @JsonProperty
  @NotNull
  private String basePath;

  @JsonProperty
  private int operationMaxRetries = 10;

  public String getRegion()
  {
    Preconditions.checkNotNull(region);
    return region;
  }

  public String getContainer()
  {
    Preconditions.checkNotNull(container);
    return container;
  }

  public String getBasePath()
  {
    Preconditions.checkNotNull(basePath);
    return basePath;
  }

  public int getOperationMaxRetries()
  {
    return operationMaxRetries;
  }

}
