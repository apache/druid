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

package io.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class IndexingServiceSelectorConfig
{
  public static final String DEFAULT_SERVICE_NAME = "druid/overlord";

  @JsonProperty
  private String serviceName = DEFAULT_SERVICE_NAME;

  public String getServiceName()
  {
    return serviceName;
  }
}
