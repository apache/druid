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

package org.apache.druid.indextable.loader.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Config for the extension that describes the configuration needed for all indexed tables that need to be loaded.
 */
public class IndexedTableLoaderConfig
{
  @Min(1)
  @JsonProperty
  private int numThreads = 1;

  @NotNull
  @JsonProperty
  private String configFilePath;

  @JsonProperty
  public int getNumThreads()
  {
    return numThreads;
  }

  @JsonProperty
  public String getConfigFilePath()
  {
    return configFilePath;
  }
}
