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

package org.apache.druid.queryng.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for the "NG" query engine.
 */
public class QueryNGConfig
{
  public static final String CONFIG_ROOT = "druid.queryng";

  /**
   * Whether the engine is enabled. It is disabled by default.
   */
  @JsonProperty("enabled")
  private boolean enabled;

  /**
   * Create an instance for testing.
   */
  public static QueryNGConfig create(boolean enabled)
  {
    QueryNGConfig config = new QueryNGConfig();
    config.enabled = enabled;
    return config;
  }

  public boolean enabled()
  {
    return enabled;
  }
}
