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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

/**
 * Config for centralizing datasource schema management in Coordinator.
 */
public class CentralizedDatasourceSchemaConfig
{
  public static final String PROPERTY_PREFIX = "druid.centralizedDatasourceSchema";

  public static final int SCHEMA_VERSION = 1;

  @JsonProperty
  private boolean enabled = false;

  // internal config meant for testing
  @JsonProperty
  private boolean taskSchemaPublishDisabled = false;
  @JsonProperty
  private boolean backFillEnabled = true;
  @JsonProperty
  private long backFillPeriod = 60000;

  @JsonProperty
  public boolean isEnabled()
  {
    return enabled;
  }

  @JsonProperty
  public boolean isBackFillEnabled()
  {
    return backFillEnabled;
  }

  @JsonProperty
  public long getBackFillPeriod()
  {
    return backFillPeriod;
  }

  @JsonProperty
  public boolean isTaskSchemaPublishDisabled()
  {
    return taskSchemaPublishDisabled;
  }

  public static CentralizedDatasourceSchemaConfig create()
  {
    return new CentralizedDatasourceSchemaConfig();
  }

  public static CentralizedDatasourceSchemaConfig create(boolean enabled)
  {
    CentralizedDatasourceSchemaConfig config = new CentralizedDatasourceSchemaConfig();
    config.setEnabled(enabled);
    return config;
  }

  @VisibleForTesting
  public void setEnabled(boolean enabled)
  {
    this.enabled = enabled;
  }

  @VisibleForTesting
  public void setBackFillEnabled(boolean backFillEnabled)
  {
    this.backFillEnabled = backFillEnabled;
  }

  @VisibleForTesting
  public void setBackFillPeriod(long backFillPeriod)
  {
    this.backFillPeriod = backFillPeriod;
  }
}
