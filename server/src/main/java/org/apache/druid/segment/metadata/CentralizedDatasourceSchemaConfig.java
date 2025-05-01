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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;

import javax.annotation.Nullable;

/**
 * Config for caching datasource schema on the Coordinator. This config must be
 * bound on the following services:
 * <ul>
 * <li>Coordinator: to enable backfill of segment schema in the metadata store
 * and caching of schema in memory.</li>
 * <li>Overlord: to enable publish of schema when segments are committed.</li>
 * <li>Peons: to enable inclusion of schema in segment publish requests.</li>
 * </ul>
 * The corresponding segment schema table is created in the metadata store only
 * when this config is enabled for the first time on a cluster. Subsequent
 * disabling of this config does not drop the table but the services do not read
 * or update it anymore.
 */
public class CentralizedDatasourceSchemaConfig
{
  public static final String PROPERTY_PREFIX = "druid.centralizedDatasourceSchema";
  private static final CentralizedDatasourceSchemaConfig DEFAULT
      = new CentralizedDatasourceSchemaConfig(null, null, null, null);

  /**
   * Current version of the persisted segment schema format.
   */
  public static final int SCHEMA_VERSION = 1;

  @JsonProperty
  private final boolean enabled;
  @JsonProperty
  private final boolean taskSchemaPublishDisabled;
  @JsonProperty
  private final boolean backFillEnabled;
  @JsonProperty
  private final long backFillPeriod;

  @JsonCreator
  public CentralizedDatasourceSchemaConfig(
      @JsonProperty("enabled") @Nullable Boolean enabled,
      @JsonProperty("backfillEnabled") @Nullable Boolean backFillEnabled,
      @JsonProperty("backfillPeriod") @Nullable Long backFillPeriod,
      @JsonProperty("taskSchemaPublishDisabled") @Nullable Boolean taskSchemaPublishDisabled
  )
  {
    this.enabled = Configs.valueOrDefault(enabled, false);
    this.backFillEnabled = Configs.valueOrDefault(backFillEnabled, true);
    this.backFillPeriod = Configs.valueOrDefault(backFillPeriod, 60_000L);
    this.taskSchemaPublishDisabled = Configs.valueOrDefault(taskSchemaPublishDisabled, false);
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public boolean isBackFillEnabled()
  {
    return backFillEnabled;
  }

  /**
   * Period in milliseconds dictating the frequency of the schema backfill job.
   */
  public long getBackFillPeriodInMillis()
  {
    return backFillPeriod;
  }

  /**
   * Config used to disable publishing of schema when a task commits segments.
   * This config is used only in integration tests to verify that schema is
   * populated correctly even when tasks fail to publish the schema.
   */
  public boolean isTaskSchemaPublishDisabled()
  {
    return taskSchemaPublishDisabled;
  }

  /**
   * @return Default config with schema management and caching disabled.
   */
  public static CentralizedDatasourceSchemaConfig create()
  {
    return DEFAULT;
  }

  public static CentralizedDatasourceSchemaConfig enabled(boolean enabled)
  {
    return new CentralizedDatasourceSchemaConfig(enabled, null, null, null);
  }
}
