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

package org.apache.druid.server.coordinator.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.joda.time.Duration;

/**
 * Config to cleanup unused segment schemas.
 * <p>
 * Extended from {@link MetadataCleanupConfig} to allow having different defaults.
 */
public class KillUnusedSchemasConfig extends MetadataCleanupConfig
{
  public static final KillUnusedSchemasConfig DEFAULT = new KillUnusedSchemasConfig(null, null, null);

  @JsonCreator
  public KillUnusedSchemasConfig(
      @JsonProperty("on") Boolean cleanupEnabled,
      @JsonProperty("period") Duration cleanupPeriod,
      @JsonProperty("durationToRetain") Duration durationToRetain
  )
  {
    super(
        Configs.valueOrDefault(cleanupEnabled, true),
        Configs.valueOrDefault(cleanupPeriod, Duration.standardHours(1)),
        Configs.valueOrDefault(durationToRetain, Duration.standardHours(6))
    );
  }
}
