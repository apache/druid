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
  @JsonProperty
  private boolean enabled = false;

  // If realtime segment schema should be published in segment announcement flow
  // This config is temporary and will be removed.
  @JsonProperty
  private boolean announceRealtimeSegmentSchema = false;

  public boolean isEnabled()
  {
    return enabled;
  }

  public boolean announceRealtimeSegmentSchema()
  {
    return announceRealtimeSegmentSchema;
  }

  public static CentralizedDatasourceSchemaConfig create()
  {
    return new CentralizedDatasourceSchemaConfig();
  }

  @VisibleForTesting
  public void setEnabled(boolean enabled)
  {
    this.enabled = enabled;
  }

  @VisibleForTesting
  public void setAnnounceRealtimeSegmentSchema(boolean announceRealtimeSegmentSchema)
  {
    this.announceRealtimeSegmentSchema = announceRealtimeSegmentSchema;
  }
}
