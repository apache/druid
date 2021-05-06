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

package org.apache.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseParallelIndexTaskRunner;
import org.joda.time.DateTime;

import java.util.Map;

/**
 * Configurations for ingestion tasks. These configurations can be applied to tasks when they are scheduled
 * in the overlord. If the task is a parallel task that has subtasks, the same configurations should apply
 * to all its subtasks too.
 *
 * See {@link org.apache.druid.indexing.common.config.TaskConfig} if you want to apply different configurations
 * per middleManager, indexer, and overlod.
 */
public class DefaultTaskConfig
{
  /**
   * Used to determine what protocol to use for segment allocation in {@link SinglePhaseParallelIndexTaskRunner}.
   * Every new task must use the lineage-based segment allocation protocol.
   * See {@link SinglePhaseParallelIndexTaskRunner#allocateNewSegment(String, DateTime, String, String)}
   * for more details.
   */
  public static final boolean DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY = true;

  @JsonProperty
  private final Map<String, Object> context = ImmutableMap.of(
      SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
      DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY
  );

  public Map<String, Object> getContext()
  {
    return context;
  }
}
