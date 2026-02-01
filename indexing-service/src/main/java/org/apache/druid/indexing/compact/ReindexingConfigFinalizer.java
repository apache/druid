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

package org.apache.druid.indexing.compact;

import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;

/**
 * Functional interface for customizing a {@link DataSourceCompactionConfig} for a specific
 * {@link CompactionCandidate} before creating a reindexing job. This allows template-specific
 * logic to be injected without hardcoding behavior in {@link CompactionConfigBasedJobTemplate}.
 * <p>
 * For example, cascading reindexing templates can use this to optimize filter rules based on
 * the candidate's indexing state, while simpler templates can use the identity finalizer.
 */
@FunctionalInterface
public interface ReindexingConfigFinalizer
{
  /**
   * Customize the reindexing config for a specific candidate.
   *
   * @param config the base reindexing config
   * @param candidate the segment candidate being reindexed
   * @param params the reindexing job parameters
   * @return the finalized config to use for this candidate (this may be the same as input or a modified version)
   */
  DataSourceCompactionConfig finalizeConfig(
      DataSourceCompactionConfig config,
      CompactionCandidate candidate,
      CompactionJobParams params
  );

  /**
   * Identity finalizer that returns the config unchanged.
   * Use this for templates that don't need per-candidate customization.
   */
  ReindexingConfigFinalizer IDENTITY = (config, candidate, params) -> config;
}
