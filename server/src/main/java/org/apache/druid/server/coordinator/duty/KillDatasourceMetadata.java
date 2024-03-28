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

package org.apache.druid.server.coordinator.duty;

import com.google.common.base.Strings;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CoordinatorDuty for automatic deletion of datasource metadata from the datasource table in metadata storage.
 * (Note: datasource metadata only exists for datasource created from supervisor).
 * Note that this class relies on the supervisorSpec.getDataSources names to match with the
 * 'datasource' column of the datasource metadata table.
 */
public class KillDatasourceMetadata extends MetadataCleanupDuty
{
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final MetadataSupervisorManager metadataSupervisorManager;

  public KillDatasourceMetadata(
      MetadataCleanupConfig config,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      MetadataSupervisorManager metadataSupervisorManager
  )
  {
    super("datasources", config, Stats.Kill.DATASOURCES);
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.metadataSupervisorManager = metadataSupervisorManager;
  }

  @Override
  protected int cleanupEntriesCreatedBefore(DateTime minCreatedTime)
  {
    // Datasource metadata only exists for datasource with supervisor
    // To determine if datasource metadata is still active, we check if the supervisor for that particular datasource
    // is still active or not
    Map<String, SupervisorSpec> allActiveSupervisor = metadataSupervisorManager.getLatestActiveOnly();
    Set<String> allDatasourceWithActiveSupervisor
        = allActiveSupervisor.values()
                             .stream()
                             .map(SupervisorSpec::getDataSources)
                             .flatMap(Collection::stream)
                             .filter(datasource -> !Strings.isNullOrEmpty(datasource))
                             .collect(Collectors.toSet());

    // We exclude removing datasource metadata with active supervisor
    return indexerMetadataStorageCoordinator.removeDataSourceMetadataOlderThan(
        minCreatedTime.getMillis(),
        allDatasourceWithActiveSupervisor
    );
  }
}
