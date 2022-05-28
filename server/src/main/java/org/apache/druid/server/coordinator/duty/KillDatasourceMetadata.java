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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

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
public class KillDatasourceMetadata implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillDatasourceMetadata.class);

  private final long period;
  private final long retainDuration;
  private long lastKillTime = 0;

  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final MetadataSupervisorManager metadataSupervisorManager;

  @Inject
  public KillDatasourceMetadata(
      DruidCoordinatorConfig config,
      IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      MetadataSupervisorManager metadataSupervisorManager
  )
  {
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.period = config.getCoordinatorDatasourceKillPeriod().getMillis();
    Preconditions.checkArgument(
        this.period >= config.getCoordinatorMetadataStoreManagementPeriod().getMillis(),
        "Coordinator datasource metadata kill period must be >= druid.coordinator.period.metadataStoreManagementPeriod"
    );
    this.retainDuration = config.getCoordinatorDatasourceKillDurationToRetain().getMillis();
    Preconditions.checkArgument(this.retainDuration >= 0, "Coordinator datasource metadata kill retainDuration must be >= 0");
    Preconditions.checkArgument(this.retainDuration < System.currentTimeMillis(), "Coordinator datasource metadata kill retainDuration cannot be greater than current time in ms");
    log.debug(
        "Datasource Metadata Kill Task scheduling enabled with period [%s], retainDuration [%s]",
        this.period,
        this.retainDuration
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    long currentTimeMillis = System.currentTimeMillis();
    if ((lastKillTime + period) < currentTimeMillis) {
      lastKillTime = currentTimeMillis;
      long timestamp = currentTimeMillis - retainDuration;
      try {
        // Datasource metadata only exists for datasource with supervisor
        // To determine if datasource metadata is still active, we check if the supervisor for that particular datasource
        // is still active or not
        Map<String, SupervisorSpec> allActiveSupervisor = metadataSupervisorManager.getLatestActiveOnly();
        Set<String> allDatasourceWithActiveSupervisor = allActiveSupervisor.values()
                                                                           .stream()
                                                                           .map(supervisorSpec -> supervisorSpec.getDataSources())
                                                                           .flatMap(Collection::stream)
                                                                           .filter(datasource -> !Strings.isNullOrEmpty(datasource))
                                                                           .collect(Collectors.toSet());
        // We exclude removing datasource metadata with active supervisor
        int datasourceMetadataRemovedCount = indexerMetadataStorageCoordinator.removeDataSourceMetadataOlderThan(
            timestamp,
            allDatasourceWithActiveSupervisor
        );
        ServiceEmitter emitter = params.getEmitter();
        emitter.emit(
            new ServiceMetricEvent.Builder().build(
                "metadata/kill/datasource/count",
                datasourceMetadataRemovedCount
            )
        );
        log.info(
            "Finished running KillDatasourceMetadata duty. Removed %,d datasource metadata",
            datasourceMetadataRemovedCount
        );
      }
      catch (Exception e) {
        log.error(e, "Failed to kill datasource metadata");
      }
    }
    return params;
  }
}
