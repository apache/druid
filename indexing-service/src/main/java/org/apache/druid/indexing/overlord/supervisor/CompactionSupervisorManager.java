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

package org.apache.druid.indexing.overlord.supervisor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.NotFound;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.DataSourceCompactionConfigAuditEntry;
import org.apache.druid.server.security.AuthorizationUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Wrapper over {@link SupervisorManager} used by
 * {@link org.apache.druid.indexing.overlord.http.OverlordCompactionResource}
 * to read and write compaction supervisor specs.
 */
public class CompactionSupervisorManager
{
  private final TaskMaster taskMaster;
  private final AuditManager auditManager;

  @Inject
  public CompactionSupervisorManager(
      TaskMaster taskMaster,
      AuditManager auditManager
  )
  {
    this.taskMaster = taskMaster;
    this.auditManager = auditManager;
  }

  /**
   * Creates or updates a compaction supervisor spec.
   *
   * @return true if the supervisor was updated successfully or if the supervisor
   * is already in the desired state.
   */
  public boolean updateCompactionSupervisor(
      CompactionSupervisorSpec spec,
      HttpServletRequest request
  )
  {
    return performIfLeader(manager -> {
      // Check if the spec needs to be updated
      if (manager.shouldUpdateSupervisor(spec) && manager.createOrUpdateAndStartSupervisor(spec)) {
        final String auditPayload
            = StringUtils.format("Update supervisor[%s] for datasource[%s]", spec.getId(), spec.getDataSources());
        auditManager.doAudit(
            AuditEntry.builder()
                      .key(spec.getId())
                      .type("supervisor")
                      .auditInfo(AuthorizationUtils.buildAuditInfo(request))
                      .request(AuthorizationUtils.buildRequestInfo("overlord", request))
                      .payload(auditPayload)
                      .build()
        );
      }

      return true;
    });
  }

  /**
   * Gets the compaction supervisor for the given datasource, if one exists.
   *
   * @throws DruidException if a compaction supervisor does not exist for this
   *                        datasource or if the supervisor is of an unexpected type.
   */
  public CompactionSupervisorSpec getCompactionSupervisor(String dataSource)
  {
    final String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(dataSource);

    return performIfLeader(manager -> {
      final Optional<SupervisorSpec> specOptional = manager.getSupervisorSpec(supervisorId);

      if (specOptional.isPresent()) {
        SupervisorSpec spec = specOptional.get();
        if (spec instanceof CompactionSupervisorSpec) {
          return (CompactionSupervisorSpec) spec;
        } else {
          throw DruidException.defensive(
              "Supervisor for ID[%s] is of unexpected type[%s]",
              supervisorId, spec.getClass().getSimpleName()
          );
        }
      } else {
        throw NotFound.exception("Compaction supervisor for datasource[%s] does not exist", dataSource);
      }
    });
  }

  /**
   * Deletes the compaction supervisor for the given datasource, if one exists.
   *
   * @return true if the supervisor was successfully deleted.
   * @throws DruidException if a compaction supervisor does not exist for this
   *                        datasource.
   */
  public boolean deleteCompactionSupervisor(String dataSource)
  {
    final String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(dataSource);

    return performIfLeader(manager -> {
      if (manager.stopAndRemoveSupervisor(supervisorId)) {
        return true;
      } else {
        throw NotFound.exception("Compaction supervisor for datasource[%s] does not exist", dataSource);
      }
    });
  }

  /**
   * Returns all compaction supervisors.
   */
  public List<CompactionSupervisorSpec> getAllCompactionSupervisors()
  {
    return performIfLeader(manager -> {
      final List<CompactionSupervisorSpec> compactionSpecs = new ArrayList<>();
      for (String supervisorId : Set.copyOf(manager.getSupervisorIds())) {
        Optional<SupervisorSpec> supervisorSpecOptional = manager.getSupervisorSpec(supervisorId);
        if (!supervisorSpecOptional.isPresent()) {
          continue;
        }

        final SupervisorSpec supervisorSpec = supervisorSpecOptional.get();
        if (supervisorSpec instanceof CompactionSupervisorSpec) {
          compactionSpecs.add(((CompactionSupervisorSpec) supervisorSpec));
        }
      }

      return compactionSpecs;
    });
  }

  /**
   * Gets the change history for the compaction supervisor of the given datasource.
   *
   * @return Change history for the compaction supervisor of the given datasource
   * in descending order by update time or an empty list if no history exists
   * for the compaction supervisor of this datasource.
   */
  public List<DataSourceCompactionConfigAuditEntry> getCompactionSupervisorHistory(String dataSource)
  {
    final String supervisorId = CompactionSupervisorSpec.getSupervisorIdForDatasource(dataSource);

    return performIfLeader(
        manager -> manager
            .getSupervisorHistoryForId(supervisorId)
            .stream()
            .filter(versionedSpec -> versionedSpec.getSpec() instanceof CompactionSupervisorSpec)
            .map(
                versionedSupervisorSpec -> new DataSourceCompactionConfigAuditEntry(
                    null,
                    ((CompactionSupervisorSpec) versionedSupervisorSpec.getSpec()).getSpec(),
                    null,
                    nullSafeDate(versionedSupervisorSpec.getVersion())
                )
            )
            .collect(Collectors.toList())
    );
  }

  private <T> T performIfLeader(Function<SupervisorManager, T> managerFunction)
  {
    Optional<SupervisorManager> supervisorManager = taskMaster.getSupervisorManager();
    if (supervisorManager.isPresent()) {
      return managerFunction.apply(supervisorManager.get());
    } else {
      // Encourage client to try again soon, when we'll likely have a redirect set up
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.SERVICE_UNAVAILABLE)
                          .build("Overlord is not leader");
    }
  }

  @Nullable
  private static DateTime nullSafeDate(String date)
  {
    return date == null || date.isEmpty() ? null : DateTimes.of(date);
  }
}
