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

package org.apache.druid.server.coordinator;

import java.util.LinkedList;
import java.util.List;

import org.apache.druid.audit.AuditInfo;
import org.joda.time.DateTime;

/**
 * A utility class to build the config history for a datasource from audit entries for
 * {@link CoordinatorCompactionConfig}. The {@link CoordinatorCompactionConfig} contains the entire config for the
 * cluster, so this class creates adds audit entires to the history only when a setting for this datasource or a global
 * setting has changed.
 */
public class DataSourceCompactionConfigHistory {
  private final List<DatasourceCompactionConfigAuditEntry> auditEntries = new LinkedList<>();
  private final String dataSource;
  private DatasourceCompactionConfigAuditEntry current;

  public DataSourceCompactionConfigHistory(String dataSource) {
    this.dataSource = dataSource;
  }

  public void add(CoordinatorCompactionConfig coordinatorCompactionConfig, AuditInfo auditInfo, DateTime auditTime) {
    for (DataSourceCompactionConfig dataSourceCompactionConfig : coordinatorCompactionConfig.getCompactionConfigs()) {
      if (dataSource.equals(dataSourceCompactionConfig.getDataSource())) {
        if (
            current == null ||
            (
                !current.getCompactionConfig().equals(dataSourceCompactionConfig) ||
                !current.getGlobalConfig().hasSameConfig(coordinatorCompactionConfig)
            )
        ) {
          current = new DatasourceCompactionConfigAuditEntry(
              new DatasourceCompactionConfigAuditEntry.GlobalCompactionConfig(
                  coordinatorCompactionConfig.getCompactionTaskSlotRatio(),
                  coordinatorCompactionConfig.getMaxCompactionTaskSlots(),
                  coordinatorCompactionConfig.isUseAutoScaleSlots()
              ),
              dataSourceCompactionConfig,
              auditInfo,
              auditTime
          );
          auditEntries.add(current);
        }
        break;
      }
    }
  }
  public List<DatasourceCompactionConfigAuditEntry> getHistory() {
    return auditEntries;
  }
}