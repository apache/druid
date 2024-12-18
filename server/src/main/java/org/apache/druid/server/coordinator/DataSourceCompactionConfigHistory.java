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

import org.apache.druid.audit.AuditInfo;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Stack;

/**
 * A utility class to build the config history for a datasource from audit entries for
 * {@link DruidCompactionConfig}. The {@link DruidCompactionConfig} contains the entire config for the
 * cluster, so this class creates adds audit entires to the history only when a setting for this datasource or a global
 * setting has changed.
 */
public class DataSourceCompactionConfigHistory
{
  private final Stack<DataSourceCompactionConfigAuditEntry> auditEntries = new Stack<>();
  private final String dataSource;

  public DataSourceCompactionConfigHistory(String dataSource)
  {
    this.dataSource = dataSource;
  }

  public void add(DruidCompactionConfig compactionConfig, AuditInfo auditInfo, DateTime auditTime)
  {
    final DataSourceCompactionConfigAuditEntry previousEntry = auditEntries.isEmpty() ? null : auditEntries.peek();
    final DataSourceCompactionConfigAuditEntry newEntry = new DataSourceCompactionConfigAuditEntry(
        compactionConfig.clusterConfig(),
        compactionConfig.findConfigForDatasource(dataSource).orNull(),
        auditInfo,
        auditTime
    );

    final boolean shouldAddEntry;
    if (previousEntry == null) {
      shouldAddEntry = newEntry.getCompactionConfig() != null;
    } else {
      shouldAddEntry = !newEntry.hasSameConfig(previousEntry);
    }

    if (shouldAddEntry) {
      auditEntries.push(newEntry);
    }
  }

  public List<DataSourceCompactionConfigAuditEntry> getEntries()
  {
    return auditEntries;
  }
}
