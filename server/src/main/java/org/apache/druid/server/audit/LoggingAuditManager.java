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

package org.apache.druid.server.audit;

import com.google.inject.Inject;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.error.DruidException;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
 * Audit manager that logs audited events at the level specified in
 * {@link LoggingAuditManagerConfig}.
 */
public class LoggingAuditManager implements AuditManager
{
  private final AuditLogger auditLogger;
  private final LoggingAuditManagerConfig managerConfig;
  private final AuditSerdeHelper serdeHelper;

  @Inject
  public LoggingAuditManager(
      AuditManagerConfig config,
      AuditSerdeHelper serdeHelper
  )
  {
    if (!(config instanceof LoggingAuditManagerConfig)) {
      throw DruidException.defensive("Config[%s] is not an instance of LoggingAuditManagerConfig", config);
    }

    this.managerConfig = (LoggingAuditManagerConfig) config;
    this.serdeHelper = serdeHelper;
    this.auditLogger = new AuditLogger(managerConfig.getLogLevel());
  }

  @Override
  public void doAudit(AuditEntry entry)
  {
    if (serdeHelper.shouldProcessAuditEntry(entry)) {
      auditLogger.log(serdeHelper.processAuditEntry(entry));
    }
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String key, String type, Interval interval)
  {
    return Collections.emptyList();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String type, Interval interval)
  {
    return Collections.emptyList();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String key, String type, int limit)
  {
    return Collections.emptyList();
  }

  @Override
  public List<AuditEntry> fetchAuditHistory(String type, int limit)
  {
    return Collections.emptyList();
  }

  @Override
  public int removeAuditLogsOlderThan(long timestamp)
  {
    return 0;
  }
}
