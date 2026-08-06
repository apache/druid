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

import org.apache.druid.audit.AuditManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class KillAuditLogTest
{
  @Mock
  private AuditManager mockAuditManager;

  @Mock
  private DruidCoordinatorRuntimeParams mockDruidCoordinatorRuntimeParams;

  private KillAuditLog killAuditLog;
  private CoordinatorRunStats runStats;

  @BeforeEach
  public void setup()
  {
    runStats = new CoordinatorRunStats();
    Mockito.when(mockDruidCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(runStats);
  }

  @Test
  public void testRunSkipIfLastRunLessThanPeriod()
  {
    final MetadataCleanupConfig config
        = new MetadataCleanupConfig(true, new Duration(Long.MAX_VALUE), new Duration("PT1S"));
    killAuditLog = new KillAuditLog(config, mockAuditManager);
    killAuditLog.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verifyNoInteractions(mockAuditManager);
  }

  @Test
  public void testRunNotSkipIfLastRunMoreThanPeriod()
  {
    final MetadataCleanupConfig config
        = new MetadataCleanupConfig(true, new Duration("PT6S"), new Duration("PT1S"));
    killAuditLog = new KillAuditLog(config, mockAuditManager);
    killAuditLog.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockAuditManager).removeAuditLogsOlderThan(ArgumentMatchers.anyLong());
    Assertions.assertTrue(runStats.hasStat(Stats.Kill.AUDIT_LOGS));
  }
}
