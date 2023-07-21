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

import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KillSupervisorsCustomDutyTest
{
  @Mock
  private MetadataSupervisorManager mockMetadataSupervisorManager;

  @Mock
  private DruidCoordinatorRuntimeParams mockDruidCoordinatorRuntimeParams;

  @Mock
  private DruidCoordinatorConfig coordinatorConfig;

  private KillSupervisorsCustomDuty killSupervisors;

  @Before
  public void setup()
  {
    Mockito.when(coordinatorConfig.getCoordinatorMetadataStoreManagementPeriod())
           .thenReturn(new Duration(3600 * 1000));
  }

  @Test
  public void testConstructorFailIfRetainDurationNull()
  {
    final IllegalArgumentException exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> killSupervisors = new KillSupervisorsCustomDuty(null, mockMetadataSupervisorManager, coordinatorConfig)
    );
    Assert.assertEquals(
        "[KillSupervisorsCustomDuty.durationToRetain] must be 0 milliseconds or higher",
        exception.getMessage()
    );
  }

  @Test
  public void testConstructorFailIfRetainDurationInvalid()
  {
    final IllegalArgumentException exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> killSupervisors = new KillSupervisorsCustomDuty(
            new Duration("PT-1S"),
            mockMetadataSupervisorManager,
            coordinatorConfig
        )
    );
    Assert.assertEquals(
        "[KillSupervisorsCustomDuty.durationToRetain] must be 0 milliseconds or higher",
        exception.getMessage()
    );
  }

  @Test
  public void testConstructorSuccess()
  {
    killSupervisors = new KillSupervisorsCustomDuty(
        new Duration("PT1S"),
        mockMetadataSupervisorManager,
        coordinatorConfig
    );
    Assert.assertNotNull(killSupervisors);
  }

  @Test
  public void testRun()
  {
    final CoordinatorRunStats runStats = new CoordinatorRunStats();
    Mockito.when(mockDruidCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(runStats);
    killSupervisors = new KillSupervisorsCustomDuty(
        new Duration("PT1S"),
        mockMetadataSupervisorManager,
        coordinatorConfig
    );
    killSupervisors.run(mockDruidCoordinatorRuntimeParams);
    Mockito.verify(mockMetadataSupervisorManager).removeTerminatedSupervisorsOlderThan(ArgumentMatchers.anyLong());
    Assert.assertTrue(runStats.hasStat(Stats.Kill.SUPERVISOR_SPECS));
  }
}
