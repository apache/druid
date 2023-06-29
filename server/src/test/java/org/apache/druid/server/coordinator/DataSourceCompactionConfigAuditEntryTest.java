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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceCompactionConfigAuditEntryTest
{
  private static final double COMPACTION_TASK_SLOT_RATIO = 0.1;
  private static final int MAX_COMPACTION_SLOTS = 9;
  private static final boolean USE_AUTO_SCALE_SLOTS = true;

  @Mock
  private CoordinatorCompactionConfig coordinatorCompactionConfig;

  @Before
  public void setUp()
  {
    Mockito.when(coordinatorCompactionConfig.getCompactionTaskSlotRatio()).thenReturn(COMPACTION_TASK_SLOT_RATIO);
    Mockito.when(coordinatorCompactionConfig.getMaxCompactionTaskSlots()).thenReturn(MAX_COMPACTION_SLOTS);
    Mockito.when(coordinatorCompactionConfig.isUseAutoScaleSlots()).thenReturn(USE_AUTO_SCALE_SLOTS);
  }

  @Test
  public void testhasSameConfigWithSameBaseConfigShouldReturnTrue()
  {
    DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig config =
        new DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig(
            COMPACTION_TASK_SLOT_RATIO,
            MAX_COMPACTION_SLOTS,
            USE_AUTO_SCALE_SLOTS
        );

    Assert.assertTrue(config.hasSameConfig(coordinatorCompactionConfig));
  }

  @Test
  public void testhasSameConfigWithDifferentUseAutoScaleSlotsShouldReturnFalse()
  {
    DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig config =
        new DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig(
            COMPACTION_TASK_SLOT_RATIO,
            MAX_COMPACTION_SLOTS,
            !USE_AUTO_SCALE_SLOTS
        );

    Assert.assertFalse(config.hasSameConfig(coordinatorCompactionConfig));
  }

  @Test
  public void testhasSameConfigWithDifferentMaxCompactionSlotsShouldReturnFalse()
  {
    DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig config =
        new DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig(
            COMPACTION_TASK_SLOT_RATIO,
            MAX_COMPACTION_SLOTS + 1,
            USE_AUTO_SCALE_SLOTS
        );

    Assert.assertFalse(config.hasSameConfig(coordinatorCompactionConfig));
  }

  @Test
  public void testhasSameConfigWithDifferentCompactionSlotRatioShouldReturnFalse()
  {
    DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig config =
        new DataSourceCompactionConfigAuditEntry.GlobalCompactionConfig(
            COMPACTION_TASK_SLOT_RATIO - 0.03,
            MAX_COMPACTION_SLOTS,
            USE_AUTO_SCALE_SLOTS
        );

    Assert.assertFalse(config.hasSameConfig(coordinatorCompactionConfig));
  }
}
