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

import com.google.common.collect.ImmutableList;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceCompactionConfigHistoryTest
{
  private static final String DATASOURCE = "DATASOURCE";
  private static final String DATASOURCE_2 = "DATASOURCE_2";
  private static final String DATASOURCE_NOT_EXISTS = "DATASOURCE_NOT_EXISTS";
  private static final double COMPACTION_TASK_SLOT_RATIO = 0.1;
  private static final int MAX_COMPACTION_TASK_SLOTS = 9;
  private static final boolean USE_AUTO_SCALE_SLOTS = false;
  private static final DateTime AUDIT_TIME = DateTimes.of(2023, 1, 13, 9, 0);
  private static final DateTime AUDIT_TIME_2 = DateTimes.of(2023, 1, 13, 9, 30);
  private static final DateTime AUDIT_TIME_3 = DateTimes.of(2023, 1, 13, 10, 0);

  @Mock
  private CoordinatorCompactionConfig compactionConfig;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private DataSourceCompactionConfig configForDataSource;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private DataSourceCompactionConfig configForDataSourceWithChange;
  @Mock(answer = Answers.RETURNS_MOCKS)
  private DataSourceCompactionConfig configForDataSource2;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AuditInfo auditInfo;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AuditInfo auditInfo2;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private AuditInfo auditInfo3;

  private DataSourceCompactionConfigHistory target;

  @Before
  public void setUp()
  {
    Mockito.when(compactionConfig.getCompactionTaskSlotRatio()).thenReturn(COMPACTION_TASK_SLOT_RATIO);
    Mockito.when(compactionConfig.getMaxCompactionTaskSlots()).thenReturn(MAX_COMPACTION_TASK_SLOTS);
    Mockito.when(compactionConfig.isUseAutoScaleSlots()).thenReturn(USE_AUTO_SCALE_SLOTS);
    Mockito.when(configForDataSource.getDataSource()).thenReturn(DATASOURCE);
    Mockito.when(configForDataSourceWithChange.getDataSource()).thenReturn(DATASOURCE);
    Mockito.when(configForDataSource2.getDataSource()).thenReturn(DATASOURCE_2);
    Mockito.when(compactionConfig.getCompactionConfigs())
           .thenReturn(ImmutableList.of(configForDataSource, configForDataSource2));
    target = new DataSourceCompactionConfigHistory(DATASOURCE);
  }

  @Test
  public void testAddCompactionConfigShouldAddToHistory()
  {
    target.add(compactionConfig, auditInfo, AUDIT_TIME);
    Assert.assertEquals(1, target.getHistory().size());
    DataSourceCompactionConfigAuditEntry auditEntry = target.getHistory().get(0);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME, auditEntry.getAuditTime());
  }

  @Test
  public void testAddAndDeleteCompactionConfigShouldAddBothToHistory()
  {
    target.add(compactionConfig, auditInfo, AUDIT_TIME);
    Mockito.when(compactionConfig.getCompactionConfigs()).thenReturn(ImmutableList.of(configForDataSource2));
    target.add(compactionConfig, auditInfo2, AUDIT_TIME_2);
    Assert.assertEquals(2, target.getHistory().size());
    DataSourceCompactionConfigAuditEntry auditEntry = target.getHistory().get(0);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME, auditEntry.getAuditTime());
    auditEntry = target.getHistory().get(1);
    Assert.assertEquals(null, auditEntry.getCompactionConfig());
    Assert.assertEquals(auditInfo2, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME_2, auditEntry.getAuditTime());
  }

  @Test
  public void testAddAndDeleteAnotherCompactionConfigShouldNotAddToHistory()
  {
    target.add(compactionConfig, auditInfo, AUDIT_TIME);
    Mockito.when(compactionConfig.getCompactionConfigs()).thenReturn(ImmutableList.of(configForDataSource));
    target.add(compactionConfig, auditInfo2, AUDIT_TIME_2);
    Assert.assertEquals(1, target.getHistory().size());
    DataSourceCompactionConfigAuditEntry auditEntry = target.getHistory().get(0);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME, auditEntry.getAuditTime());
  }

  @Test
  public void testAddDeletedAddCompactionConfigShouldAddAllToHistory()
  {
    target.add(compactionConfig, auditInfo, AUDIT_TIME);
    Mockito.when(compactionConfig.getCompactionConfigs()).thenReturn(ImmutableList.of(configForDataSource2));
    target.add(compactionConfig, auditInfo2, AUDIT_TIME_2);
    Mockito.when(compactionConfig.getCompactionConfigs())
           .thenReturn(ImmutableList.of(configForDataSourceWithChange, configForDataSource2));
    target.add(compactionConfig, auditInfo3, AUDIT_TIME_3);
    Assert.assertEquals(3, target.getHistory().size());
    DataSourceCompactionConfigAuditEntry auditEntry = target.getHistory().get(0);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME, auditEntry.getAuditTime());
    auditEntry = target.getHistory().get(2);
    Assert.assertEquals(configForDataSourceWithChange, auditEntry.getCompactionConfig());
    Assert.assertEquals(auditInfo3, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME_3, auditEntry.getAuditTime());
  }

  @Test
  public void testAddAndChangeCompactionConfigShouldAddBothToHistory()
  {
    target.add(compactionConfig, auditInfo, AUDIT_TIME);
    Mockito.when(compactionConfig.getCompactionConfigs()).thenReturn(ImmutableList.of(configForDataSourceWithChange));
    target.add(compactionConfig, auditInfo2, AUDIT_TIME_2);
    Assert.assertEquals(2, target.getHistory().size());
    DataSourceCompactionConfigAuditEntry auditEntry = target.getHistory().get(0);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME, auditEntry.getAuditTime());
    auditEntry = target.getHistory().get(1);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo2, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME_2, auditEntry.getAuditTime());
  }

  @Test
  public void testAddAndChangeGlobalSettingsShouldAddTwice()
  {
    target.add(compactionConfig, auditInfo, AUDIT_TIME);
    int newMaxTaskSlots = MAX_COMPACTION_TASK_SLOTS - 1;
    Mockito.when(compactionConfig.getMaxCompactionTaskSlots()).thenReturn(newMaxTaskSlots);
    target.add(compactionConfig, auditInfo2, AUDIT_TIME_2);
    Assert.assertEquals(2, target.getHistory().size());
    DataSourceCompactionConfigAuditEntry auditEntry = target.getHistory().get(0);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME, auditEntry.getAuditTime());
    Assert.assertEquals(MAX_COMPACTION_TASK_SLOTS, auditEntry.getGlobalConfig().getMaxCompactionTaskSlots());
    auditEntry = target.getHistory().get(1);
    Assert.assertEquals(DATASOURCE, auditEntry.getCompactionConfig().getDataSource());
    Assert.assertEquals(auditInfo2, auditEntry.getAuditInfo());
    Assert.assertEquals(AUDIT_TIME_2, auditEntry.getAuditTime());
    Assert.assertEquals(newMaxTaskSlots, auditEntry.getGlobalConfig().getMaxCompactionTaskSlots());
  }

  @Test
  public void testAddCompactionConfigDoesNotHaveDataSourceWithNoHistoryShouldNotAdd()
  {
    target = new DataSourceCompactionConfigHistory(DATASOURCE_NOT_EXISTS);
    target.add(compactionConfig, auditInfo, AUDIT_TIME);
    Assert.assertTrue(target.getHistory().isEmpty());
  }

}
