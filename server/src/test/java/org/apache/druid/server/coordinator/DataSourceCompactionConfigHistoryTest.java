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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.TestDataSource;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceCompactionConfigHistoryTest
{
  private final AuditInfo auditInfo = new AuditInfo("author", "identity", "comment", "ip");
  private final DataSourceCompactionConfig wikiCompactionConfig
      = DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();

  private DataSourceCompactionConfigHistory wikiAuditHistory;

  @Before
  public void setup()
  {
    wikiAuditHistory = new DataSourceCompactionConfigHistory(TestDataSource.WIKI);
  }

  @Test
  public void testAddDatasourceConfigShouldAddToHistory()
  {
    final DateTime auditTime = DateTimes.nowUtc();
    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(wikiCompactionConfig),
        auditInfo,
        auditTime
    );

    Assert.assertEquals(1, wikiAuditHistory.getEntries().size());
    DataSourceCompactionConfigAuditEntry auditEntry = wikiAuditHistory.getEntries().get(0);
    Assert.assertEquals(wikiCompactionConfig, auditEntry.getCompactionConfig());
    Assert.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assert.assertEquals(auditTime, auditEntry.getAuditTime());
  }

  @Test
  public void testAddDeleteDatasourceConfigShouldAddBothToHistory()
  {
    final DateTime auditTime = DateTimes.nowUtc();
    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(wikiCompactionConfig),
        auditInfo,
        auditTime
    );
    wikiAuditHistory.add(DruidCompactionConfig.empty(), auditInfo, auditTime.plusHours(2));

    final List<DataSourceCompactionConfigAuditEntry> entries = wikiAuditHistory.getEntries();
    Assert.assertEquals(2, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    Assert.assertEquals(wikiCompactionConfig, firstEntry.getCompactionConfig());
    Assert.assertEquals(auditInfo, firstEntry.getAuditInfo());
    Assert.assertEquals(auditTime, firstEntry.getAuditTime());

    final DataSourceCompactionConfigAuditEntry secondEntry = entries.get(1);
    Assert.assertNull(secondEntry.getCompactionConfig());
    Assert.assertEquals(firstEntry.getGlobalConfig(), secondEntry.getGlobalConfig());
    Assert.assertEquals(auditInfo, secondEntry.getAuditInfo());
    Assert.assertEquals(auditTime.plusHours(2), secondEntry.getAuditTime());
  }

  @Test
  public void testAddDeleteAnotherDatasourceConfigShouldNotAddToHistory()
  {
    final DataSourceCompactionConfig koalaCompactionConfig
        = DataSourceCompactionConfig.builder().forDataSource(TestDataSource.KOALA).build();

    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(koalaCompactionConfig),
        auditInfo,
        DateTimes.nowUtc()
    );
    wikiAuditHistory.add(DruidCompactionConfig.empty(), auditInfo, DateTimes.nowUtc());

    Assert.assertTrue(wikiAuditHistory.getEntries().isEmpty());
  }

  @Test
  public void testAddDeleteAddDatasourceConfigShouldAddAllToHistory()
  {
    final DateTime auditTime = DateTimes.nowUtc();
    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(wikiCompactionConfig),
        auditInfo,
        auditTime
    );
    wikiAuditHistory.add(
        DruidCompactionConfig.empty(),
        auditInfo,
        auditTime.plusHours(2)
    );
    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(wikiCompactionConfig),
        auditInfo,
        auditTime.plusHours(3)
    );

    final List<DataSourceCompactionConfigAuditEntry> entries = wikiAuditHistory.getEntries();
    Assert.assertEquals(3, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    final DataSourceCompactionConfigAuditEntry thirdEntry = entries.get(2);
    Assert.assertTrue(firstEntry.hasSameConfig(thirdEntry));
  }

  @Test
  public void testAddModifyDatasourceConfigShouldAddBothToHistory()
  {
    final DateTime auditTime = DateTimes.nowUtc();
    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(wikiCompactionConfig),
        auditInfo,
        auditTime
    );


    final DataSourceCompactionConfig updatedWikiConfig
        = DataSourceCompactionConfig.builder()
                                    .forDataSource(TestDataSource.WIKI)
                                    .withSkipOffsetFromLatest(Period.hours(5))
                                    .build();
    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(updatedWikiConfig),
        auditInfo,
        auditTime.plusHours(3)
    );

    final List<DataSourceCompactionConfigAuditEntry> entries = wikiAuditHistory.getEntries();
    Assert.assertEquals(2, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    final DataSourceCompactionConfigAuditEntry secondEntry = entries.get(1);
    Assert.assertEquals(firstEntry.getGlobalConfig(), secondEntry.getGlobalConfig());

    Assert.assertEquals(wikiCompactionConfig, firstEntry.getCompactionConfig());
    Assert.assertEquals(updatedWikiConfig, secondEntry.getCompactionConfig());
    Assert.assertFalse(firstEntry.hasSameConfig(secondEntry));
  }

  @Test
  public void testAddAndModifyClusterConfigShouldAddTwice()
  {
    final DruidCompactionConfig originalConfig
        = DruidCompactionConfig.empty().withDatasourceConfig(wikiCompactionConfig);

    wikiAuditHistory.add(originalConfig, auditInfo, DateTimes.nowUtc());

    final DruidCompactionConfig updatedConfig = originalConfig.withClusterConfig(
        new ClusterCompactionConfig(0.2, null, null, null)
    );
    wikiAuditHistory.add(updatedConfig, auditInfo, DateTimes.nowUtc());

    final List<DataSourceCompactionConfigAuditEntry> entries = wikiAuditHistory.getEntries();
    Assert.assertEquals(2, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    final DataSourceCompactionConfigAuditEntry secondEntry = entries.get(1);
    Assert.assertEquals(secondEntry.getCompactionConfig(), firstEntry.getCompactionConfig());

    Assert.assertEquals(originalConfig.clusterConfig(), firstEntry.getGlobalConfig());
    Assert.assertEquals(updatedConfig.clusterConfig(), secondEntry.getGlobalConfig());
    Assert.assertFalse(firstEntry.hasSameConfig(secondEntry));
  }
}
