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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class DataSourceCompactionConfigHistoryTest
{
  private final AuditInfo auditInfo = new AuditInfo("author", "identity", "comment", "ip");
  private final DataSourceCompactionConfig wikiCompactionConfig
      = InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build();

  private DataSourceCompactionConfigHistory wikiAuditHistory;

  @BeforeEach
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

    Assertions.assertEquals(1, wikiAuditHistory.getEntries().size());
    DataSourceCompactionConfigAuditEntry auditEntry = wikiAuditHistory.getEntries().get(0);
    Assertions.assertEquals(wikiCompactionConfig, auditEntry.getCompactionConfig());
    Assertions.assertEquals(auditInfo, auditEntry.getAuditInfo());
    Assertions.assertEquals(auditTime, auditEntry.getAuditTime());
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
    Assertions.assertEquals(2, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    Assertions.assertEquals(wikiCompactionConfig, firstEntry.getCompactionConfig());
    Assertions.assertEquals(auditInfo, firstEntry.getAuditInfo());
    Assertions.assertEquals(auditTime, firstEntry.getAuditTime());

    final DataSourceCompactionConfigAuditEntry secondEntry = entries.get(1);
    Assertions.assertNull(secondEntry.getCompactionConfig());
    Assertions.assertEquals(firstEntry.getGlobalConfig(), secondEntry.getGlobalConfig());
    Assertions.assertEquals(auditInfo, secondEntry.getAuditInfo());
    Assertions.assertEquals(auditTime.plusHours(2), secondEntry.getAuditTime());
  }

  @Test
  public void testAddDeleteAnotherDatasourceConfigShouldNotAddToHistory()
  {
    final DataSourceCompactionConfig koalaCompactionConfig
        = InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.KOALA).build();

    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(koalaCompactionConfig),
        auditInfo,
        DateTimes.nowUtc()
    );
    wikiAuditHistory.add(DruidCompactionConfig.empty(), auditInfo, DateTimes.nowUtc());

    Assertions.assertTrue(wikiAuditHistory.getEntries().isEmpty());
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
    Assertions.assertEquals(3, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    final DataSourceCompactionConfigAuditEntry thirdEntry = entries.get(2);
    Assertions.assertTrue(firstEntry.hasSameConfig(thirdEntry));
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
        = InlineSchemaDataSourceCompactionConfig.builder()
                                                .forDataSource(TestDataSource.WIKI)
                                                .withSkipOffsetFromLatest(Period.hours(5))
                                                .build();
    wikiAuditHistory.add(
        DruidCompactionConfig.empty().withDatasourceConfig(updatedWikiConfig),
        auditInfo,
        auditTime.plusHours(3)
    );

    final List<DataSourceCompactionConfigAuditEntry> entries = wikiAuditHistory.getEntries();
    Assertions.assertEquals(2, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    final DataSourceCompactionConfigAuditEntry secondEntry = entries.get(1);
    Assertions.assertEquals(firstEntry.getGlobalConfig(), secondEntry.getGlobalConfig());

    Assertions.assertEquals(wikiCompactionConfig, firstEntry.getCompactionConfig());
    Assertions.assertEquals(updatedWikiConfig, secondEntry.getCompactionConfig());
    Assertions.assertFalse(firstEntry.hasSameConfig(secondEntry));
  }

  @Test
  public void testAddAndModifyClusterConfigShouldAddTwice()
  {
    final DruidCompactionConfig originalConfig
        = DruidCompactionConfig.empty().withDatasourceConfig(wikiCompactionConfig);

    wikiAuditHistory.add(originalConfig, auditInfo, DateTimes.nowUtc());

    final DruidCompactionConfig updatedConfig = originalConfig.withClusterConfig(
        new ClusterCompactionConfig(0.2, null, null, null, null, null)
    );
    wikiAuditHistory.add(updatedConfig, auditInfo, DateTimes.nowUtc());

    final List<DataSourceCompactionConfigAuditEntry> entries = wikiAuditHistory.getEntries();
    Assertions.assertEquals(2, entries.size());

    final DataSourceCompactionConfigAuditEntry firstEntry = entries.get(0);
    final DataSourceCompactionConfigAuditEntry secondEntry = entries.get(1);
    Assertions.assertEquals(secondEntry.getCompactionConfig(), firstEntry.getCompactionConfig());

    Assertions.assertEquals(originalConfig.clusterConfig(), firstEntry.getGlobalConfig());
    Assertions.assertEquals(updatedConfig.clusterConfig(), secondEntry.getGlobalConfig());
    Assertions.assertFalse(firstEntry.hasSameConfig(secondEntry));
  }
}
