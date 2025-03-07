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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DataSourceCompactionConfigAuditEntryTest
{
  private final AuditInfo auditInfo = new AuditInfo("author", "identity", "comment", "ip");
  
  private final DataSourceCompactionConfigAuditEntry firstEntry = new DataSourceCompactionConfigAuditEntry(
      new ClusterCompactionConfig(0.1, 9, true, null),
      DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
      auditInfo,
      DateTimes.nowUtc()
  );

  @Test
  public void testhasSameConfigWithSameBaseConfigIsTrue()
  {
    final DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 9, true, null),
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assert.assertTrue(firstEntry.hasSameConfig(secondEntry));
    Assert.assertTrue(secondEntry.hasSameConfig(firstEntry));
  }

  @Test
  public void testhasSameConfigWithDifferentClusterConfigIsFalse()
  {
    DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.2, 9, false, null),
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assert.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assert.assertFalse(secondEntry.hasSameConfig(firstEntry));

    secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 10, true, null),
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assert.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assert.assertFalse(secondEntry.hasSameConfig(firstEntry));
  }

  @Test
  public void testhasSameConfigWithDifferentDatasourceConfigIsFalse()
  {
    DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 9, true, null),
        DataSourceCompactionConfig.builder().forDataSource(TestDataSource.KOALA).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assert.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assert.assertFalse(secondEntry.hasSameConfig(firstEntry));
  }

  @Test
  public void testhasSameConfigWithNullDatasourceConfigIsFalse()
  {
    final DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 9, true, null),
        null,
        auditInfo,
        DateTimes.nowUtc()
    );
    Assert.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assert.assertFalse(secondEntry.hasSameConfig(firstEntry));
  }
}
