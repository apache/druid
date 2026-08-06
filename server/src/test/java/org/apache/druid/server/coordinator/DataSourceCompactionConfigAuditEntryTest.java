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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.WARN)
public class DataSourceCompactionConfigAuditEntryTest
{
  private final AuditInfo auditInfo = new AuditInfo("author", "identity", "comment", "ip");
  
  private final DataSourceCompactionConfigAuditEntry firstEntry = new DataSourceCompactionConfigAuditEntry(
      new ClusterCompactionConfig(0.1, 9, null, null, null, null),
      InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
      auditInfo,
      DateTimes.nowUtc()
  );

  @Test
  public void testhasSameConfigWithSameBaseConfigIsTrue()
  {
    final DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 9, null, null, null, null),
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assertions.assertTrue(firstEntry.hasSameConfig(secondEntry));
    Assertions.assertTrue(secondEntry.hasSameConfig(firstEntry));
  }

  @Test
  public void testhasSameConfigWithDifferentClusterConfigIsFalse()
  {
    DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.2, 9, null, null, null, null),
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assertions.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assertions.assertFalse(secondEntry.hasSameConfig(firstEntry));

    secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 10, null, null, null, null),
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.WIKI).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assertions.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assertions.assertFalse(secondEntry.hasSameConfig(firstEntry));
  }

  @Test
  public void testhasSameConfigWithDifferentDatasourceConfigIsFalse()
  {
    DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 9, null, null, null, null),
        InlineSchemaDataSourceCompactionConfig.builder().forDataSource(TestDataSource.KOALA).build(),
        auditInfo,
        DateTimes.nowUtc()
    );
    Assertions.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assertions.assertFalse(secondEntry.hasSameConfig(firstEntry));
  }

  @Test
  public void testhasSameConfigWithNullDatasourceConfigIsFalse()
  {
    final DataSourceCompactionConfigAuditEntry secondEntry = new DataSourceCompactionConfigAuditEntry(
        new ClusterCompactionConfig(0.1, 9, null, null, null, null),
        null,
        auditInfo,
        DateTimes.nowUtc()
    );
    Assertions.assertFalse(firstEntry.hasSameConfig(secondEntry));
    Assertions.assertFalse(secondEntry.hasSameConfig(firstEntry));
  }
}
