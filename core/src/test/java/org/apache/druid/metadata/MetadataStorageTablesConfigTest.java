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

package org.apache.druid.metadata;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MetadataStorageTablesConfigTest
{
  /**
   * Pretty lame test: mostly to get the static checks to not complain.
   */
  @Test
  public void testDefaults()
  {
    MetadataStorageTablesConfig config = MetadataStorageTablesConfig.fromBase(null);
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE, config.getBase());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_dataSource", config.getDataSourceTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_pendingSegments", config.getPendingSegmentsTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_segments", config.getSegmentsTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_rules", config.getRulesTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_config", config.getConfigTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_tasks", config.getTasksTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_tasklogs", config.getTaskLogTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_tasklocks", config.getTaskLockTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_audit", config.getAuditTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_supervisors", config.getSupervisorTable());
    assertEquals(MetadataStorageTablesConfig.DEFAULT_BASE + "_tableDefn", config.getTableDefnTable());
  }
}
