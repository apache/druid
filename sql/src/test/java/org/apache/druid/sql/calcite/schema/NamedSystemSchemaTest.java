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

package org.apache.druid.sql.calcite.schema;

import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.easymock.EasyMock;
import org.easymock.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class NamedSystemSchemaTest extends CalciteTestBase
{
  private static final String SCHEMA_NAME = "sys";

  @Mock
  private SystemSchema systemSchema;

  private PlannerConfig plannerConfig;

  private NamedSystemSchema target;

  @BeforeEach
  void setUp()
  {
    plannerConfig = EasyMock.createMock(PlannerConfig.class);
    target = new NamedSystemSchema(plannerConfig, systemSchema);
  }

  @Test
  void getSchemaNameShouldReturnName()
  {
    assertEquals(SCHEMA_NAME, target.getSchemaName());
  }

  @Test
  void getSchemaShouldReturnSchema()
  {
    assertEquals(systemSchema, target.getSchema());
  }

  @Test
  void resourceTypeAuthDisabled()
  {
    EasyMock.expect(plannerConfig.isAuthorizeSystemTablesDirectly()).andReturn(false).once();
    EasyMock.replay(plannerConfig);
    assertNull(target.getSchemaResourceType("servers"));
    EasyMock.verify(plannerConfig);
  }

  @Test
  void resourceTypeAuthEnabled()
  {
    EasyMock.expect(plannerConfig.isAuthorizeSystemTablesDirectly()).andReturn(true).once();
    EasyMock.replay(plannerConfig);
    assertEquals(ResourceType.SYSTEM_TABLE, target.getSchemaResourceType("servers"));
    EasyMock.verify(plannerConfig);
  }
}
