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

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.schema.Schema;
import org.apache.druid.query.SystemTableDataSource;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.run.NativeSqlEngine;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SystemTableDataProviderTest
{
  @Test
  public void testNativeTablesExposeSystemMetadata()
  {
    final NativeTasksTable tasks = new NativeTasksTable();
    Assertions.assertEquals("tasks", ((SystemTableDataSource) tasks.getDataSource()).getTable());
    Assertions.assertFalse(tasks.isJoinable());
    Assertions.assertFalse(tasks.isBroadcast());
    Assertions.assertEquals(Schema.TableType.SYSTEM_TABLE, tasks.getJdbcTableType());

    final NativeServerPropertiesTable serverProperties = new NativeServerPropertiesTable();
    Assertions.assertEquals(
        "server_properties",
        ((SystemTableDataSource) serverProperties.getDataSource()).getTable()
    );
    Assertions.assertFalse(serverProperties.isJoinable());
    Assertions.assertFalse(serverProperties.isBroadcast());
    Assertions.assertEquals(Schema.TableType.SYSTEM_TABLE, serverProperties.getJdbcTableType());
  }

  @Test
  public void testSystemSchemaSelectsNativeTablesOnlyWhenEnabledForNativeEngine()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    final SqlEngine nativeEngine = EasyMock.createMock(SqlEngine.class);
    final SqlEngine bindableEngine = EasyMock.createMock(SqlEngine.class);
    final PlannerContext disabledContext = EasyMock.createMock(PlannerContext.class);
    final PlannerContext bindableContext = EasyMock.createMock(PlannerContext.class);
    final PlannerContext enabledContext = EasyMock.createMock(PlannerContext.class);
    EasyMock.expect(nativeEngine.name()).andReturn(NativeSqlEngine.NAME).anyTimes();
    EasyMock.expect(bindableEngine.name()).andReturn("bindable").anyTimes();
    EasyMock.expect(disabledContext.useNativeQueryForSystemTables()).andReturn(false).once();
    EasyMock.expect(bindableContext.useNativeQueryForSystemTables()).andReturn(true).once();
    EasyMock.expect(bindableContext.getEngine()).andReturn(bindableEngine).once();
    EasyMock.expect(enabledContext.useNativeQueryForSystemTables()).andReturn(true).once();
    EasyMock.expect(enabledContext.getEngine()).andReturn(nativeEngine).once();
    EasyMock.expect(table.unwrap(NativeSystemTable.class)).andReturn(NativeTasksTable::new).once();
    EasyMock.replay(table, nativeEngine, bindableEngine, disabledContext, bindableContext, enabledContext);

    Assertions.assertFalse(SystemSchema.canUseNativeSystemTable(table, disabledContext));
    Assertions.assertFalse(SystemSchema.canUseNativeSystemTable(table, bindableContext));
    Assertions.assertTrue(SystemSchema.canUseNativeSystemTable(table, enabledContext));

    EasyMock.verify(table, nativeEngine, bindableEngine, disabledContext, bindableContext, enabledContext);
  }

  @Test
  public void testSystemSchemaGetsNativeRepresentation()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    EasyMock.expect(table.unwrap(NativeSystemTable.class)).andReturn(NativeTasksTable::new).once();
    EasyMock.replay(table);

    Assertions.assertInstanceOf(NativeTasksTable.class, SystemSchema.getNativeSystemTable(table));
    EasyMock.verify(table);
  }

  @Test
  public void testSystemSchemaRejectsTableWithoutNativeCapability()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    EasyMock.expect(table.unwrap(NativeSystemTable.class)).andReturn(null).once();
    EasyMock.replay(table);

    Assertions.assertNull(SystemSchema.getNativeSystemTable(table));
    EasyMock.verify(table);
  }

  @Test
  public void testQualifiedNameAloneDoesNotEnableNativeExecution()
  {
    final RelOptTable table = EasyMock.createMock(RelOptTable.class);
    EasyMock.expect(table.getQualifiedName()).andStubReturn(List.of("sys", "tasks"));
    EasyMock.expect(table.unwrap(NativeSystemTable.class)).andReturn(null).once();
    EasyMock.replay(table);

    Assertions.assertNull(SystemSchema.getNativeSystemTable(table));
    EasyMock.verify(table);
  }
}
