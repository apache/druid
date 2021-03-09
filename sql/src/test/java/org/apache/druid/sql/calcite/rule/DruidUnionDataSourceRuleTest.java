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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.rel.core.Project;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidOuterQueryRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidRelsTest;
import org.apache.druid.sql.calcite.rel.DruidUnionDataSourceRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class DruidUnionDataSourceRuleTest
{
  private final DruidTable fooDruidTable = new DruidTable(
      new TableDataSource("foo"),
      RowSignature.builder()
                  .addTimeColumn()
                  .add("col1", ValueType.STRING)
                  .add("col2", ValueType.LONG)
                  .build(),
      false,
      false
  );

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableScan()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SCAN,
        fooDruidTable,
        null,
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableMapping()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockMappingProject(ImmutableList.of(1), 3),
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("col1")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableProject()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockNonMappingProject(),
        null
    );

    Assert.assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_tableFilterPlusMapping()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockMappingProject(ImmutableList.of(1), 3),
        DruidRelsTest.mockFilter()
    );

    Assert.assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_unionScan()
  {
    final DruidUnionDataSourceRel druidRel = DruidRelsTest.mockDruidRel(
        DruidUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(fooDruidTable.getRowSignature().getColumnNames()),
        PartialDruidQuery.Stage.SCAN,
        null,
        null,
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_unionMapping()
  {
    final Project project = DruidRelsTest.mockMappingProject(ImmutableList.of(2, 1), 3);
    final Mappings.TargetMapping mapping = project.getMapping();
    final String[] mappedColumnNames = new String[mapping.getTargetCount()];

    final List<String> columnNames = fooDruidTable.getRowSignature().getColumnNames();
    for (int i = 0; i < columnNames.size(); i++) {
      mappedColumnNames[mapping.getTargetOpt(i)] = columnNames.get(i);
    }

    final DruidUnionDataSourceRel druidRel = DruidRelsTest.mockDruidRel(
        DruidUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(Arrays.asList(mappedColumnNames)),
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("col2", "col1")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_unionProject()
  {
    final DruidUnionDataSourceRel druidRel = DruidRelsTest.mockDruidRel(
        DruidUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(fooDruidTable.getRowSignature().getColumnNames()),
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        DruidRelsTest.mockNonMappingProject(),
        null
    );

    Assert.assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_outerQuery()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidOuterQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    Assert.assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }

  @Test
  public void test_getColumnNamesIfTableOrUnion_join()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    Assert.assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel)
    );
  }
}
