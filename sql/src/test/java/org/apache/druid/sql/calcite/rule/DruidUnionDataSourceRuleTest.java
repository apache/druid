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

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.rel.DruidJoinQueryRel;
import org.apache.druid.sql.calcite.rel.DruidOuterQueryRel;
import org.apache.druid.sql.calcite.rel.DruidQueryRel;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidRelsTest;
import org.apache.druid.sql.calcite.rel.DruidUnionDataSourceRel;
import org.apache.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DruidUnionDataSourceRuleTest
{
  private final DruidTable fooDruidTable = new DatasourceTable(
      new PhysicalDatasourceMetadata(
          new TableDataSource("foo"),
          RowSignature.builder()
                      .addTimeColumn()
                      .add("col1", ColumnType.STRING)
                      .add("col2", ColumnType.LONG)
                      .build(),
          false,
          false
      )
  );

  @Test
  public void test_isUnionArm_tableScan()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SCAN,
        fooDruidTable,
        null,
        null
    );

    Assertions.assertTrue(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_tableMapping()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockMappingProject(ImmutableList.of(2, 1), 3),
        null
    );

    Assertions.assertTrue(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_tableProject()
  {
    // A transforming projection is allowed: it becomes virtual columns on the arm's ProjectDataSource.
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockNonMappingProject(),
        null
    );

    Assertions.assertTrue(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_tableFilterPlusMapping()
  {
    // A filter is allowed whatever it reads: it becomes a FilteredDataSource, on top of the arm's ProjectDataSource if
    // it reads one of the projection's virtual columns.
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockMappingProject(ImmutableList.of(1), 3),
        DruidRelsTest.mockFilter()
    );

    Assertions.assertTrue(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_tableAggregate()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.AGGREGATE,
        fooDruidTable,
        null,
        null
    );

    Assertions.assertFalse(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_unionScan()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.SCAN,
        null,
        null,
        null
    );

    Assertions.assertTrue(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_unionWithFilter()
  {
    // Flattening this union into another would lose its filter.
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.WHERE_FILTER,
        null,
        null,
        null
    );

    Assertions.assertFalse(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_outerQuery()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidOuterQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    Assertions.assertFalse(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }

  @Test
  public void test_isUnionArm_join()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    Assertions.assertFalse(DruidUnionDataSourceRule.isUnionArm(druidRel, null));
  }
}
