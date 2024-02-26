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
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.util.mapping.Mappings;
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
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DruidUnionDataSourceRuleTest
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
  void get_column_names_if_table_or_union_table_scan()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SCAN,
        fooDruidTable,
        null,
        null
    );

    assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_table_mapping()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockMappingProject(ImmutableList.of(1), 3),
        null
    );

    assertEquals(
        Optional.of(ImmutableList.of("col1")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_table_project()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockNonMappingProject(),
        null
    );

    assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_table_filter_plus_mapping()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        fooDruidTable,
        DruidRelsTest.mockMappingProject(ImmutableList.of(1), 3),
        DruidRelsTest.mockFilter()
    );

    assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_union_scan()
  {
    final DruidUnionDataSourceRel druidRel = DruidRelsTest.mockDruidRel(
        DruidUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(fooDruidTable.getRowSignature().getColumnNames()),
        PartialDruidQuery.Stage.SCAN,
        null,
        null,
        null
    );

    assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_union_mapping()
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

    assertEquals(
        Optional.of(ImmutableList.of("col2", "col1")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_union_project()
  {
    final DruidUnionDataSourceRel druidRel = DruidRelsTest.mockDruidRel(
        DruidUnionDataSourceRel.class,
        rel -> EasyMock.expect(rel.getUnionColumnNames()).andReturn(fooDruidTable.getRowSignature().getColumnNames()),
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        DruidRelsTest.mockNonMappingProject(),
        null
    );

    assertEquals(
        Optional.of(ImmutableList.of("__time", "col1", "col2")),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_outer_query()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidOuterQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }

  @Test
  void get_column_names_if_table_or_union_join()
  {
    final DruidRel<?> druidRel = DruidRelsTest.mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        null,
        null
    );

    assertEquals(
        Optional.empty(),
        DruidUnionDataSourceRule.getColumnNamesIfTableOrUnion(druidRel, null)
    );
  }
}
