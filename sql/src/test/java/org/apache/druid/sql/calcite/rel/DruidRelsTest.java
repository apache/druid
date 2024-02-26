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

package org.apache.druid.sql.calcite.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DruidRelsTest
{
  @Test
  void is_scan_or_mapping_scan()
  {
    final DruidRel<?> rel = mockDruidRel(DruidQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    assertTrue(DruidRels.isScanOrMapping(rel, true));
    assertTrue(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  void is_scan_or_mapping_scan_join()
  {
    final DruidRel<?> rel = mockDruidRel(DruidJoinQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    assertTrue(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  void is_scan_or_mapping_scan_union()
  {
    final DruidRel<?> rel = mockDruidRel(DruidUnionDataSourceRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    assertTrue(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  void is_scan_or_mapping_scan_query()
  {
    final DruidRel<?> rel = mockDruidRel(DruidOuterQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  void is_scan_or_mapping_mapping()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    assertTrue(DruidRels.isScanOrMapping(rel, true));
    assertTrue(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_mapping_join()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    assertTrue(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_mapping_union()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    assertTrue(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_mapping_query()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidOuterQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_non_mapping()
  {
    final Project project = mockNonMappingProject();
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_non_mapping_join()
  {
    final Project project = mockNonMappingProject();
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_non_mapping_union()
  {
    final Project project = mockNonMappingProject();
    final DruidRel<?> rel = mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_filter_then_project()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_filter_then_project_join()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_filter_then_project_union()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  void is_scan_or_mapping_filter()
  {
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.WHERE_FILTER,
        null,
        null,
        mockFilter()
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  void is_scan_or_mapping_filter_join()
  {
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.WHERE_FILTER,
        null,
        null,
        mockFilter()
    );
    assertFalse(DruidRels.isScanOrMapping(rel, true));
    assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  void is_scan_or_mapping_all_stages()
  {
    final ImmutableSet<PartialDruidQuery.Stage> okStages = ImmutableSet.of(
        PartialDruidQuery.Stage.SCAN,
        PartialDruidQuery.Stage.SELECT_PROJECT
    );

    for (PartialDruidQuery.Stage stage : PartialDruidQuery.Stage.values()) {
      final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
      final DruidRel<?> rel = mockDruidRel(
          DruidQueryRel.class,
          stage,
          null,
          project,
          null
      );

      assertEquals(okStages.contains(stage), DruidRels.isScanOrMapping(rel, true), stage.toString());
      assertEquals(okStages.contains(stage), DruidRels.isScanOrMapping(rel, false), stage.toString());

      EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
    }
  }

  public static DruidRel<?> mockDruidRel(
      final Class<? extends DruidRel<?>> clazz,
      final PartialDruidQuery.Stage stage,
      @Nullable DruidTable druidTable,
      @Nullable Project selectProject,
      @Nullable Filter whereFilter
  )
  {
    return mockDruidRel(clazz, rel -> {}, stage, druidTable, selectProject, whereFilter);
  }

  public static <T extends DruidRel<?>> T mockDruidRel(
      final Class<T> clazz,
      final Consumer<T> additionalExpectationsFunction,
      final PartialDruidQuery.Stage stage,
      @Nullable DruidTable druidTable,
      @Nullable Project selectProject,
      @Nullable Filter whereFilter
  )
  {
    // DruidQueryRels rely on a ton of Calcite stuff like RelOptCluster, RelOptTable, etc, which is quite verbose to
    // create real instances of. So, tragically, we'll use EasyMock.
    final PartialDruidQuery mockPartialQuery = EasyMock.mock(PartialDruidQuery.class);
    EasyMock.expect(mockPartialQuery.stage()).andReturn(stage).anyTimes();
    EasyMock.expect(mockPartialQuery.getSelectProject()).andReturn(selectProject).anyTimes();
    EasyMock.expect(mockPartialQuery.getWhereFilter()).andReturn(whereFilter).anyTimes();

    final RelOptTable mockRelOptTable = EasyMock.mock(RelOptTable.class);

    final T mockRel = EasyMock.mock(clazz);
    EasyMock.expect(mockRel.getPartialDruidQuery()).andReturn(mockPartialQuery).anyTimes();
    EasyMock.expect(mockRel.getTable()).andReturn(mockRelOptTable).anyTimes();
    if (clazz == DruidQueryRel.class) {
      EasyMock.expect(((DruidQueryRel) mockRel).getDruidTable()).andReturn(druidTable).anyTimes();
    }
    additionalExpectationsFunction.accept(mockRel);

    EasyMock.replay(mockRel, mockPartialQuery, mockRelOptTable);
    return mockRel;
  }

  public static Project mockMappingProject(final List<Integer> sources, final int sourceCount)
  {
    final Project mockProject = EasyMock.mock(Project.class);
    EasyMock.expect(mockProject.isMapping()).andReturn(true).anyTimes();

    final Mappings.PartialMapping mapping = new Mappings.PartialMapping(sources, sourceCount, MappingType.SURJECTION);

    EasyMock.expect(mockProject.getMapping()).andReturn(mapping).anyTimes();
    EasyMock.replay(mockProject);
    return mockProject;
  }

  public static Project mockNonMappingProject()
  {
    final Project mockProject = EasyMock.mock(Project.class);
    EasyMock.expect(mockProject.isMapping()).andReturn(false).anyTimes();
    EasyMock.replay(mockProject);
    return mockProject;
  }

  public static Filter mockFilter()
  {
    return EasyMock.mock(Filter.class);
  }
}
