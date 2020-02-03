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

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

public class DruidRelsTest
{
  @Test
  public void test_isScanOrMapping_scan()
  {
    final DruidRel<?> rel = mockDruidRel(DruidQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null);
    Assert.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assert.assertTrue(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_scanJoin()
  {
    final DruidRel<?> rel = mockDruidRel(DruidJoinQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null);
    Assert.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_scanQuery()
  {
    final DruidRel<?> rel = mockDruidRel(DruidOuterQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null);
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_mapping()
  {
    final Project project = mockProject(true);
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        project,
        null
    );
    Assert.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assert.assertTrue(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_mappingJoin()
  {
    final Project project = mockProject(true);
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        project,
        null
    );
    Assert.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMapping()
  {
    final Project project = mockProject(false);
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        project,
        null
    );
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMappingJoin()
  {
    final Project project = mockProject(false);
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        project,
        null
    );
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProject()
  {
    final Project project = mockProject(true);
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        project,
        mockFilter()
    );
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProjectJoin()
  {
    final Project project = mockProject(true);
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        project,
        mockFilter()
    );
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filter()
  {
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.WHERE_FILTER,
        null,
        mockFilter()
    );
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_filterJoin()
  {
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.WHERE_FILTER,
        null,
        mockFilter()
    );
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assert.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_allStages()
  {
    final ImmutableSet<PartialDruidQuery.Stage> okStages = ImmutableSet.of(
        PartialDruidQuery.Stage.SCAN,
        PartialDruidQuery.Stage.SELECT_PROJECT
    );

    for (PartialDruidQuery.Stage stage : PartialDruidQuery.Stage.values()) {
      final Project project = mockProject(true);
      final DruidRel<?> rel = mockDruidRel(
          DruidQueryRel.class,
          stage,
          project,
          null
      );

      Assert.assertEquals(stage.toString(), okStages.contains(stage), DruidRels.isScanOrMapping(rel, true));
      Assert.assertEquals(stage.toString(), okStages.contains(stage), DruidRels.isScanOrMapping(rel, false));

      EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
    }
  }

  private static DruidRel<?> mockDruidRel(
      final Class<? extends DruidRel<?>> clazz,
      final PartialDruidQuery.Stage stage,
      @Nullable Project selectProject,
      @Nullable Filter whereFilter
  )
  {
    // DruidQueryRels rely on a ton of Calcite stuff like RelOptCluster, RelOptTable, etc, which is quite verbose to
    // create real instances of. So, tragically, we'll use EasyMock.
    final DruidRel<?> mockRel = EasyMock.mock(clazz);
    final PartialDruidQuery mockPartialQuery = EasyMock.mock(PartialDruidQuery.class);
    EasyMock.expect(mockPartialQuery.stage()).andReturn(stage).anyTimes();
    EasyMock.expect(mockPartialQuery.getSelectProject()).andReturn(selectProject).anyTimes();
    EasyMock.expect(mockPartialQuery.getWhereFilter()).andReturn(whereFilter).anyTimes();
    EasyMock.expect(mockRel.getPartialDruidQuery()).andReturn(mockPartialQuery).anyTimes();
    EasyMock.replay(mockRel, mockPartialQuery);
    return mockRel;
  }

  private static Project mockProject(final boolean mapping)
  {
    final Project mockProject = EasyMock.mock(Project.class);
    EasyMock.expect(mockProject.isMapping()).andReturn(mapping).anyTimes();
    EasyMock.replay(mockProject);
    return mockProject;
  }

  private static Filter mockFilter()
  {
    return EasyMock.mock(Filter.class);
  }
}
