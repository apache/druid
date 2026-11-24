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
import org.apache.druid.error.DruidException;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.ProjectDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Tests for {@link DruidRels}.
 */
public class DruidRelsTest
{
  private static final TableDataSource FOO = new TableDataSource("foo");

  private static final RowSignature FOO_SIGNATURE =
      RowSignature.builder().add("dim1", ColumnType.STRING).add("dim2", ColumnType.STRING).build();

  private static final ExpressionVirtualColumn PLUS_ONE =
      new ExpressionVirtualColumn("v0", "dim1 + 1", ColumnType.LONG, ExprMacroTable.nil());

  @Test
  public void test_computeUnionColumnNames_armsAgree()
  {
    Assertions.assertEquals(
        List.of("__time", "dim1"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("__time", "dim1"), List.of("__time", "dim1")),
            List.of(Set.of(), Set.of())
        )
    );
  }

  @Test
  public void test_computeUnionColumnNames_firstArmNameFree()
  {
    // The second arm presents its "dim4" as "v0", which it uses for nothing else.
    Assertions.assertEquals(
        List.of("dim1", "v0"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("dim1", "v0"), List.of("dim1", "dim4")),
            List.of(Set.of("v0"), Set.of())
        )
    );
  }

  @Test
  public void test_computeUnionColumnNames_firstArmNameReadInsideOtherArm()
  {
    // The second arm's "v0" reads its own "m1", which presenting "v0" as "m1" would replace.
    Assertions.assertEquals(
        List.of("dim1", "u1"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("dim1", "m1"), List.of("dim1", "v0")),
            List.of(Set.of(), Set.of("v0", "m1"))
        )
    );
  }

  @Test
  public void test_computeUnionColumnNames_firstArmRepeatsName()
  {
    // The second arm presents different columns where the first arm repeats "a", so only one can be named "a".
    Assertions.assertEquals(
        List.of("a", "u1"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("a", "a"), List.of("b", "c")),
            List.of(Set.of(), Set.of())
        )
    );
  }

  @Test
  public void test_computeUnionColumnNames_allArmsRepeatName()
  {
    Assertions.assertEquals(
        List.of("a", "a"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("a", "a"), List.of("a", "a")),
            List.of(Set.of(), Set.of())
        )
    );
  }

  @Test
  public void test_computeUnionColumnNames_swap()
  {
    Assertions.assertEquals(
        List.of("u0", "u1"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("dim1", "dim2"), List.of("dim2", "dim1")),
            List.of(Set.of(), Set.of())
        )
    );
  }

  @Test
  public void test_computeUnionColumnNames_timeFromOtherColumn()
  {
    // Only an arm's own time column can be presented as "__time".
    Assertions.assertEquals(
        List.of("u0", "dim1"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("__time", "dim1"), List.of("t", "dim1")),
            List.of(Set.of(), Set.of())
        )
    );
  }

  @Test
  public void test_computeUnionColumnNames_freshPrefixUnused()
  {
    // The second arm reads "a", so the column needs a fresh name, and "u0" is taken too.
    Assertions.assertEquals(
        List.of("_u0"),
        DruidRels.computeUnionColumnNames(
            List.of(List.of("a"), List.of("b")),
            List.of(Set.of(), Set.of("a", "u0"))
        )
    );
  }

  @Test
  public void test_createMappingDataSource_renames()
  {
    Assertions.assertEquals(
        new ProjectDataSource(
            FOO,
            VirtualColumns.create(new ExpressionVirtualColumn("out", Parser.identifier("dim1"), ColumnType.STRING))
        ),
        DruidRels.createMappingDataSource(FOO, FOO_SIGNATURE, List.of("out", "dim2"))
    );
  }

  @Test
  public void test_createMappingDataSource_unknownType()
  {
    Assertions.assertEquals(
        new ProjectDataSource(
            FOO,
            VirtualColumns.create(new ExpressionVirtualColumn("out", Parser.identifier("dim1"), null))
        ),
        DruidRels.createMappingDataSource(FOO, RowSignature.builder().add("dim1", null).build(), List.of("out"))
    );
  }

  @Test
  public void test_createMappingDataSource_nothingRenamed()
  {
    Assertions.assertSame(FOO, DruidRels.createMappingDataSource(FOO, FOO_SIGNATURE, List.of("dim1", "dim2")));
  }

  @Test
  public void test_createMappingDataSource_composes()
  {
    final ProjectDataSource inner = new ProjectDataSource(FOO, VirtualColumns.create(PLUS_ONE));

    Assertions.assertEquals(
        new ProjectDataSource(
            FOO,
            VirtualColumns.create(
                PLUS_ONE,
                new ExpressionVirtualColumn("u1", Parser.identifier("dim2"), ColumnType.STRING)
            )
        ),
        DruidRels.createMappingDataSource(
            inner,
            RowSignature.builder().add("v0", ColumnType.LONG).add("dim2", ColumnType.STRING).build(),
            List.of("v0", "u1")
        )
    );
  }

  @Test
  public void test_createMappingDataSource_readsInnerVirtualColumn()
  {
    final ProjectDataSource inner = new ProjectDataSource(FOO, VirtualColumns.create(PLUS_ONE));

    Assertions.assertEquals(
        new ProjectDataSource(
            FOO,
            VirtualColumns.create(
                PLUS_ONE,
                new ExpressionVirtualColumn("u0", Parser.identifier("v0"), ColumnType.LONG)
            )
        ),
        DruidRels.createMappingDataSource(
            inner,
            RowSignature.builder().add("v0", ColumnType.LONG).add("dim2", ColumnType.STRING).build(),
            List.of("u0", "dim2")
        )
    );
  }

  @Test
  public void test_createMappingDataSource_swap()
  {
    // Each new name is the name the other column reads, so neither can be a virtual column.
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> DruidRels.createMappingDataSource(
            FOO,
            RowSignature.builder().add("dim2", ColumnType.STRING).add("dim1", ColumnType.STRING).build(),
            List.of("dim1", "dim2")
        )
    );
    Assertions.assertTrue(e.getMessage().startsWith("Cannot present a column as["), e.getMessage());
  }

  @Test
  public void test_createMappingDataSource_newNameReadByInnerVirtualColumn()
  {
    // "v0" reads the base's "dim1", which presenting "dim2" as "dim1" would replace.
    final ProjectDataSource inner = new ProjectDataSource(FOO, VirtualColumns.create(PLUS_ONE));
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> DruidRels.createMappingDataSource(
            inner,
            RowSignature.builder().add("v0", ColumnType.LONG).add("dim2", ColumnType.STRING).build(),
            List.of("x", "dim1")
        )
    );
    Assertions.assertEquals(
        "Cannot present a column as[dim1], since virtual column[v0] reads a different column of that name",
        e.getMessage()
    );
  }

  @Test
  public void test_createMappingDataSource_timeFromOtherColumn()
  {
    // A virtual column cannot take the name of the time column, so it cannot be presented from anything else.
    Assertions.assertThrows(
        RuntimeException.class,
        () -> DruidRels.createMappingDataSource(
            FOO,
            RowSignature.builder().add("t", ColumnType.LONG).build(),
            List.of(ColumnHolder.TIME_COLUMN_NAME)
        )
    );
  }

  @Test
  public void test_isScanOrMapping_scan()
  {
    final DruidRel<?> rel = mockDruidRel(DruidQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_scanJoin()
  {
    final DruidRel<?> rel = mockDruidRel(DruidJoinQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_scanUnion()
  {
    final DruidRel<?> rel = mockDruidRel(DruidUnionDataSourceRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_scanQuery()
  {
    final DruidRel<?> rel = mockDruidRel(DruidOuterQueryRel.class, PartialDruidQuery.Stage.SCAN, null, null, null);
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));
    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_mapping()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_mappingJoin()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_mappingUnion()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assertions.assertTrue(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_mappingQuery()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidOuterQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMapping()
  {
    final Project project = mockNonMappingProject();
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMappingJoin()
  {
    final Project project = mockNonMappingProject();
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_nonMappingUnion()
  {
    final Project project = mockNonMappingProject();
    final DruidRel<?> rel = mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        null
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProject()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProjectJoin()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filterThenProjectUnion()
  {
    final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
    final DruidRel<?> rel = mockDruidRel(
        DruidUnionDataSourceRel.class,
        PartialDruidQuery.Stage.SELECT_PROJECT,
        null,
        project,
        mockFilter()
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery(), project);
  }

  @Test
  public void test_isScanOrMapping_filter()
  {
    final DruidRel<?> rel = mockDruidRel(
        DruidQueryRel.class,
        PartialDruidQuery.Stage.WHERE_FILTER,
        null,
        null,
        mockFilter()
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

    EasyMock.verify(rel, rel.getPartialDruidQuery());
  }

  @Test
  public void test_isScanOrMapping_filterJoin()
  {
    final DruidRel<?> rel = mockDruidRel(
        DruidJoinQueryRel.class,
        PartialDruidQuery.Stage.WHERE_FILTER,
        null,
        null,
        mockFilter()
    );
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, true));
    Assertions.assertFalse(DruidRels.isScanOrMapping(rel, false));

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
      final Project project = mockMappingProject(ImmutableList.of(1, 0), 2);
      final DruidRel<?> rel = mockDruidRel(
          DruidQueryRel.class,
          stage,
          null,
          project,
          null
      );

      Assertions.assertEquals(okStages.contains(stage), DruidRels.isScanOrMapping(rel, true), stage.toString());
      Assertions.assertEquals(okStages.contains(stage), DruidRels.isScanOrMapping(rel, false), stage.toString());

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
