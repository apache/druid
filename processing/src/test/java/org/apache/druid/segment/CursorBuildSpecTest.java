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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class CursorBuildSpecTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(CursorBuildSpec.class).usingGetClass().verify();
  }

  @Test
  public void testIsCompatibleOrdering()
  {
    // test specified preferred ordering by the query
    CursorBuildSpec spec1 = CursorBuildSpec.builder()
                                           .setPhysicalColumns(ImmutableSet.of(ColumnHolder.TIME_COLUMN_NAME, "x", "y"))
                                           .setPreferredOrdering(
                                               ImmutableList.of(
                                                   OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
                                                   OrderBy.ascending("x")
                                               )
                                           )
                                           .build();
    // fail if cursor isn't fully ordered by the preferred ordering of the spec
    Assertions.assertFalse(spec1.isCompatibleOrdering(ImmutableList.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME))));
    // pass if the cursor ordering exactly matches
    Assertions.assertTrue(
        spec1.isCompatibleOrdering(
            ImmutableList.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("x"))
        )
    );
    // pass if the cursor ordering is exactly the reverse
    Assertions.assertTrue(
        spec1.isCompatibleOrdering(
            ImmutableList.of(OrderBy.descending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.descending("x"))
        )
    );
    // pass if the cursor ordering includes additional ordering not specified by the spec preferred ordering
    Assertions.assertTrue(
        spec1.isCompatibleOrdering(
            ImmutableList.of(
                OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
                OrderBy.ascending("x"),
                OrderBy.descending("y")
            )
        )
    );
    // fail if the cursor ordering is different
    Assertions.assertFalse(
        spec1.isCompatibleOrdering(
            ImmutableList.of(
                OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
                OrderBy.descending("y"),
                OrderBy.ascending("x")
            )
        )
    );
    // fail if the cursor ordering is different
    Assertions.assertFalse(
        spec1.isCompatibleOrdering(
            ImmutableList.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.descending("x"))
        )
    );

    // test no specified preferred ordering by the reader
    CursorBuildSpec spec2 = CursorBuildSpec.builder()
                                           .setPhysicalColumns(ImmutableSet.of(ColumnHolder.TIME_COLUMN_NAME, "x", "y"))
                                           .build();
    Assertions.assertTrue(
        spec2.isCompatibleOrdering(
            ImmutableList.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("x"))
        )
    );
  }

  @Test
  public void testBuilderAndFilterNoExistingFilterWithPhysicalColumns()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder()
                       .setPhysicalColumns(ImmutableSet.of("x", "y"))
                       .setVirtualColumns(
                           VirtualColumns.create(
                               new ExpressionVirtualColumn(
                                   "v0",
                                   "y + 2",
                                   ColumnType.LONG,
                                   TestExprMacroTable.INSTANCE
                               )
                           )
                       );

    builder.andBaseFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assertions.assertEquals(new EqualityFilter("z", ColumnType.STRING, "hello", null), builder.getFilter());
    Assertions.assertEquals(
        ImmutableSet.of("x", "y", "z"),
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterNoExistingFilterWithPhysicalColumnsNoNewReferences()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder()
                       .setPhysicalColumns(ImmutableSet.of("x", "y"))
                       .setVirtualColumns(
                           VirtualColumns.create(
                               new ExpressionVirtualColumn(
                                   "v0",
                                   "y + 2",
                                   ColumnType.LONG,
                                   TestExprMacroTable.INSTANCE
                               )
                           )
                       );

    builder.andBaseFilter(new EqualityFilter("x", ColumnType.STRING, "hello", null));

    Assertions.assertEquals(new EqualityFilter("x", ColumnType.STRING, "hello", null), builder.getFilter());
    Assertions.assertEquals(
        ImmutableSet.of("x", "y"),
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterExistingFilterWithPhysicalColumns()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder()
                       .setFilter(new EqualityFilter("x", ColumnType.STRING, "foo", null))
                       .setPhysicalColumns(ImmutableSet.of("x", "y"))
                       .setVirtualColumns(
                           VirtualColumns.create(
                               new ExpressionVirtualColumn(
                                   "v0",
                                   "y + 2",
                                   ColumnType.LONG,
                                   TestExprMacroTable.INSTANCE
                               )
                           )
                       );

    builder.andBaseFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assertions.assertEquals(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("x", ColumnType.STRING, "foo", null),
                new EqualityFilter("z", ColumnType.STRING, "hello", null)
            )
        ),
        builder.getFilter()
    );
    Assertions.assertEquals(
        ImmutableSet.of("x", "y", "z"),
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterNoExistingFilterNoPhysicalColumns()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder();

    builder.andBaseFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assertions.assertEquals(new EqualityFilter("z", ColumnType.STRING, "hello", null), builder.getFilter());
    Assertions.assertNull(
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterExistingFilterNoPhysicalColumns()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder().setFilter(new EqualityFilter("x", ColumnType.STRING, "foo", null));

    builder.andBaseFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assertions.assertEquals(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("x", ColumnType.STRING, "foo", null),
                new EqualityFilter("z", ColumnType.STRING, "hello", null)
            )
        ),
        builder.getFilter()
    );
    Assertions.assertNull(
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testAndFilterNull()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder = CursorBuildSpec.builder();

    Throwable t = Assertions.assertThrows(
        DruidException.class,
        () -> builder.andBaseFilter(null)
    );

    Assertions.assertEquals("filterToAdd must not be null", t.getMessage());
  }

  @Test
  public void testAddBaseFilter()
  {
    final CursorBuildSpec spec =
        CursorBuildSpec.builder()
                       .setPhysicalColumns(ImmutableSet.of("x"))
                       .setVirtualColumns(VirtualColumns.create(expression("v", "concat(x, 'a')")))
                       .andBaseFilter(new EqualityFilter("y", ColumnType.STRING, "a", null))
                       .build();

    Assertions.assertEquals(new EqualityFilter("y", ColumnType.STRING, "a", null), spec.getFilter());
    Assertions.assertEquals(ImmutableSet.of("x", "y"), spec.getPhysicalColumns());
  }

  @Test
  public void testAddBaseFilterShadowed()
  {
    final CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder().setVirtualColumns(VirtualColumns.create(expression("y", "'a'")));

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> builder.andBaseFilter(new EqualityFilter("y", ColumnType.STRING, "a", null))
    );
    Assertions.assertEquals(DruidException.Persona.USER, e.getTargetPersona());
    Assertions.assertEquals("Virtual column name[y] is not permitted", e.getMessage());
  }

  @Test
  public void testAddBaseFilterShadowedByDotNotation()
  {
    // A virtual column that uses dot notation also takes the names that begin with its own, followed by a dot.
    final VirtualColumn dotNotation = Mockito.mock(VirtualColumn.class);
    Mockito.when(dotNotation.getOutputName()).thenReturn("m");
    Mockito.when(dotNotation.requiredColumns()).thenReturn(ImmutableList.of());
    Mockito.when(dotNotation.usesDotNotation()).thenReturn(true);
    final CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder().setVirtualColumns(VirtualColumns.create(dotNotation));

    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> builder.andBaseFilter(new EqualityFilter("m.k", ColumnType.STRING, "a", null))
    );
    Assertions.assertEquals("Virtual column name[m] is not permitted", e.getMessage());
  }

  @Test
  public void testAddBaseVirtualColumns()
  {
    // "u" is one of ours, read by the spec as a physical column until now, and "w" reads it.
    final CursorBuildSpec spec =
        CursorBuildSpec.builder()
                       .setPhysicalColumns(ImmutableSet.of("u", "z"))
                       .setVirtualColumns(VirtualColumns.create(expression("w", "concat(u, 'a')")))
                       .addBaseVirtualColumns(VirtualColumns.create(expression("u", "concat(x, y)")))
                       .build();

    Assertions.assertEquals(ImmutableList.of("u", "w"), spec.getVirtualColumns().getColumnNames());
    Assertions.assertEquals(ImmutableSet.of("x", "y", "z"), spec.getPhysicalColumns());
  }

  @Test
  public void testAddBaseVirtualColumnsNoPhysicalColumns()
  {
    final CursorBuildSpec spec =
        CursorBuildSpec.builder()
                       .addBaseVirtualColumns(VirtualColumns.create(expression("u", "concat(x, y)")))
                       .build();

    Assertions.assertEquals(ImmutableList.of("u"), spec.getVirtualColumns().getColumnNames());
    Assertions.assertNull(spec.getPhysicalColumns());
  }

  @Test
  public void testAddBaseVirtualColumnsShadowed()
  {
    final VirtualColumns ours = VirtualColumns.create(expression("u", "concat(x, 'a')"));

    // A virtual column already set may take neither the name of one of ours, nor of a column one of ours reads.
    for (final String name : ImmutableList.of("u", "x")) {
      final CursorBuildSpec.CursorBuildSpecBuilder builder =
          CursorBuildSpec.builder().setVirtualColumns(VirtualColumns.create(expression(name, "'q'")));
      final DruidException e =
          Assertions.assertThrows(DruidException.class, () -> builder.addBaseVirtualColumns(ours));
      Assertions.assertEquals(
          "Virtual column name[" + name + "] is not permitted",
          e.getMessage()
      );
    }
  }

  private static VirtualColumn expression(final String name, final String expression)
  {
    return new ExpressionVirtualColumn(name, expression, ColumnType.STRING, TestExprMacroTable.INSTANCE);
  }
}
