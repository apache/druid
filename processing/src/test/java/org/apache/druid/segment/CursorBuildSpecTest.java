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
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Test;

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
    Assert.assertFalse(spec1.isCompatibleOrdering(ImmutableList.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME))));
    // pass if the cursor ordering exactly matches
    Assert.assertTrue(
        spec1.isCompatibleOrdering(
            ImmutableList.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME), OrderBy.ascending("x"))
        )
    );
    // pass if the cursor ordering includes additional ordering not specified by the spec preferred ordering
    Assert.assertTrue(
        spec1.isCompatibleOrdering(
            ImmutableList.of(
                OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
                OrderBy.ascending("x"),
                OrderBy.descending("y")
            )
        )
    );
    // fail if the cursor ordering is different
    Assert.assertFalse(
        spec1.isCompatibleOrdering(
            ImmutableList.of(
                OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
                OrderBy.descending("y"),
                OrderBy.ascending("x")
            )
        )
    );

    // test no specified preferred ordering by the reader
    CursorBuildSpec spec2 = CursorBuildSpec.builder()
                                           .setPhysicalColumns(ImmutableSet.of(ColumnHolder.TIME_COLUMN_NAME, "x", "y"))
                                           .build();
    Assert.assertTrue(
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

    builder.andFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assert.assertEquals(new EqualityFilter("z", ColumnType.STRING, "hello", null), builder.getFilter());
    Assert.assertEquals(
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

    builder.andFilter(new EqualityFilter("x", ColumnType.STRING, "hello", null));

    Assert.assertEquals(new EqualityFilter("x", ColumnType.STRING, "hello", null), builder.getFilter());
    Assert.assertEquals(
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

    builder.andFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assert.assertEquals(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("x", ColumnType.STRING, "foo", null),
                new EqualityFilter("z", ColumnType.STRING, "hello", null)
            )
        ),
        builder.getFilter()
    );
    Assert.assertEquals(
        ImmutableSet.of("x", "y", "z"),
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterNoExistingFilterNoPhysicalColumns()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder();

    builder.andFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assert.assertEquals(new EqualityFilter("z", ColumnType.STRING, "hello", null), builder.getFilter());
    Assert.assertNull(
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterExistingFilterNoPhysicalColumns()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder =
        CursorBuildSpec.builder().setFilter(new EqualityFilter("x", ColumnType.STRING, "foo", null));

    builder.andFilter(new EqualityFilter("z", ColumnType.STRING, "hello", null));

    Assert.assertEquals(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("x", ColumnType.STRING, "foo", null),
                new EqualityFilter("z", ColumnType.STRING, "hello", null)
            )
        ),
        builder.getFilter()
    );
    Assert.assertNull(
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterNoExistingFilterUsingVirtualColumn()
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
    builder.andFilter(new RangeFilter("v0", ColumnType.LONG, 1L, 3L, true, true, null));

    Assert.assertEquals(
        new RangeFilter("v0", ColumnType.LONG, 1L, 3L, true, true, null),
        builder.getFilter()
    );
    Assert.assertEquals(
        ImmutableSet.of("x", "y"),
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testBuilderAndFilterExistingFilterNewFilterUsingVirtualColumn()
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
    builder.andFilter(new RangeFilter("v0", ColumnType.LONG, 1L, 3L, true, true, null));

    Assert.assertEquals(
        new AndFilter(
            ImmutableList.of(
                new EqualityFilter("x", ColumnType.STRING, "foo", null),
                new RangeFilter("v0", ColumnType.LONG, 1L, 3L, true, true, null)
            )
        ),
        builder.getFilter()
    );
    Assert.assertEquals(
        ImmutableSet.of("x", "y"),
        builder.getPhysicalColumns()
    );
  }

  @Test
  public void testAndFilterNull()
  {
    CursorBuildSpec.CursorBuildSpecBuilder builder = CursorBuildSpec.builder();

    Throwable t = Assert.assertThrows(
        DruidException.class,
        () -> builder.andFilter(null)
    );

    Assert.assertEquals("filterToAdd must not be null", t.getMessage());
  }
}
