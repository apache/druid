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

import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.OrderBy;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link Cursors}.
 */
public class CursorsTest
{
  private static final List<OrderBy> ORDERING = List.of(
      OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME),
      OrderBy.ascending("a"),
      OrderBy.descending("b"),
      OrderBy.ascending("c")
  );

  @Test
  public void test_trimOrdering_none()
  {
    Assertions.assertSame(ORDERING, Cursors.trimOrdering(ORDERING, VirtualColumns.EMPTY));
    Assertions.assertSame(ORDERING, Cursors.trimOrdering(ORDERING, virtualColumns("x")));
  }

  @Test
  public void test_trimOrdering_shadowed()
  {
    Assertions.assertEquals(ORDERING.subList(0, 2), Cursors.trimOrdering(ORDERING, virtualColumns("b")));
    Assertions.assertEquals(
        ORDERING.subList(0, 1),
        Cursors.trimOrdering(ORDERING, virtualColumns("c", "a"))
    );
  }

  private static VirtualColumns virtualColumns(final String... names)
  {
    final VirtualColumn[] virtualColumns = new VirtualColumn[names.length];
    for (int i = 0; i < names.length; i++) {
      virtualColumns[i] = new ExpressionVirtualColumn(names[i], "'q'", null, ExprMacroTable.nil());
    }
    return VirtualColumns.create(virtualColumns);
  }
}
