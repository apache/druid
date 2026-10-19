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

package org.apache.druid.query;

import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Tests for {@link UnnestDataSource}.
 */
public class UnnestDataSourceTest extends InitializedNullHandlingTest
{
  private static final DataSource BASE =
      FilteredDataSource.create(new TableDataSource("foo"), DimFilters.dimEquals("dim3", "x"));
  private static final ExpressionVirtualColumn UNNEST_COLUMN =
      new ExpressionVirtualColumn("u", "array(dim1, dim2)", ColumnType.STRING_ARRAY, ExprMacroTable.nil());

  @Test
  public void test_getReservedColumns()
  {
    // Names reserved by the base are not reserved, since virtual columns applied on top of an unnest never reach it.
    Assertions.assertEquals(
        Set.of("u", "dim1", "dim2"),
        UnnestDataSource.create(BASE, UNNEST_COLUMN, null).getReservedColumns()
    );
    Assertions.assertEquals(
        Set.of("u", "dim1", "dim2", "dim4"),
        UnnestDataSource.create(BASE, UNNEST_COLUMN, DimFilters.dimEquals("dim4", "a")).getReservedColumns()
    );
  }
}
