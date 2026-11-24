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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.filter.DimFilters;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

/**
 * Tests for {@link ProjectDataSource}.
 */
public class ProjectDataSourceTest extends InitializedNullHandlingTest
{
  private static final TableDataSource FOO = new TableDataSource("foo");

  @Test
  public void test_getReservedColumns()
  {
    final DataSource dataSource = new ProjectDataSource(
        FilteredDataSource.create(FOO, DimFilters.dimEquals("dim3", "x")),
        VirtualColumns.create(
            new ExpressionVirtualColumn("v0", "concat(dim1, dim2)", ColumnType.STRING, ExprMacroTable.nil())
        )
    );

    Assertions.assertEquals(Set.of("v0", "dim1", "dim2", "dim3"), dataSource.getReservedColumns());
  }

  @Test
  public void test_serde() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final DataSource dataSource = new ProjectDataSource(
        FOO,
        VirtualColumns.create(new ExpressionVirtualColumn("out", Parser.identifier("dim1"), null))
    );

    Assertions.assertEquals(
        dataSource,
        jsonMapper.readValue(jsonMapper.writeValueAsString(dataSource), DataSource.class)
    );
  }
}
