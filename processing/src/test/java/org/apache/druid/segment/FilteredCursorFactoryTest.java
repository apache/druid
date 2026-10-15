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

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Tests for {@link FilteredCursorFactory}.
 */
public class FilteredCursorFactoryTest extends InitializedNullHandlingTest
{
  private static final Closer CLOSER = Closer.create();
  private static FilteredCursorFactory filtered;

  @BeforeAll
  public static void setUpClass(@TempDir final File tempDir)
  {
    filtered = new FilteredCursorFactory(
        new QueryableIndexCursorFactory(CLOSER.register(SmallSegmentTestHelper.makeIndex(tempDir))),
        new EqualityFilter("dim", ColumnType.STRING, "b", null)
    );
  }

  @AfterAll
  public static void tearDownClass() throws IOException
  {
    CLOSER.close();
  }

  @Test
  public void test_virtualColumn()
  {
    Assertions.assertEquals(
        List.of(List.of("b", "x?")),
        SmallSegmentTestHelper.readRows(filtered, spec("v"), "dim", "v")
    );
  }

  @Test
  public void test_virtualColumnShadowsFilterColumn()
  {
    // The filter would read the virtual column rather than the table's own "dim".
    final CursorBuildSpec spec = spec("dim");
    final String message = "Virtual column name[dim] is not permitted";

    Assertions.assertEquals(
        message,
        Assertions.assertThrows(DruidException.class, () -> filtered.makeCursorHolder(spec)).getMessage()
    );
    Assertions.assertEquals(
        message,
        Assertions.assertThrows(DruidException.class, () -> filtered.makeCursorHolderAsync(spec)).getMessage()
    );
  }

  /**
   * Returns a spec with a virtual column of the given name that appends "?" to "dim2".
   */
  private static CursorBuildSpec spec(final String name)
  {
    return CursorBuildSpec.builder()
                          .setVirtualColumns(
                              VirtualColumns.create(
                                  new ExpressionVirtualColumn(
                                      name,
                                      "concat(\"dim2\",'?')",
                                      ColumnType.STRING,
                                      ExprMacroTable.nil()
                                  )
                              )
                          )
                          .build();
  }
}
