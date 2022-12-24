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

package org.apache.druid.query.rowsandcols;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class MapOfColumnsRowsAndColumnsTest extends RowsAndColumnsTestBase
{
  public MapOfColumnsRowsAndColumnsTest()
  {
    super(MapOfColumnsRowsAndColumns.class);
  }

  @Test
  public void testMakeWithEmptyAndNull()
  {
    boolean exceptionThrown = false;
    try {
      MapOfColumnsRowsAndColumns.fromMap(null);
    }
    catch (ISE ex) {
      Assert.assertEquals("map[null] cannot be null or empty.", ex.getMessage());
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);

    exceptionThrown = false;
    try {
      MapOfColumnsRowsAndColumns.fromMap(Collections.emptyMap());
    }
    catch (ISE ex) {
      Assert.assertEquals("map[{}] cannot be null or empty.", ex.getMessage());
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }

  @Test
  public void testExceptionOnMismatchedCells()
  {
    boolean exceptionThrown = false;
    try {
      MapOfColumnsRowsAndColumns.of(
          "1", new IntArrayColumn(new int[]{0}),
          "2", new IntArrayColumn(new int[]{0, 1})
      );
    }
    catch (ISE ex) {
      Assert.assertEquals("Mismatched numCells, expectedNumCells[1], actual[2] from col[2].", ex.getMessage());
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
  }
}
