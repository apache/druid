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

package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.query.rowsandcols.util.FindResult;
import org.junit.Assert;
import org.junit.Test;

public class LongArrayColumnTest
{
  @Test
  public void testLongArrayColumnWithLongValues()
  {
    Column column = new LongArrayColumn(new long[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    ColumnAccessor accessor = column.toAccessor();

    for (int i = 0; i < 10; i++) {
      Assert.assertFalse(accessor.isNull(i));
      Assert.assertEquals(i, accessor.getLong(i));
      Assert.assertEquals((long) i, accessor.getObject(i));
      Assert.assertEquals(i, accessor.getDouble(i), 0);
      Assert.assertEquals(i, accessor.getInt(i));
    }
  }

  @Test
  public void testFindLong()
  {
    Column column = new LongArrayColumn(new long[] {1, 1, 1, 3, 5, 5, 6, 7, 8, 9});
    BinarySearchableAccessor accessor = (BinarySearchableAccessor) column.toAccessor();

    FindResult findResult = accessor.findLong(0, accessor.numRows(), 1);
    Assert.assertTrue(findResult.wasFound());
    Assert.assertEquals(0, findResult.getStartRow());
    Assert.assertEquals(3, findResult.getEndRow());

    findResult = accessor.findLong(0, accessor.numRows(), 6);
    Assert.assertTrue(findResult.wasFound());
    Assert.assertEquals(6, findResult.getStartRow());
    Assert.assertEquals(7, findResult.getEndRow());

    Assert.assertFalse(accessor.findLong(0, accessor.numRows(), 2).wasFound());
    Assert.assertFalse(accessor.findLong(0, 3, 9).wasFound());
  }

  @Test
  public void testOtherTypeFinds()
  {
    Column column = new LongArrayColumn(new long[] {0, 1, 2, 3, 4, 5, Long.MAX_VALUE});
    BinarySearchableAccessor accessor = (BinarySearchableAccessor) column.toAccessor();

    FindResult findResult = accessor.findNull(0, accessor.numRows());
    Assert.assertFalse(findResult.wasFound()); // Always false for long array columns

    findResult = accessor.findDouble(0, accessor.numRows(), 3.0);
    Assert.assertTrue(findResult.wasFound());
    Assert.assertEquals(3, findResult.getStartRow());
    Assert.assertEquals(4, findResult.getEndRow());

    findResult = accessor.findFloat(0, accessor.numRows(), 1.0f);
    Assert.assertTrue(findResult.wasFound());
    Assert.assertEquals(1, findResult.getStartRow());
    Assert.assertEquals(2, findResult.getEndRow());
  }
}
