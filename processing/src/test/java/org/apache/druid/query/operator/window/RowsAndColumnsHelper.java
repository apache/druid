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

package org.apache.druid.query.operator.window;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Assert;

public class RowsAndColumnsHelper
{
  public static void assertEquals(RowsAndColumns rac, String name, int[] expectedResults)
  {
    final Column column = rac.findColumn(name);
    Assert.assertNotNull(column);
    final ColumnAccessor accessor = column.toAccessor();
    Assert.assertEquals(expectedResults.length, accessor.numCells());
    for (int i = 0; i < expectedResults.length; ++i) {
      Assert.assertEquals(StringUtils.format("%s[%s]", name, i), expectedResults[i], accessor.getInt(i));
    }
  }

  public static void assertEquals(RowsAndColumns rac, String name, long[] expectedResults)
  {
    final Column column = rac.findColumn(name);
    Assert.assertNotNull(column);
    final ColumnAccessor accessor = column.toAccessor();
    Assert.assertEquals(expectedResults.length, accessor.numCells());
    for (int i = 0; i < expectedResults.length; ++i) {
      Assert.assertEquals(StringUtils.format("%s[%s]", name, i), expectedResults[i], accessor.getLong(i));
    }
  }

  public static void assertEquals(RowsAndColumns rac, String name, double[] expectedResults)
  {
    final Column column = rac.findColumn(name);
    Assert.assertNotNull(column);
    final ColumnAccessor accessor = column.toAccessor();
    Assert.assertEquals(expectedResults.length, accessor.numCells());
    for (int i = 0; i < expectedResults.length; ++i) {
      Assert.assertEquals(StringUtils.format("%s[%s]", name, i), expectedResults[i], accessor.getDouble(i), 0.0d);
    }
  }

  private final RowsAndColumns rac;

  public RowsAndColumnsHelper(
      RowsAndColumns rac
  )
  {
    this.rac = rac;
  }

  public ColumnHelper forColumn(String column, ColumnType expectedType)
  {
    return new ColumnHelper(rac.findColumn(column), expectedType);
  }

  public static class ColumnHelper
  {
    private final Column col;
    private final ColumnType expectedType;
    private final Object[] expectedVals;
    private final boolean[] expectedNulls;

    public ColumnHelper(Column col, ColumnType expectedType)
    {
      this.col = col;
      this.expectedType = expectedType;
      this.expectedVals = new Object[col.toAccessor().numCells()];
      this.expectedNulls = new boolean[expectedVals.length];
    }

    public ColumnHelper setExpectation(int[] expectedVals)
    {
      for (int i = 0; i < expectedVals.length; i++) {
        this.expectedVals[i] = expectedVals[i];
      }
      return this;
    }

    public ColumnHelper setExpectation(long[] expectedVals)
    {
      for (int i = 0; i < expectedVals.length; i++) {
        this.expectedVals[i] = expectedVals[i];
      }
      return this;
    }

    public ColumnHelper setExpectation(double[] expectedVals)
    {
      for (int i = 0; i < expectedVals.length; i++) {
        this.expectedVals[i] = expectedVals[i];
      }
      return this;
    }

    public ColumnHelper setExpectation(float[] expectedVals)
    {
      for (int i = 0; i < expectedVals.length; i++) {
        this.expectedVals[i] = expectedVals[i];
      }
      return this;
    }

    public ColumnHelper setExpectation(Object[] expectedVals)
    {
      System.arraycopy(expectedVals, 0, this.expectedVals, 0, expectedVals.length);
      return this;
    }

    public ColumnHelper setNulls(int[] nullIndexes)
    {
      for (int nullIndex : nullIndexes) {
        this.expectedNulls[nullIndex] = true;
      }
      return this;
    }

    public void validate()
    {
      final ColumnAccessor accessor = col.toAccessor();
      Assert.assertEquals(expectedType, accessor.getType());

      for (int i = 0; i < accessor.numCells(); ++i) {
        final String msg = String.valueOf(i);
        Object expectedVal = expectedVals[i];
        if (expectedVal == null) {
          Assert.assertTrue(msg, expectedNulls[i]);
          Assert.assertTrue(msg, accessor.isNull(i));
          Assert.assertNull(msg, accessor.getObject(i));
        }
        if (expectedVal instanceof Float) {
          if (expectedNulls[i]) {
            Assert.assertTrue(msg, accessor.isNull(i));
            Assert.assertEquals(msg, 0.0f, accessor.getFloat(i), 0.0);
          } else {
            Assert.assertFalse(msg, accessor.isNull(i));
            Assert.assertEquals(msg, (Float) expectedVal, accessor.getFloat(i), 0.0);
          }
        } else if (expectedVal instanceof Double) {
          if (expectedNulls[i]) {
            Assert.assertTrue(msg, accessor.isNull(i));
            Assert.assertEquals(msg, 0.0d, accessor.getDouble(i), 0.0);
          } else {
            Assert.assertFalse(msg, accessor.isNull(i));
            Assert.assertEquals(msg, (Double) expectedVal, accessor.getDouble(i), 0.0);
          }
        } else if (expectedVal instanceof Integer) {
          if (expectedNulls[i]) {
            Assert.assertTrue(msg, accessor.isNull(i));
            Assert.assertEquals(msg, 0, accessor.getInt(i));
          } else {
            Assert.assertFalse(msg, accessor.isNull(i));
            Assert.assertEquals(msg, ((Integer) expectedVal).intValue(), accessor.getInt(i));
          }
        } else if (expectedVal instanceof Long) {
          if (expectedNulls[i]) {
            Assert.assertTrue(msg, accessor.isNull(i));
            Assert.assertEquals(msg, 0, accessor.getLong(i));
          } else {
            Assert.assertFalse(msg, accessor.isNull(i));
            Assert.assertEquals(msg, ((Long) expectedVal).longValue(), accessor.getLong(i));
          }
        } else {
          if (expectedNulls[i]) {
            Assert.assertTrue(msg, accessor.isNull(i));
            Assert.assertNull(msg, accessor.getObject(i));
            // This is just for consistency in the tests.  It's likely more a validation of the test setup than the
            // actual logic, but let's keep it for the consistency.
            Assert.assertNull(msg, expectedVals[i]);
          } else {
            final Object obj = accessor.getObject(i);
            Assert.assertFalse(msg, accessor.isNull(i));
            Assert.assertNotNull(msg, obj);
            Assert.assertEquals(msg, expectedVals[i], obj);
          }
        }
      }
    }
  }
}
