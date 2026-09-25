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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.jupiter.api.Assertions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class RowsAndColumnsHelper
{
  public static void assertEquals(RowsAndColumns rac, String name, int[] expectedResults)
  {
    final Column column = rac.findColumn(name);
    Assertions.assertNotNull(column);
    final ColumnAccessor accessor = column.toAccessor();
    Assertions.assertEquals(expectedResults.length, accessor.numRows());
    for (int i = 0; i < expectedResults.length; ++i) {
      Assertions.assertEquals(expectedResults[i], accessor.getInt(i), StringUtils.format("%s[%s]", name, i));
    }
  }

  public static void assertEquals(RowsAndColumns rac, String name, long[] expectedResults)
  {
    final Column column = rac.findColumn(name);
    Assertions.assertNotNull(column);
    final ColumnAccessor accessor = column.toAccessor();
    Assertions.assertEquals(expectedResults.length, accessor.numRows());
    for (int i = 0; i < expectedResults.length; ++i) {
      Assertions.assertEquals(expectedResults[i], accessor.getLong(i), StringUtils.format("%s[%s]", name, i));
    }
  }

  public static void assertEquals(RowsAndColumns rac, String name, double[] expectedResults)
  {
    final Column column = rac.findColumn(name);
    Assertions.assertNotNull(column);
    final ColumnAccessor accessor = column.toAccessor();
    Assertions.assertEquals(expectedResults.length, accessor.numRows());
    for (int i = 0; i < expectedResults.length; ++i) {
      Assertions.assertEquals(expectedResults[i], accessor.getDouble(i), 0.0d, StringUtils.format("%s[%s]", name, i));
    }
  }

  private final Map<String, ColumnHelper> helpers = new LinkedHashMap<>();
  private Set<String> fullColumnSet;
  private AtomicReference<Integer> expectedSize = new AtomicReference<>();

  public RowsAndColumnsHelper()
  {
  }

  public RowsAndColumnsHelper expectColumn(String col, int[] expectedVals)
  {
    final ColumnHelper helper = columnHelper(col, expectedVals.length, ColumnType.LONG);
    helper.setExpectation(expectedVals);
    return this;
  }

  public RowsAndColumnsHelper expectColumn(String col, long[] expectedVals)
  {
    final ColumnHelper helper = columnHelper(col, expectedVals.length, ColumnType.LONG);
    helper.setExpectation(expectedVals);
    return this;
  }

  public RowsAndColumnsHelper expectColumn(String col, double[] expectedVals)
  {
    final ColumnHelper helper = columnHelper(col, expectedVals.length, ColumnType.DOUBLE);
    helper.setExpectation(expectedVals);
    return this;
  }

  public RowsAndColumnsHelper expectColumn(String col, float[] expectedVals)
  {
    final ColumnHelper helper = columnHelper(col, expectedVals.length, ColumnType.FLOAT);
    helper.setExpectation(expectedVals);
    return this;
  }

  public RowsAndColumnsHelper expectColumn(String col, ColumnType type, Object... expectedVals)
  {
    return expectColumn(col, expectedVals, type);
  }

  public RowsAndColumnsHelper expectColumn(String col, Object[] expectedVals, ColumnType type)
  {
    IntArrayList nullPositions = new IntArrayList();
    for (int i = 0; i < expectedVals.length; i++) {
      if (expectedVals[i] == null) {
        nullPositions.add(i);
      }
    }

    final ColumnHelper helper = columnHelper(col, expectedVals.length, type);
    helper.setExpectation(expectedVals);
    if (!nullPositions.isEmpty()) {
      helper.setNulls(nullPositions.toIntArray());
    }
    return this;
  }

  public static RowsAndColumns makeSingleColumnRac(int... values)
  {
    return MapOfColumnsRowsAndColumns.fromMap(
        ImmutableMap.of("column", new IntArrayColumn(values))
    );
  }

  public static RowsAndColumnsHelper expectedSingleColumnRac(int... values)
  {
    return new RowsAndColumnsHelper().expectColumn("column", values).allColumnsRegistered();
  }

  public ColumnHelper columnHelper(String column, int expectedSize, ColumnType expectedType)
  {
    if (this.expectedSize.get() == null) {
      this.expectedSize.set(expectedSize);
    }
    Assertions.assertEquals(this.expectedSize.get().intValue(), expectedSize, "Columns should be defined with same size");
    ColumnHelper retVal = helpers.get(column);
    if (retVal == null) {
      retVal = new ColumnHelper(expectedSize, expectedType);
      helpers.put(column, retVal);
      return retVal;
    } else {
      throw new ISE(
          "column[%s] expectations already defined, size[%s], type[%s]",
          column,
          retVal.expectedVals.length,
          retVal.expectedType
      );
    }
  }

  public RowsAndColumnsHelper expectFullColumns(Set<String> fullColumnSet)
  {
    this.fullColumnSet = fullColumnSet;
    return this;
  }

  public RowsAndColumnsHelper allColumnsRegistered()
  {
    this.fullColumnSet = ImmutableSet.copyOf(helpers.keySet());
    return this;
  }

  public void validate(RowsAndColumns rac)
  {
    validate("", rac);
  }

  public void validate(String name, RowsAndColumns rac)
  {
    if (fullColumnSet != null) {
      final Collection<String> columnNames = rac.getColumnNames();
      Assertions.assertEquals(fullColumnSet.size(), columnNames.size(), name);
      Assertions.assertTrue(fullColumnSet.containsAll(columnNames), name);
    }

    for (Map.Entry<String, ColumnHelper> entry : helpers.entrySet()) {
      final Column racColumn = rac.findColumn(entry.getKey());
      Assertions.assertNotNull(racColumn);
      entry.getValue().validate(StringUtils.format("%s.%s", name, entry.getKey()), racColumn);
    }
  }

  public static class ColumnHelper
  {
    private final ColumnType expectedType;
    private final Object[] expectedVals;
    private final boolean[] expectedNulls;

    public ColumnHelper(int expectedSize, ColumnType expectedType)
    {
      this.expectedType = expectedType;
      this.expectedVals = new Object[expectedSize];
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

    public void validate(String msgBase, Column col)
    {
      final ColumnAccessor accessor = col.toAccessor();

      Assertions.assertEquals(expectedType, accessor.getType(), msgBase);
      Assertions.assertEquals(expectedVals.length, accessor.numRows(), msgBase);
      for (int i = 0; i < accessor.numRows(); ++i) {
        final String msg = StringUtils.format("%s[%s]", msgBase, i);
        Object expectedVal = expectedVals[i];
        if (expectedVal == null) {
          Assertions.assertTrue(expectedNulls[i], msg);
          Assertions.assertTrue(accessor.isNull(i), msg);
          Assertions.assertNull(accessor.getObject(i), msg);
        }

        Assertions.assertEquals(expectedNulls[i], accessor.isNull(i), msg + " is null?");
        if (expectedVal instanceof Float) {
          if (expectedNulls[i]) {
            Assertions.assertEquals(0.0f, accessor.getFloat(i), 0.0, msg);
          } else {
            Assertions.assertEquals((Float) expectedVal, accessor.getFloat(i), 0.0, msg);
          }
        } else if (expectedVal instanceof Double) {
          if (expectedNulls[i]) {
            Assertions.assertEquals(0.0d, accessor.getDouble(i), 0.0, msg);
          } else {
            Assertions.assertEquals((Double) expectedVal, accessor.getDouble(i), 0.0, msg);
          }
        } else if (expectedVal instanceof Integer) {
          if (expectedNulls[i]) {
            Assertions.assertEquals(0, accessor.getInt(i), msg);
          } else {
            Assertions.assertEquals(((Integer) expectedVal).intValue(), accessor.getInt(i), msg);
          }
        } else if (expectedVal instanceof Long) {
          if (expectedNulls[i]) {
            Assertions.assertEquals(0, accessor.getLong(i), msg);
          } else {
            Assertions.assertEquals(((Long) expectedVal).longValue(), accessor.getLong(i), msg);
          }
        } else if (expectedVal instanceof Object[]) {
          Object actualVal = accessor.getObject(i);
          if (expectedNulls[i]) {
            Assertions.assertNull(accessor.getObject(i), msg);
          } else {
            if (actualVal instanceof ArrayList) {
              Assertions.assertArrayEquals((Object[]) expectedVals[i], ((ArrayList<?>) actualVal).toArray(), msg);
            } else {
              Assertions.assertArrayEquals((Object[]) expectedVals[i], (Object[]) actualVal, msg);
            }
          }
        } else {
          if (expectedNulls[i]) {
            Assertions.assertNull(accessor.getObject(i), msg);
            // asserting null on the expected value is here for consistency in the tests.  If it fails, it's most
            // likely indicative of something wrong with the test setup than the actual logic, we keep it for
            // sanity's sake to things consistent.
            Assertions.assertNull(expectedVal, msg);
          } else {
            final Object obj = accessor.getObject(i);
            Assertions.assertNotNull(obj, msg);
            Assertions.assertEquals(expectedVal, obj, msg);
          }
        }
      }
    }
  }
}
