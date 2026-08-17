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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.RowAdapter;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class InlineDataSourceTest
{
  private final AtomicLong iterationCounter = new AtomicLong();

  // raw arrays, e.g. double[]{1.0, 2.0} on round trip get deserialized into List<Double>, so just model that
  private final List<Object[]> rows = ImmutableList.of(
      new Object[]{DateTimes.of("2000").getMillis(), "foo", 0d, ImmutableMap.of("n", "0"), ImmutableList.of(1.0, 2.0)},
      new Object[]{DateTimes.of("2000").getMillis(), "bar", 1d, ImmutableMap.of("n", "1"), ImmutableList.of(2.0, 4.0)},
      new Object[]{DateTimes.of("2000").getMillis(), "baz", 2d, ImmutableMap.of("n", "2"), ImmutableList.of(3.0, 6.0)}
  );

  private final Iterable<Object[]> rowsIterable = () -> {
    iterationCounter.incrementAndGet();
    return rows.iterator();
  };

  private final List<String> expectedColumnNames = ImmutableList.of(
      ColumnHolder.TIME_COLUMN_NAME,
      "str",
      "double",
      "complex",
      "double_array"
  );

  private final ColumnType someComplex = new ColumnType(ValueType.COMPLEX, "foo", null);
  private final List<ColumnType> expectedColumnTypes = ImmutableList.of(
      ColumnType.LONG,
      ColumnType.STRING,
      ColumnType.DOUBLE,
      someComplex,
      ColumnType.DOUBLE_ARRAY
  );

  private final RowSignature expectedRowSignature;

  private final InlineDataSource listDataSource;

  private final InlineDataSource iterableDataSource;

  public InlineDataSourceTest()
  {
    final RowSignature.Builder builder = RowSignature.builder();

    for (int i = 0; i < expectedColumnNames.size(); i++) {
      builder.add(expectedColumnNames.get(i), expectedColumnTypes.get(i));
    }

    expectedRowSignature = builder.build();
    listDataSource = InlineDataSource.fromIterable(rows, expectedRowSignature);
    iterableDataSource = InlineDataSource.fromIterable(rowsIterable, expectedRowSignature);
  }

  @Test
  public void test_getTableNames()
  {
    Assertions.assertEquals(Collections.emptySet(), listDataSource.getTableNames());
    Assertions.assertEquals(Collections.emptySet(), iterableDataSource.getTableNames());
  }

  @Test
  public void test_getColumnNames()
  {
    Assertions.assertEquals(expectedColumnNames, listDataSource.getColumnNames());
    Assertions.assertEquals(expectedColumnNames, iterableDataSource.getColumnNames());
  }

  @Test
  public void test_getColumnTypes()
  {
    Assertions.assertEquals(expectedColumnTypes, listDataSource.getColumnTypes());
    Assertions.assertEquals(expectedColumnTypes, iterableDataSource.getColumnTypes());
  }

  @Test
  public void test_getChildren()
  {
    Assertions.assertEquals(Collections.emptyList(), listDataSource.getChildren());
    Assertions.assertEquals(Collections.emptyList(), iterableDataSource.getChildren());
  }

  @Test
  public void test_getRowSignature()
  {
    Assertions.assertEquals(
        RowSignature.builder()
                    .add(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG)
                    .add("str", ColumnType.STRING)
                    .add("double", ColumnType.DOUBLE)
                    .add("complex", someComplex)
                    .add("double_array", ColumnType.DOUBLE_ARRAY)
                    .build(),
        listDataSource.getRowSignature()
    );
  }

  @Test
  public void test_isCacheable()
  {
    Assertions.assertFalse(listDataSource.isCacheable(true));
    Assertions.assertFalse(listDataSource.isCacheable(false));
  }

  @Test
  public void test_isGlobal()
  {
    Assertions.assertTrue(listDataSource.isGlobal());
  }

  @Test
  public void test_isConcrete()
  {
    Assertions.assertTrue(listDataSource.isProcessable());
  }

  @Test
  public void test_rowAdapter()
  {
    final RowAdapter<Object[]> adapter = listDataSource.rowAdapter();
    final Object[] row = rows.get(1);

    Assertions.assertEquals(DateTimes.of("2000").getMillis(), adapter.timestampFunction().applyAsLong(row));
    Assertions.assertEquals("bar", adapter.columnFunction("str").apply(row));
    Assertions.assertEquals(1d, adapter.columnFunction("double").apply(row));
    Assertions.assertEquals(ImmutableMap.of("n", "1"), adapter.columnFunction("complex").apply(row));
    Assertions.assertEquals(ImmutableList.of(2.0, 4.0), adapter.columnFunction("double_array").apply(row));
  }

  @Test
  public void test_getRows_list()
  {
    Assertions.assertSame(this.rows, listDataSource.getRowsAsList());
  }

  @Test
  public void test_getRows_iterable()
  {
    final Iterable<Object[]> iterable = iterableDataSource.getRows();
    Assertions.assertNotSame(this.rows, iterable);

    // No iteration yet.
    Assertions.assertEquals(0, iterationCounter.get());

    assertRowsEqual(this.rows, ImmutableList.copyOf(iterable));

    // OK, now we've iterated.
    Assertions.assertEquals(1, iterationCounter.get());

    // Read again, we should iterate again.
    //noinspection MismatchedQueryAndUpdateOfCollection
    final List<Object[]> ignored = Lists.newArrayList(iterable);
    Assertions.assertEquals(2, iterationCounter.get());
  }

  @Test
  public void test_getRowsAsList_list()
  {
    Assertions.assertSame(this.rows, listDataSource.getRowsAsList());
  }

  @Test
  public void test_getRowsAsList_iterable()
  {
    final List<Object[]> list = iterableDataSource.getRowsAsList();

    Assertions.assertEquals(1, iterationCounter.get());
    assertRowsEqual(this.rows, list);

    // Read again, we should *not* iterate again (in contrast to "test_getRows_iterable").
    //noinspection MismatchedQueryAndUpdateOfCollection
    final List<Object[]> ignored = Lists.newArrayList(list);
    Assertions.assertEquals(1, iterationCounter.get());
  }

  @Test
  public void test_withChildren_empty()
  {
    Assertions.assertSame(listDataSource, listDataSource.withChildren(Collections.emptyList()));
  }

  @Test
  public void test_withChildren_nonEmpty()
  {
    Throwable t = Assertions.assertThrows(
        IllegalArgumentException.class,
        // Workaround so "withChildren" isn't flagged as unused in the DataSource interface.
        () -> ((DataSource) listDataSource).withChildren(ImmutableList.of(new TableDataSource("foo")))
    );
    Assertions.assertEquals("Cannot accept children", t.getMessage());
  }

  @Test
  public void test_equals()
  {
    EqualsVerifier.forClass(InlineDataSource.class)
                  .usingGetClass()
                  .withNonnullFields("rows", "signature")
                  .verify();
  }

  @Test
  public void test_toString_iterable()
  {
    // Verify that toString does not iterate the rows.
    final String ignored = iterableDataSource.toString();
    Assertions.assertEquals(0, iterationCounter.get());
  }

  @Test
  public void test_serde_list() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final InlineDataSource deserialized = (InlineDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(listDataSource),
        DataSource.class
    );

    Assertions.assertEquals(listDataSource.getColumnNames(), deserialized.getColumnNames());
    Assertions.assertEquals(listDataSource.getColumnTypes(), deserialized.getColumnTypes());
    Assertions.assertEquals(listDataSource.getRowSignature(), deserialized.getRowSignature());
    assertRowsEqual(listDataSource.getRows(), deserialized.getRows());
  }

  @Test
  public void test_serde_iterable() throws Exception
  {
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final InlineDataSource deserialized = (InlineDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(iterableDataSource),
        DataSource.class
    );

    // Lazy iterables turn into Lists upon serialization.
    Assertions.assertEquals(listDataSource.getColumnNames(), deserialized.getColumnNames());
    Assertions.assertEquals(listDataSource.getColumnTypes(), deserialized.getColumnTypes());
    Assertions.assertEquals(listDataSource.getRowSignature(), deserialized.getRowSignature());
    assertRowsEqual(listDataSource.getRows(), deserialized.getRows());

    // Should have iterated once.
    Assertions.assertEquals(1, iterationCounter.get());
  }

  @Test
  public void test_serde_untyped() throws Exception
  {
    // Create a row signature with no types set.
    final RowSignature.Builder builder = RowSignature.builder();
    for (String columnName : expectedRowSignature.getColumnNames()) {
      builder.add(columnName, null);
    }

    final RowSignature untypedSignature = builder.build();
    final InlineDataSource untypedDataSource = InlineDataSource.fromIterable(rows, untypedSignature);

    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
    final InlineDataSource deserialized = (InlineDataSource) jsonMapper.readValue(
        jsonMapper.writeValueAsString(untypedDataSource),
        DataSource.class
    );

    Assertions.assertEquals(untypedDataSource.getColumnNames(), deserialized.getColumnNames());
    Assertions.assertEquals(untypedDataSource.getColumnTypes(), deserialized.getColumnTypes());
    Assertions.assertEquals(untypedDataSource.getRowSignature(), deserialized.getRowSignature());
    Assertions.assertNull(deserialized.getColumnTypes());
    assertRowsEqual(listDataSource.getRows(), deserialized.getRows());
  }

  /**
   * This method exists because "equals" on two equivalent Object[] won't return true, so we need to check
   * for equality deeply.
   */
  private static void assertRowsEqual(final Iterable<Object[]> expectedRows, final Iterable<Object[]> actualRows)
  {
    if (expectedRows instanceof List && actualRows instanceof List) {
      // Only check equality deeply when both rows1 and rows2 are Lists, i.e., non-lazy.
      final List<Object[]> expectedRowsList = (List<Object[]>) expectedRows;
      final List<Object[]> actualRowsList = (List<Object[]>) actualRows;

      final int sz = expectedRowsList.size();
      Assertions.assertEquals(sz, actualRowsList.size(), "number of rows");

      // Super slow for LinkedLists, but we don't expect those to be used here.
      // (They're generally forbidden in Druid except for special cases.)
      for (int i = 0; i < sz; i++) {
        Assertions.assertArrayEquals(expectedRowsList.get(i), actualRowsList.get(i), "row #" + i);
      }
    } else {
      // If they're not both Lists, we don't want to iterate them during equality checks, so do a non-deep check.
      // This might still return true if whatever class they are has another way of checking equality. But, usually we
      // expect this to return false.
      Assertions.assertEquals(expectedRows, actualRows, "rows");
    }
  }

}
