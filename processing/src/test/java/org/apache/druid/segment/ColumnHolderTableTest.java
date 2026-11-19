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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.segment.column.BaseColumnHolder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.projections.ConstantTimeColumn;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ColumnHolderTableTest
{
  @Test
  public void testGet()
  {
    final BaseColumnHolder a = makeHolder();
    final BaseColumnHolder b = makeHolder();
    final AtomicInteger loads = new AtomicInteger();

    final ColumnHolderTable table = ColumnHolderTable.builder()
                                                     .put("a", a)
                                                     .putSupplier(
                                                         "b",
                                                         Suppliers.memoize(() -> {
                                                           loads.incrementAndGet();
                                                           return b;
                                                         })
                                                     )
                                                     .build();

    Assertions.assertEquals(List.of("a", "b"), table.getColumnNames());
    Assertions.assertFalse(table.isEmpty());
    Assertions.assertTrue(table.contains("b"));
    Assertions.assertFalse(table.contains("c"));
    Assertions.assertEquals(0, loads.get());

    Assertions.assertSame(a, table.get("a"));
    Assertions.assertSame(b, table.get("b"));
    Assertions.assertSame(b, table.get("b"));
    Assertions.assertNull(table.get("c"));
    Assertions.assertEquals(1, loads.get());
  }

  @Test
  public void testGetWithManyColumns()
  {
    final List<String> names = new ArrayList<>();
    final List<BaseColumnHolder> holders = new ArrayList<>();
    final ColumnHolderTable.Builder builder = ColumnHolderTable.builder();
    for (int i = 99; i >= 0; i--) {
      // reverse order, with gaps, so names are neither inserted sorted nor contiguous
      final String name = "c" + (i * 2);
      final BaseColumnHolder holder = makeHolder();
      names.add(name);
      holders.add(holder);
      builder.put(name, holder);
    }
    final ColumnHolderTable table = builder.build();

    Assertions.assertEquals(names, table.getColumnNames());
    for (int i = 0; i < names.size(); i++) {
      Assertions.assertSame(holders.get(i), table.get(names.get(i)), names.get(i));
    }
    Assertions.assertNull(table.get("c1"));
    Assertions.assertNull(table.get("a"));
    Assertions.assertNull(table.get("d"));
    Assertions.assertNull(table.get(null));
    Assertions.assertFalse(table.contains("c197"));
  }

  @Test
  public void testLayoutSharedByTablesWithSameColumns()
  {
    final ColumnHolderTable table1 = ColumnHolderTable.builder().put("a", makeHolder()).put("b", makeHolder()).build();
    final ColumnHolderTable table2 = ColumnHolderTable.builder().put("a", makeHolder()).put("b", makeHolder()).build();
    final ColumnHolderTable table3 = ColumnHolderTable.builder().put("b", makeHolder()).put("a", makeHolder()).build();

    Assertions.assertSame(table1.getColumnNames(), table2.getColumnNames());
    Assertions.assertNotSame(table1.getColumnNames(), table3.getColumnNames());
    Assertions.assertNotSame(table1.get("a"), table2.get("a"));
  }

  @Test
  public void testRename()
  {
    final BaseColumnHolder t = makeHolder();
    final ColumnHolderTable.Builder builder = ColumnHolderTable.builder().put("t", t).put("x", makeHolder());
    builder.rename("t", ColumnHolder.TIME_COLUMN_NAME);
    final ColumnHolderTable table = builder.build();

    Assertions.assertEquals(List.of("x", ColumnHolder.TIME_COLUMN_NAME), table.getColumnNames());
    Assertions.assertSame(t, table.get(ColumnHolder.TIME_COLUMN_NAME));
    Assertions.assertNull(table.get("t"));
  }

  @Test
  public void testToBuilder()
  {
    final BaseColumnHolder a = makeHolder();
    final BaseColumnHolder b = makeHolder();
    final BaseColumnHolder c = makeHolder();
    final AtomicInteger loads = new AtomicInteger();

    final ColumnHolderTable table = ColumnHolderTable.builder()
                                                     .put("a", a)
                                                     .putSupplier(
                                                         "b",
                                                         Suppliers.memoize(() -> {
                                                           loads.incrementAndGet();
                                                           return b;
                                                         })
                                                     )
                                                     .build();

    final ColumnHolderTable extended1 = table.toBuilder().put("c", c).build();
    final ColumnHolderTable extended2 = table.toBuilder().put("c", makeHolder()).build();
    Assertions.assertEquals(List.of("a", "b", "c"), extended1.getColumnNames());
    Assertions.assertSame(extended1.getColumnNames(), extended2.getColumnNames());
    Assertions.assertFalse(table.contains("c"));
    Assertions.assertEquals(0, loads.get());

    Assertions.assertSame(a, extended1.get("a"));
    Assertions.assertSame(b, extended1.get("b"));
    Assertions.assertSame(c, extended1.get("c"));
    Assertions.assertSame(b, table.get("b"));
    Assertions.assertEquals(1, loads.get());

    final BaseColumnHolder a2 = makeHolder();
    final ColumnHolderTable replaced = table.toBuilder().put("a", a2).build();
    Assertions.assertSame(table.getColumnNames(), replaced.getColumnNames());
    Assertions.assertSame(a2, replaced.get("a"));
    Assertions.assertSame(a, table.get("a"));
  }

  @Test
  public void testFromSupplierMapAndToSupplierMap()
  {
    final BaseColumnHolder a = makeHolder();
    final BaseColumnHolder b = makeHolder();
    final Map<String, Supplier<BaseColumnHolder>> map = new LinkedHashMap<>();
    map.put("b", () -> b);
    map.put("a", () -> a);

    final ColumnHolderTable table = ColumnHolderTable.fromSupplierMap(map);
    Assertions.assertEquals(List.of("b", "a"), table.getColumnNames());
    Assertions.assertSame(a, table.get("a"));
    Assertions.assertSame(b, table.get("b"));
    Assertions.assertSame(
        table.getColumnNames(),
        ColumnHolderTable.builder().put("b", b).put("a", a).build().getColumnNames()
    );

    final Map<String, Supplier<BaseColumnHolder>> roundTrip =
        ColumnHolderTable.builder().put("a", a).putSupplier("b", map.get("b")).build().toSupplierMap();
    Assertions.assertEquals(List.of("a", "b"), List.copyOf(roundTrip.keySet()));
    Assertions.assertSame(a, roundTrip.get("a").get());
    Assertions.assertSame(b, roundTrip.get("b").get());
  }

  @Test
  public void testEmpty()
  {
    final ColumnHolderTable table = ColumnHolderTable.builder().build();
    Assertions.assertTrue(table.isEmpty());
    Assertions.assertEquals(List.of(), table.getColumnNames());
    Assertions.assertNull(table.get(ColumnHolder.TIME_COLUMN_NAME));
  }

  private static BaseColumnHolder makeHolder()
  {
    return ConstantTimeColumn.makeConstantTimeSupplier(10, 0L).get();
  }
}
