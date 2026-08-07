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
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusteringColumnSelectorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class ConcatenatingCursorTest
{
  private static final RowSignature CLUSTER_SIGNATURE = RowSignature.builder().add("tenant", ColumnType.STRING).build();

  private final Closer closer = Closer.create();

  @Test
  void testWalksTwoNonEmptyGroupsBackToBack()
  {
    FakeCursorHolder a = new FakeCursorHolder(List.of("a1", "a2"));
    FakeCursorHolder b = new FakeCursorHolder(List.of("b1"));

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );

    ConcatenatingCursor c = new ConcatenatingCursor(
        List.of(holderSupplier(a), holderSupplier(b)),
        List.of(new Object[]{"acme"}, new Object[]{"globex"}),
        wrapper,
        Map.of()
    );

    ColumnValueSelector tenant = c.getColumnSelectorFactory().makeColumnValueSelector("tenant");
    ColumnValueSelector metric = c.getColumnSelectorFactory().makeColumnValueSelector("metric");

    // Group 1: tenant=acme, metric=a1
    Assertions.assertEquals("acme", tenant.getObject());
    Assertions.assertEquals("a1", metric.getObject());
    c.advance();

    // Group 1: tenant=acme, metric=a2
    Assertions.assertEquals("acme", tenant.getObject());
    Assertions.assertEquals("a2", metric.getObject());
    c.advance();

    // Group transition → tenant=globex, metric=b1
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals("globex", tenant.getObject());
    Assertions.assertEquals("b1", metric.getObject());
    c.advance();

    Assertions.assertTrue(c.isDone());
    Assertions.assertDoesNotThrow(closer::close);
    Assertions.assertTrue(a.closed);
    Assertions.assertTrue(b.closed);
  }

  @Test
  void testSkipsLeadingEmptyGroup()
  {
    FakeCursorHolder empty = new FakeCursorHolder(List.of());
    FakeCursorHolder full = new FakeCursorHolder(List.of("x"));

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"placeholder"}
    );

    ConcatenatingCursor c = new ConcatenatingCursor(
        List.of(holderSupplier(empty), holderSupplier(full)),
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper,
        Map.of()
    );

    ColumnValueSelector tenant = c.getColumnSelectorFactory().makeColumnValueSelector("tenant");
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals("b", tenant.getObject());
    c.advance();
    Assertions.assertTrue(c.isDone());
    Assertions.assertDoesNotThrow(closer::close);
    Assertions.assertTrue(empty.closed);
    Assertions.assertTrue(full.closed);
  }

  @Test
  void testSkipsTrailingEmptyGroup()
  {
    FakeCursorHolder full = new FakeCursorHolder(List.of("x"));
    FakeCursorHolder empty = new FakeCursorHolder(List.of());

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"a"}
    );

    ConcatenatingCursor c = new ConcatenatingCursor(
        List.of(holderSupplier(full), holderSupplier(empty)),
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper,
        Map.of()
    );

    ColumnValueSelector tenant = c.getColumnSelectorFactory().makeColumnValueSelector("tenant");
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals("a", tenant.getObject());
    c.advance();
    Assertions.assertTrue(c.isDone());
  }

  @Test
  void testAllEmptyGroups()
  {
    FakeCursorHolder e1 = new FakeCursorHolder(List.of());
    FakeCursorHolder e2 = new FakeCursorHolder(List.of());

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"placeholder"}
    );

    ConcatenatingCursor c = new ConcatenatingCursor(
        List.of(holderSupplier(e1), holderSupplier(e2)),
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper,
        Map.of()
    );

    Assertions.assertTrue(c.isDone());
  }

  @Test
  void testSingleGroupDegenerateCase()
  {
    FakeCursorHolder only = new FakeCursorHolder(List.of("x", "y"));

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"a"}
    );

    ConcatenatingCursor c = new ConcatenatingCursor(
        List.of(holderSupplier(only)),
        List.<Object[]>of(new Object[]{"a"}),
        wrapper,
        Map.of()
    );

    ColumnValueSelector tenant = c.getColumnSelectorFactory().makeColumnValueSelector("tenant");
    ColumnValueSelector metric = c.getColumnSelectorFactory().makeColumnValueSelector("metric");

    Assertions.assertEquals("a", tenant.getObject());
    Assertions.assertEquals("x", metric.getObject());
    c.advance();
    Assertions.assertEquals("a", tenant.getObject());
    Assertions.assertEquals("y", metric.getObject());
    c.advance();
    Assertions.assertTrue(c.isDone());
  }

  @Test
  void testCloseClosesAllOpenedHolders()
  {
    FakeCursorHolder a = new FakeCursorHolder(List.of("a1"));
    FakeCursorHolder b = new FakeCursorHolder(List.of("b1"));

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"x"}
    );

    ConcatenatingCursor c = new ConcatenatingCursor(
        List.of(holderSupplier(a), holderSupplier(b)),
        List.of(new Object[]{"x"}, new Object[]{"y"}),
        wrapper,
        Map.of()
    );

    // Walk through, opens both holders.
    c.getColumnSelectorFactory();
    c.advance();
    c.advance();   // exhausts
    Assertions.assertDoesNotThrow(closer::close);
    Assertions.assertTrue(a.closed);
    Assertions.assertTrue(b.closed);
  }

  @Test
  void testGroupsAreOpenedLazilyOnTransitionNotEagerly()
  {
    final boolean[] secondOpened = {false};
    FakeCursorHolder first = new FakeCursorHolder(List.of("x"));

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"a"}
    );

    List<Supplier<CursorHolder>> suppliers = new ArrayList<>();
    suppliers.add(holderSupplier(first));
    suppliers.add(() -> {
      secondOpened[0] = true;
      return new FakeCursorHolder(List.of("y"));
    });

    ConcatenatingCursor c = new ConcatenatingCursor(
        suppliers,
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper,
        Map.of()
    );

    // Init opens first group only. Second is still untouched.
    c.getColumnSelectorFactory();
    Assertions.assertFalse(secondOpened[0]);

    c.advance();   // exhausts first → opens second
    Assertions.assertTrue(secondOpened[0]);
  }

  @Test
  void testResetReIteratesWithoutClosingHolders()
  {
    // reset() must not close per-group holders, those are owned by the outer Closer. After reset, the cursor
    // should re-iterate from the start; the suppliers are memoized so they return the same holders, and
    // CursorHolder.asCursor() returns a fresh cursor each call.
    FakeCursorHolder a = new FakeCursorHolder(List.of("a1", "a2"));
    FakeCursorHolder b = new FakeCursorHolder(List.of("b1"));

    ClusteringColumnSelectorFactory wrapper = new ClusteringColumnSelectorFactory(
        new FakeFactory(List.of(), new int[]{0}),
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );

    ConcatenatingCursor c = new ConcatenatingCursor(
        List.of(holderSupplier(a), holderSupplier(b)),
        List.of(new Object[]{"acme"}, new Object[]{"globex"}),
        wrapper,
        Map.of()
    );

    ColumnValueSelector tenant = c.getColumnSelectorFactory().makeColumnValueSelector("tenant");
    ColumnValueSelector metric = c.getColumnSelectorFactory().makeColumnValueSelector("metric");

    // Walk to exhaustion.
    Assertions.assertEquals("a1", metric.getObject());
    c.advance();
    Assertions.assertEquals("a2", metric.getObject());
    c.advance();
    Assertions.assertEquals("b1", metric.getObject());
    c.advance();
    Assertions.assertTrue(c.isDone());

    // Holders MUST remain open across reset; only the outer Closer closes them.
    Assertions.assertFalse(a.closed);
    Assertions.assertFalse(b.closed);

    c.reset();

    // Re-walk from the start, exercising both groups again.
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals("acme", tenant.getObject());
    Assertions.assertEquals("a1", metric.getObject());
    c.advance();
    Assertions.assertEquals("a2", metric.getObject());
    c.advance();
    Assertions.assertEquals("globex", tenant.getObject());
    Assertions.assertEquals("b1", metric.getObject());
    c.advance();
    Assertions.assertTrue(c.isDone());
  }

  private Supplier<CursorHolder> holderSupplier(FakeCursorHolder h)
  {
    return Suppliers.memoize(() -> closer.register(h))::get;
  }

  private static class FakeStringValueSelector implements ColumnValueSelector<Object>
  {
    private final List<String> rows;
    private final int[] offset;

    FakeStringValueSelector(List<String> rows, int[] offset)
    {
      this.rows = rows;
      this.offset = offset;
    }

    @Override
    public double getDouble()
    {
      return 0;
    }

    @Override
    public float getFloat()
    {
      return 0;
    }

    @Override
    public long getLong()
    {
      return 0;
    }

    @Override
    public boolean isNull()
    {
      return rows.get(offset[0]) == null;
    }

    @Nullable
    @Override
    public Object getObject()
    {
      return rows.get(offset[0]);
    }

    @Override
    public Class<?> classOfObject()
    {
      return String.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }
  }

  private static class FakeFactory implements ColumnSelectorFactory
  {
    private final List<String> metricRows;
    private final int[] offset;

    FakeFactory(List<String> metricRows, int[] offset)
    {
      this.metricRows = metricRows;
      this.offset = offset;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("not used in these tests");
    }

    @Override
    public ColumnValueSelector makeColumnValueSelector(String columnName)
    {
      if ("metric".equals(columnName)) {
        return new FakeStringValueSelector(metricRows, offset);
      }
      return NilColumnValueSelector.instance();
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }

  private static class FakeCursor implements Cursor
  {
    private final List<String> rows;
    private final int[] offset = new int[]{0};
    private final FakeFactory factory;

    FakeCursor(List<String> rows)
    {
      this.rows = rows;
      this.factory = new FakeFactory(rows, offset);
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      return factory;
    }

    @Override
    public void advance()
    {
      offset[0]++;
    }

    @Override
    public void advanceUninterruptibly()
    {
      offset[0]++;
    }

    @Override
    public boolean isDone()
    {
      return offset[0] >= rows.size();
    }

    @Override
    public boolean isDoneOrInterrupted()
    {
      return isDone();
    }

    @Override
    public void reset()
    {
      offset[0] = 0;
    }
  }

  private static class FakeCursorHolder implements CursorHolder
  {
    private final List<String> rows;
    private boolean closed;

    FakeCursorHolder(List<String> rows)
    {
      this.rows = rows;
    }

    @Override
    public Cursor asCursor()
    {
      return new FakeCursor(rows);
    }

    @Override
    public boolean canVectorize()
    {
      return false;
    }

    @Override
    public List<org.apache.druid.query.OrderBy> getOrdering()
    {
      return List.of();
    }

    @Override
    public void close()
    {
      closed = true;
    }
  }
}
