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

package org.apache.druid.segment.vector;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.projections.ClusteringVectorColumnSelectorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

class ConcatenatingVectorCursorTest
{
  private static final RowSignature CLUSTER_SIGNATURE = RowSignature.builder().add("tenant", ColumnType.STRING).build();

  private final Closer closer = Closer.create();

  @Test
  void testWalksTwoNonEmptyGroupsBackToBack()
  {
    FakeVectorCursorHolder a = new FakeVectorCursorHolder(List.of("a1", "a2"), 4);
    FakeVectorCursorHolder b = new FakeVectorCursorHolder(List.of("b1"), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(a), holderSupplier(b)),
        List.of(new Object[]{"acme"}, new Object[]{"globex"}),
        wrapper
    );

    VectorObjectSelector tenant = c.getColumnSelectorFactory().makeObjectSelector("tenant");
    VectorObjectSelector metric = c.getColumnSelectorFactory().makeObjectSelector("metric");

    // Group 1 vector: ["a1", "a2"]
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals(2, c.getCurrentVectorSize());
    Object[] tenantVec1 = tenant.getObjectVector();
    Object[] metricVec1 = metric.getObjectVector();
    Assertions.assertEquals("acme", tenantVec1[0]);
    Assertions.assertEquals("acme", tenantVec1[1]);
    Assertions.assertEquals("a1", metricVec1[0]);
    Assertions.assertEquals("a2", metricVec1[1]);
    c.advance();

    // Group 2 vector: ["b1"]
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals(1, c.getCurrentVectorSize());
    Object[] tenantVec2 = tenant.getObjectVector();
    Object[] metricVec2 = metric.getObjectVector();
    Assertions.assertEquals("globex", tenantVec2[0]);
    Assertions.assertEquals("b1", metricVec2[0]);
    c.advance();

    Assertions.assertTrue(c.isDone());
    Assertions.assertDoesNotThrow(closer::close);
    Assertions.assertTrue(a.closed);
    Assertions.assertTrue(b.closed);
  }

  @Test
  void testSkipsLeadingEmptyGroup()
  {
    FakeVectorCursorHolder empty = new FakeVectorCursorHolder(List.of(), 4);
    FakeVectorCursorHolder full = new FakeVectorCursorHolder(List.of("x"), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"placeholder"}
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(empty), holderSupplier(full)),
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper
    );

    VectorObjectSelector tenant = c.getColumnSelectorFactory().makeObjectSelector("tenant");
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals("b", tenant.getObjectVector()[0]);
    c.advance();
    Assertions.assertTrue(c.isDone());
    Assertions.assertDoesNotThrow(closer::close);
    Assertions.assertTrue(empty.closed);
    Assertions.assertTrue(full.closed);
  }

  @Test
  void testSkipsTrailingEmptyGroup()
  {
    FakeVectorCursorHolder full = new FakeVectorCursorHolder(List.of("x"), 4);
    FakeVectorCursorHolder empty = new FakeVectorCursorHolder(List.of(), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"a"}
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(full), holderSupplier(empty)),
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper
    );

    VectorObjectSelector tenant = c.getColumnSelectorFactory().makeObjectSelector("tenant");
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals("a", tenant.getObjectVector()[0]);
    c.advance();
    Assertions.assertTrue(c.isDone());
  }

  @Test
  void testAllEmptyGroups()
  {
    FakeVectorCursorHolder e1 = new FakeVectorCursorHolder(List.of(), 4);
    FakeVectorCursorHolder e2 = new FakeVectorCursorHolder(List.of(), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"placeholder"}
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(e1), holderSupplier(e2)),
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper
    );

    Assertions.assertTrue(c.isDone());
    Assertions.assertEquals(0, c.getCurrentVectorSize());
  }

  @Test
  void testSingleGroupDegenerateCase()
  {
    FakeVectorCursorHolder only = new FakeVectorCursorHolder(List.of("x", "y"), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"a"}
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(only)),
        List.<Object[]>of(new Object[]{"a"}),
        wrapper
    );

    VectorObjectSelector tenant = c.getColumnSelectorFactory().makeObjectSelector("tenant");
    VectorObjectSelector metric = c.getColumnSelectorFactory().makeObjectSelector("metric");

    Assertions.assertEquals(2, c.getCurrentVectorSize());
    Assertions.assertEquals("a", tenant.getObjectVector()[0]);
    Assertions.assertEquals("a", tenant.getObjectVector()[1]);
    Assertions.assertEquals("x", metric.getObjectVector()[0]);
    Assertions.assertEquals("y", metric.getObjectVector()[1]);
    c.advance();
    Assertions.assertTrue(c.isDone());
  }

  @Test
  void testPartialVectorAtGroupBoundary()
  {
    // Group A has 3 rows but max vector size is 4; its vector is partial (size = 3, not 4).
    FakeVectorCursorHolder a = new FakeVectorCursorHolder(List.of("a1", "a2", "a3"), 4);
    FakeVectorCursorHolder b = new FakeVectorCursorHolder(List.of("b1", "b2"), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"acme"}
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(a), holderSupplier(b)),
        List.of(new Object[]{"acme"}, new Object[]{"globex"}),
        wrapper
    );

    VectorObjectSelector tenant = c.getColumnSelectorFactory().makeObjectSelector("tenant");

    // Group A's vector is partial; 3 of max 4.
    Assertions.assertEquals(3, c.getCurrentVectorSize());
    Assertions.assertEquals("acme", tenant.getObjectVector()[0]);
    c.advance();

    // Group B's vector starts fresh; 2 rows.
    Assertions.assertFalse(c.isDone());
    Assertions.assertEquals(2, c.getCurrentVectorSize());
    Assertions.assertEquals("globex", tenant.getObjectVector()[0]);
    Assertions.assertEquals("globex", tenant.getObjectVector()[1]);
    c.advance();

    Assertions.assertTrue(c.isDone());
  }

  @Test
  void testCloserClosesAllOpenedHolders()
  {
    FakeVectorCursorHolder a = new FakeVectorCursorHolder(List.of("a1"), 4);
    FakeVectorCursorHolder b = new FakeVectorCursorHolder(List.of("b1"), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"x"}
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(a), holderSupplier(b)),
        List.of(new Object[]{"x"}, new Object[]{"y"}),
        wrapper
    );

    c.getColumnSelectorFactory();
    c.advance();
    c.advance();    // exhausts
    // Outer holder owns the closer; ConcatenatingVectorCursor itself is not Closeable.
    Assertions.assertDoesNotThrow(closer::close);
    Assertions.assertTrue(a.closed);
    Assertions.assertTrue(b.closed);
  }

  @Test
  void testGroupsAreOpenedLazilyOnTransitionNotEagerly()
  {
    final boolean[] secondOpened = {false};
    FakeVectorCursorHolder first = new FakeVectorCursorHolder(List.of("x"), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"a"}
    );

    List<Supplier<CursorHolder>> suppliers = new ArrayList<>();
    suppliers.add(holderSupplier(first));
    suppliers.add(() -> {
      secondOpened[0] = true;
      return new FakeVectorCursorHolder(List.of("y"), 4);
    });

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        suppliers,
        List.of(new Object[]{"a"}, new Object[]{"b"}),
        wrapper
    );

    c.getColumnSelectorFactory();
    Assertions.assertFalse(secondOpened[0]);

    c.advance();    // exhausts first → opens second
    Assertions.assertTrue(secondOpened[0]);
  }

  @Test
  void testMaxVectorSizeIsConfiguredValueAcrossAllStates()
  {
    // The configured maxVectorSize is a query-level constant; it must not be derived from the current group's
    // cursor (which may not exist before init or after exhaustion).
    final int configuredMaxVectorSize = 17;
    FakeVectorCursorHolder a = new FakeVectorCursorHolder(List.of("a1"), 4);

    ClusteringVectorColumnSelectorFactory wrapper = new ClusteringVectorColumnSelectorFactory(
        new FakeVectorFactory(List.of(), new int[]{0}, 4),
        CLUSTER_SIGNATURE,
        new Object[]{"acme"},
        configuredMaxVectorSize
    );

    ConcatenatingVectorCursor c = new ConcatenatingVectorCursor(
        List.of(holderSupplier(a)),
        List.<Object[]>of(new Object[]{"acme"}),
        wrapper
    );

    // Pre-init.
    Assertions.assertEquals(configuredMaxVectorSize, c.getMaxVectorSize());
    // Wrapper factory reports the same.
    Assertions.assertEquals(configuredMaxVectorSize, c.getColumnSelectorFactory().getMaxVectorSize());
    // After init.
    Assertions.assertEquals(configuredMaxVectorSize, c.getMaxVectorSize());
    c.advance();
    // Post-exhaustion.
    Assertions.assertTrue(c.isDone());
    Assertions.assertEquals(configuredMaxVectorSize, c.getMaxVectorSize());
  }

  private Supplier<CursorHolder> holderSupplier(FakeVectorCursorHolder h)
  {
    return Suppliers.memoize(() -> closer.register(h))::get;
  }

  private static class FakeVectorObjectSelector implements VectorObjectSelector
  {
    private final List<Object> rows;
    private final int[] offset;
    private final int maxVectorSize;
    private final Object[] buffer;

    FakeVectorObjectSelector(List<Object> rows, int[] offset, int maxVectorSize)
    {
      this.rows = rows;
      this.offset = offset;
      this.maxVectorSize = maxVectorSize;
      this.buffer = new Object[maxVectorSize];
    }

    @Override
    public Object[] getObjectVector()
    {
      final int size = Math.min(maxVectorSize, rows.size() - offset[0]);
      for (int i = 0; i < size; i++) {
        buffer[i] = rows.get(offset[0] + i);
      }
      return buffer;
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxVectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return Math.min(maxVectorSize, rows.size() - offset[0]);
    }
  }

  private static class FakeVectorFactory implements VectorColumnSelectorFactory
  {
    private final List<Object> metricRows;
    private final int[] offset;
    private final int maxVectorSize;

    FakeVectorFactory(List<Object> metricRows, int[] offset, int maxVectorSize)
    {
      this.metricRows = metricRows;
      this.offset = offset;
      this.maxVectorSize = maxVectorSize;
    }

    @Override
    public ReadableVectorInspector getReadableVectorInspector()
    {
      return new ReadableVectorInspector()
      {
        @Override
        public int getMaxVectorSize()
        {
          return maxVectorSize;
        }

        @Override
        public int getCurrentVectorSize()
        {
          return Math.min(maxVectorSize, metricRows.size() - offset[0]);
        }

        @Override
        public int getId()
        {
          return offset[0];
        }
      };
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxVectorSize;
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("not used in these tests");
    }

    @Override
    public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("not used in these tests");
    }

    @Override
    public VectorValueSelector makeValueSelector(String column)
    {
      throw new UnsupportedOperationException("not used in these tests");
    }

    @Override
    public VectorObjectSelector makeObjectSelector(String column)
    {
      if ("metric".equals(column)) {
        return new FakeVectorObjectSelector(metricRows, offset, maxVectorSize);
      }
      throw new UnsupportedOperationException("unknown column [" + column + "]");
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }

  private static class FakeVectorCursor implements VectorCursor
  {
    private final List<Object> rows;
    private final int maxVectorSize;
    private final int[] offset = new int[]{0};
    private final FakeVectorFactory factory;

    FakeVectorCursor(List<Object> rows, int maxVectorSize)
    {
      this.rows = rows;
      this.maxVectorSize = maxVectorSize;
      this.factory = new FakeVectorFactory(rows, offset, maxVectorSize);
    }

    @Override
    public VectorColumnSelectorFactory getColumnSelectorFactory()
    {
      return factory;
    }

    @Override
    public void advance()
    {
      offset[0] += getCurrentVectorSize();
    }

    @Override
    public boolean isDone()
    {
      return offset[0] >= rows.size();
    }

    @Override
    public void reset()
    {
      offset[0] = 0;
    }

    @Override
    public int getMaxVectorSize()
    {
      return maxVectorSize;
    }

    @Override
    public int getCurrentVectorSize()
    {
      return Math.min(maxVectorSize, rows.size() - offset[0]);
    }
  }

  private static class FakeVectorCursorHolder implements CursorHolder
  {
    private final List<Object> rows;
    private final int maxVectorSize;
    boolean closed;

    FakeVectorCursorHolder(List<Object> rows, int maxVectorSize)
    {
      this.rows = rows;
      this.maxVectorSize = maxVectorSize;
    }

    @Override
    public Cursor asCursor()
    {
      throw new UnsupportedOperationException("vector-only holder for these tests");
    }

    @Override
    public VectorCursor asVectorCursor()
    {
      return new FakeVectorCursor(rows, maxVectorSize);
    }

    @Override
    public boolean canVectorize()
    {
      return true;
    }

    @Override
    public List<OrderBy> getOrdering()
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
