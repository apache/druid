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

package org.apache.druid.segment.join;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.SegmentReference;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.join.filter.rewrite.JoinFilterRewriteConfig;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HashJoinSegmentTest extends InitializedNullHandlingTest
{
  private static final JoinFilterRewriteConfig DEFAULT_JOIN_FILTER_REWRITE_CONFIG =
      new JoinFilterRewriteConfig(
          true,
          true,
          true,
          QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
      );

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private QueryableIndexSegment baseSegment;
  private ReferenceCountingSegment referencedSegment;
  private HashJoinSegment hashJoinSegment;

  private int allReferencesAcquireCount;
  private int allReferencesCloseCount;
  private int referencedSegmentAcquireCount;
  private int referencedSegmentClosedCount;
  private int indexedTableJoinableReferenceAcquireCount;
  private int indexedTableJoinableReferenceCloseCount;
  private boolean j0Closed;
  private boolean j1Closed;

  @Before
  public void setUp() throws IOException
  {
    allReferencesAcquireCount = 0;
    allReferencesCloseCount = 0;
    referencedSegmentAcquireCount = 0;
    referencedSegmentClosedCount = 0;
    indexedTableJoinableReferenceAcquireCount = 0;
    indexedTableJoinableReferenceCloseCount = 0;
    j0Closed = false;
    j1Closed = false;

    baseSegment = new QueryableIndexSegment(
        JoinTestHelper.createFactIndexBuilder(temporaryFolder.newFolder()).buildMMappedIndex(),
        SegmentId.dummy("facts")
    );

    List<JoinableClause> joinableClauses = ImmutableList.of(
        new JoinableClause(
            "j0.",
            new IndexedTableJoinable(JoinTestHelper.createCountriesIndexedTable())
            {
              @Override
              public Optional<Closeable> acquireReferences()
              {
                if (!j0Closed) {
                  indexedTableJoinableReferenceAcquireCount++;
                  Closer closer = Closer.create();
                  closer.register(() -> indexedTableJoinableReferenceCloseCount++);
                  return Optional.of(closer);
                }
                return Optional.empty();
              }
            },
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("1", "j0.", ExprMacroTable.nil())
        ),
        new JoinableClause(
            "j1.",
            new IndexedTableJoinable(JoinTestHelper.createRegionsIndexedTable())
            {
              @Override
              public Optional<Closeable> acquireReferences()
              {
                if (!j1Closed) {
                  indexedTableJoinableReferenceAcquireCount++;
                  Closer closer = Closer.create();
                  closer.register(() -> indexedTableJoinableReferenceCloseCount++);
                  return Optional.of(closer);
                }
                return Optional.empty();
              }
            },
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("1", "j1.", ExprMacroTable.nil())
        )
    );

    referencedSegment = ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment);
    SegmentReference testWrapper = new SegmentReference()
    {
      @Override
      public Optional<Closeable> acquireReferences()
      {
        Closer closer = Closer.create();
        return referencedSegment.acquireReferences().map(closeable -> {
          referencedSegmentAcquireCount++;
          closer.register(closeable);
          closer.register(() -> referencedSegmentClosedCount++);
          return closer;
        });
      }

      @Override
      public SegmentId getId()
      {
        return referencedSegment.getId();
      }

      @Override
      public Interval getDataInterval()
      {
        return referencedSegment.getDataInterval();
      }

      @Nullable
      @Override
      public QueryableIndex asQueryableIndex()
      {
        return referencedSegment.asQueryableIndex();
      }

      @Override
      public StorageAdapter asStorageAdapter()
      {
        return referencedSegment.asStorageAdapter();
      }

      @Override
      public void close()
      {
        referencedSegment.close();
      }
    };
    hashJoinSegment = new HashJoinSegment(
        testWrapper,
        joinableClauses,
        null
    )
    {
      @Override
      public Optional<Closeable> acquireReferences()
      {
        Closer closer = Closer.create();
        return super.acquireReferences().map(closeable -> {
          allReferencesAcquireCount++;
          closer.register(closeable);
          closer.register(() -> allReferencesCloseCount++);
          return closer;
        });
      }
    };
  }

  @Test
  public void test_constructor_noClauses()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("'clauses' is empty, no need to create HashJoinSegment");

    List<JoinableClause> joinableClauses = ImmutableList.of();

    final HashJoinSegment ignored = new HashJoinSegment(
        ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
        joinableClauses,
        null
    );
  }

  @Test
  public void test_getId()
  {
    Assert.assertEquals(baseSegment.getId(), hashJoinSegment.getId());
  }

  @Test
  public void test_getDataInterval()
  {
    Assert.assertEquals(baseSegment.getDataInterval(), hashJoinSegment.getDataInterval());
  }

  @Test
  public void test_asQueryableIndex()
  {
    Assert.assertNull(hashJoinSegment.asQueryableIndex());
  }

  @Test
  public void test_asStorageAdapter()
  {
    Assert.assertThat(
        hashJoinSegment.asStorageAdapter(),
        CoreMatchers.instanceOf(HashJoinSegmentStorageAdapter.class)
    );
  }

  @Test
  public void testJoinableClausesAreClosedWhenReferencesUsed() throws IOException
  {
    Assert.assertFalse(referencedSegment.isClosed());

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assert.assertTrue(maybeCloseable.isPresent());

    Assert.assertEquals(1, referencedSegmentAcquireCount);
    Assert.assertEquals(2, indexedTableJoinableReferenceAcquireCount);
    Assert.assertEquals(1, allReferencesAcquireCount);
    Assert.assertEquals(0, referencedSegmentClosedCount);
    Assert.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assert.assertEquals(0, allReferencesCloseCount);

    Closeable closer = maybeCloseable.get();
    closer.close();

    Assert.assertFalse(referencedSegment.isClosed());
    Assert.assertEquals(1, referencedSegmentClosedCount);
    Assert.assertEquals(2, indexedTableJoinableReferenceCloseCount);
    Assert.assertEquals(1, allReferencesCloseCount);

  }

  @Test
  public void testJoinableClausesClosedIfSegmentIsAlreadyClosed()
  {
    Assert.assertFalse(referencedSegment.isClosed());

    referencedSegment.close();
    Assert.assertTrue(referencedSegment.isClosed());

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assert.assertFalse(maybeCloseable.isPresent());
    Assert.assertEquals(0, referencedSegmentAcquireCount);
    Assert.assertEquals(0, indexedTableJoinableReferenceAcquireCount);
    Assert.assertEquals(0, allReferencesAcquireCount);
    Assert.assertEquals(0, referencedSegmentClosedCount);
    Assert.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assert.assertEquals(0, allReferencesCloseCount);
  }

  @Test
  public void testJoinableClausesClosedIfJoinableZeroIsAlreadyClosed()
  {
    Assert.assertFalse(referencedSegment.isClosed());
    j0Closed = true;

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assert.assertFalse(maybeCloseable.isPresent());
    Assert.assertEquals(1, referencedSegmentAcquireCount);
    Assert.assertEquals(0, indexedTableJoinableReferenceAcquireCount);
    Assert.assertEquals(0, allReferencesAcquireCount);
    Assert.assertEquals(1, referencedSegmentClosedCount);
    Assert.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assert.assertEquals(0, allReferencesCloseCount);
  }

  @Test
  public void testJoinableClausesClosedIfJoinableOneIsAlreadyClosed()
  {
    Assert.assertFalse(referencedSegment.isClosed());
    j1Closed = true;

    Optional<Closeable> maybeCloseable = hashJoinSegment.acquireReferences();
    Assert.assertFalse(maybeCloseable.isPresent());
    Assert.assertEquals(1, referencedSegmentAcquireCount);
    Assert.assertEquals(1, indexedTableJoinableReferenceAcquireCount);
    Assert.assertEquals(0, allReferencesAcquireCount);
    Assert.assertEquals(1, referencedSegmentClosedCount);
    Assert.assertEquals(1, indexedTableJoinableReferenceCloseCount);
    Assert.assertEquals(0, allReferencesCloseCount);
  }
}
