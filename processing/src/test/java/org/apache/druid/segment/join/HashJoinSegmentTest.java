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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.filter.JoinFilterAnalyzer;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.filter.JoinableClauses;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HashJoinSegmentTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private QueryableIndexSegment baseSegment;
  private ReferenceCountingSegment wrappedBaseSegment;
  private HashJoinSegment hashJoinSegment;

  private int hashJoinSegmentReferenceCloseCount;
  private int indexedTableJoinableReferenceCloseCount;

  @BeforeClass
  public static void setUpStatic()
  {
    NullHandling.initializeForTests();
  }

  @Before
  public void setUp() throws IOException
  {
    hashJoinSegmentReferenceCloseCount = 0;
    indexedTableJoinableReferenceCloseCount = 0;

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
              public void close() throws IOException
              {
                indexedTableJoinableReferenceCloseCount++;
                super.close();
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
              public void close() throws IOException
              {
                indexedTableJoinableReferenceCloseCount++;
                super.close();
              }
            },
            JoinType.LEFT,
            JoinConditionAnalysis.forExpression("1", "j1.", ExprMacroTable.nil())
        )
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        JoinableClauses.fromList(joinableClauses),
        VirtualColumns.EMPTY,
        null,
        true,
        true,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
    );

    wrappedBaseSegment = ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment);
    hashJoinSegment = new HashJoinSegment(
        wrappedBaseSegment,
        joinableClauses,
        joinFilterPreAnalysis
    )
    {
      @Override
      public Optional<Closer> acquireReferences()
      {
        Optional<Closer> closer = super.acquireReferences();
        closer.map(c -> c.register(() -> hashJoinSegmentReferenceCloseCount++));
        return closer;
      }
    };
  }

  @Test
  public void test_constructor_noClauses()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("'clauses' is empty, no need to create HashJoinSegment");

    List<JoinableClause> joinableClauses = ImmutableList.of();

    JoinFilterPreAnalysis joinFilterPreAnalysis = JoinFilterAnalyzer.computeJoinFilterPreAnalysis(
        JoinableClauses.fromList(joinableClauses),
        VirtualColumns.EMPTY,
        null,
        true,
        true,
        true,
        QueryContexts.DEFAULT_ENABLE_JOIN_FILTER_REWRITE_MAX_SIZE
    );

    final HashJoinSegment ignored = new HashJoinSegment(
        ReferenceCountingSegment.wrapRootGenerationSegment(baseSegment),
        joinableClauses,
        joinFilterPreAnalysis
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
    Assert.assertFalse(wrappedBaseSegment.isClosed());
    Assert.assertEquals(0, hashJoinSegmentReferenceCloseCount);
    Assert.assertEquals(0, indexedTableJoinableReferenceCloseCount);

    Optional<Closer> maybeCloser = hashJoinSegment.acquireReferences();
    Assert.assertTrue(maybeCloser.isPresent());

    Closer closer = maybeCloser.get();
    closer.close();
    Assert.assertFalse(wrappedBaseSegment.isClosed());
    Assert.assertEquals(1, hashJoinSegmentReferenceCloseCount);
    Assert.assertEquals(2, indexedTableJoinableReferenceCloseCount);
  }

  @Test
  public void testJoinableClausesClosedIfSegmentIsAlreadyClosed() throws IOException
  {
    Assert.assertFalse(wrappedBaseSegment.isClosed());
    Assert.assertEquals(0, hashJoinSegmentReferenceCloseCount);
    Assert.assertEquals(0, indexedTableJoinableReferenceCloseCount);

    wrappedBaseSegment.close();
    Assert.assertTrue(wrappedBaseSegment.isClosed());

    Optional<Closer> maybeCloser = hashJoinSegment.acquireReferences();
    Assert.assertFalse(maybeCloser.isPresent());
    Assert.assertEquals(0, hashJoinSegmentReferenceCloseCount);
    // joinables still should have been closed by acuireReferences failing to produce a closer
    Assert.assertEquals(2, indexedTableJoinableReferenceCloseCount);
  }
}
