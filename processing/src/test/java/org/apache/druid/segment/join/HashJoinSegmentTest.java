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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.MaxIngestedEventTimeInspector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountedObjectProvider;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMapFunction;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.WrappedSegment;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class HashJoinSegmentTest extends InitializedNullHandlingTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = new TemporaryFolderExtension();

  private QueryableIndexSegment baseSegment;
  private ReferenceCountedSegmentProvider baseSegmentRef;
  private ReferenceCountedObjectProvider<Segment> referencedSegment;
  private List<JoinableClause> joinableClauses;
  private Closer closer;

  private int allReferencesAcquireCount;
  private int allReferencesCloseCount;
  private int referencedSegmentAcquireCount;
  private int referencedSegmentClosedCount;
  private int indexedTableJoinableReferenceAcquireCount;
  private int indexedTableJoinableReferenceCloseCount;
  private boolean j0Closed;
  private boolean j1Closed;

  @BeforeEach
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

    joinableClauses = ImmutableList.of(
        new JoinableClause(
            "j0.",
            new IndexedTableJoinable(JoinTestHelper.createCountriesIndexedTable())
            {
              @Override
              public Optional<Closeable> acquireReference()
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
              public Optional<Closeable> acquireReference()
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

    closer = Closer.create();
    baseSegmentRef = closer.register(ReferenceCountedSegmentProvider.of(baseSegment));
    referencedSegment = () ->
        baseSegmentRef.acquireReference()
                      .map(
                          s -> {
                            final Closer closer = Closer.create();
                            referencedSegmentAcquireCount++;
                            closer.register(s);
                            closer.register(() -> referencedSegmentClosedCount++);
                            return new WrappedSegment(s)
                            {
                              @Nullable
                              @Override
                              public <T> T as(@Nonnull Class<T> clazz)
                              {
                                return s.as(clazz);
                              }

                              @Override
                              public void close() throws IOException
                              {
                                closer.close();
                              }
                            };
                          }
                      );
  }

  @AfterEach
  public void teardown() throws IOException
  {
    closer.close();
  }

  private Optional<Segment> makeJoinSegment(List<JoinableClause> joinableClauses, Filter filter, JoinFilterPreAnalysis preAnalysis)
  {
    final SegmentMapFunction segmentMapFunction = JoinDataSource.createSegmentMapFunction(
        joinableClauses,
        filter,
        preAnalysis,
        SegmentMapFunction.IDENTITY
    );
    return segmentMapFunction.apply(referencedSegment.acquireReference())
                             .map(s -> {
                               final Closer closer = Closer.create();
                               allReferencesAcquireCount++;
                               closer.register(s);
                               closer.register(() -> allReferencesCloseCount++);
                               return new WrappedSegment(s)
                               {
                                 @Nullable
                                 @Override
                                 public <T> T as(@Nonnull Class<T> clazz)
                                 {
                                   return s.as(clazz);
                                 }

                                 @Override
                                 public void close() throws IOException
                                 {
                                   closer.close();
                                 }
                               };
                             });
  }

  private Segment makeJoinSegment()
  {
    return closer.register(makeJoinSegment(joinableClauses, null, null).orElseThrow());
  }


  @Test
  public void test_constructor_noClauses()
  {
    final List<JoinableClause> empty = ImmutableList.of();
    Throwable t = Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new HashJoinSegment(
            referencedSegment.acquireReference().orElseThrow(),
            null,
            empty,
            null,
            () -> allReferencesCloseCount--
        )
    );
    Assertions.assertEquals("'clauses' and 'baseFilter' are both empty, no need to create HashJoinSegment", t.getMessage());
    Assertions.assertEquals(0, allReferencesAcquireCount);
  }

  @Test
  public void test_getId()
  {
    Assertions.assertEquals(baseSegment.getId(), makeJoinSegment().getId());
  }

  @Test
  public void test_getDataInterval()
  {
    Assertions.assertEquals(baseSegment.getDataInterval(), makeJoinSegment().getDataInterval());
  }

  @Test
  public void test_asQueryableIndex()
  {
    Assertions.assertNull(makeJoinSegment().as(QueryableIndex.class));
  }

  @Test
  public void test_asCursorFactory()
  {
    MatcherAssert.assertThat(
        makeJoinSegment().as(CursorFactory.class),
        CoreMatchers.instanceOf(HashJoinSegmentCursorFactory.class)
    );
  }

  @Test
  public void testJoinableClausesAreClosedWhenReferencesUsed() throws IOException
  {
    Assertions.assertFalse(baseSegmentRef.isClosed());

    Optional<Segment> maybeCloseable = makeJoinSegment(joinableClauses, null, null);
    Assertions.assertTrue(maybeCloseable.isPresent());

    Assertions.assertEquals(1, referencedSegmentAcquireCount);
    Assertions.assertEquals(2, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(1, allReferencesAcquireCount);
    Assertions.assertEquals(0, referencedSegmentClosedCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);

    Closeable closer = maybeCloseable.get();
    closer.close();

    Assertions.assertFalse(baseSegmentRef.isClosed());
    Assertions.assertEquals(1, referencedSegmentClosedCount);
    Assertions.assertEquals(2, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(1, allReferencesCloseCount);

  }

  @Test
  public void testJoinableClausesClosedIfSegmentIsAlreadyClosed()
  {
    Assertions.assertFalse(baseSegmentRef.isClosed());

    baseSegmentRef.close();
    Assertions.assertTrue(baseSegmentRef.isClosed());

    Optional<Segment> maybeCloseable = makeJoinSegment(joinableClauses, null, null);
    Assertions.assertFalse(maybeCloseable.isPresent());
    Assertions.assertEquals(0, referencedSegmentAcquireCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(0, allReferencesAcquireCount);
    Assertions.assertEquals(0, referencedSegmentClosedCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);
  }

  @Test
  public void testJoinableClausesClosedIfJoinableZeroIsAlreadyClosed()
  {
    Assertions.assertFalse(baseSegmentRef.isClosed());
    j0Closed = true;

    Optional<Segment> maybeCloseable = makeJoinSegment(joinableClauses, null, null);
    Assertions.assertFalse(maybeCloseable.isPresent());
    Assertions.assertEquals(1, referencedSegmentAcquireCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(0, allReferencesAcquireCount);
    Assertions.assertEquals(1, referencedSegmentClosedCount);
    Assertions.assertEquals(0, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);
  }

  @Test
  public void testJoinableClausesClosedIfJoinableOneIsAlreadyClosed()
  {
    Assertions.assertFalse(baseSegmentRef.isClosed());
    j1Closed = true;

    Optional<Segment> maybeCloseable = makeJoinSegment(joinableClauses, null, null);
    Assertions.assertFalse(maybeCloseable.isPresent());
    Assertions.assertEquals(1, referencedSegmentAcquireCount);
    Assertions.assertEquals(1, indexedTableJoinableReferenceAcquireCount);
    Assertions.assertEquals(0, allReferencesAcquireCount);
    Assertions.assertEquals(1, referencedSegmentClosedCount);
    Assertions.assertEquals(1, indexedTableJoinableReferenceCloseCount);
    Assertions.assertEquals(0, allReferencesCloseCount);
  }


  @Test
  public void testGetMinTime()
  {
    final TimeBoundaryInspector timeBoundaryInspector = makeJoinSegment().as(TimeBoundaryInspector.class);
    Assertions.assertNotNull(timeBoundaryInspector, "non-null inspector");
    Assertions.assertEquals(
        DateTimes.of("2015-09-12T00:46:58.771Z"),
        timeBoundaryInspector.getMinTime(),
        "minTime"
    );
    Assertions.assertEquals(
        DateTimes.of("2015-09-12T05:21:00.059Z"),
        timeBoundaryInspector.getMaxTime(),
        "maxTime"
    );
    Assertions.assertFalse(timeBoundaryInspector.isMinMaxExact(), "exact");
  }

  @Test
  public void testGetMaxIngestedEventTime()
  {
    final MaxIngestedEventTimeInspector inspector = makeJoinSegment().as(MaxIngestedEventTimeInspector.class);
    Assertions.assertNull(inspector);
  }
}
