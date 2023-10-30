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
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PostJoinCursorTest extends BaseHashJoinSegmentStorageAdapterTest
{

  public QueryableIndexSegment infiniteFactSegment;

  /**
   * Simulates infinite segment by using a base cursor with advance() and advanceInterruptibly()
   * reduced to a no-op.
   */
  private static class TestInfiniteQueryableIndexSegment extends QueryableIndexSegment
  {

    private static class InfiniteQueryableIndexStorageAdapter extends QueryableIndexStorageAdapter
    {
      CountDownLatch countDownLatch;

      public InfiniteQueryableIndexStorageAdapter(QueryableIndex index, CountDownLatch countDownLatch)
      {
        super(index);
        this.countDownLatch = countDownLatch;
      }

      @Override
      public Sequence<Cursor> makeCursors(
          @Nullable Filter filter,
          Interval interval,
          VirtualColumns virtualColumns,
          Granularity gran,
          boolean descending,
          @Nullable QueryMetrics<?> queryMetrics
      )
      {
        return super.makeCursors(filter, interval, virtualColumns, gran, descending, queryMetrics)
                    .map(cursor -> new CursorNoAdvance(cursor, countDownLatch));
      }

      private static class CursorNoAdvance implements Cursor
      {
        Cursor cursor;
        CountDownLatch countDownLatch;

        public CursorNoAdvance(Cursor cursor, CountDownLatch countDownLatch)
        {
          this.cursor = cursor;
          this.countDownLatch = countDownLatch;
        }

        @Override
        public ColumnSelectorFactory getColumnSelectorFactory()
        {
          return cursor.getColumnSelectorFactory();
        }

        @Override
        public DateTime getTime()
        {
          return cursor.getTime();
        }

        @Override
        public void advance()
        {
          // Do nothing to simulate infinite rows
          countDownLatch.countDown();
        }

        @Override
        public void advanceUninterruptibly()
        {
          // Do nothing to simulate infinite rows
          countDownLatch.countDown();

        }

        @Override
        public boolean isDone()
        {
          return false;
        }

        @Override
        public boolean isDoneOrInterrupted()
        {
          return cursor.isDoneOrInterrupted();
        }

        @Override
        public void reset()
        {

        }
      }
    }

    private final StorageAdapter testStorageAdaptor;

    public TestInfiniteQueryableIndexSegment(QueryableIndex index, SegmentId segmentId, CountDownLatch countDownLatch)
    {
      super(index, segmentId);
      testStorageAdaptor = new InfiniteQueryableIndexStorageAdapter(index, countDownLatch);
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      return testStorageAdaptor;
    }
  }


  private static class ExceptionHandler implements Thread.UncaughtExceptionHandler
  {

    Throwable exception;

    @Override
    public void uncaughtException(Thread t, Throwable e)
    {
      exception = e;
    }

    public Throwable getException()
    {
      return exception;
    }
  }

  @Test
  public void testAdvanceWithInterruption() throws IOException, InterruptedException
  {

    final int rowsBeforeInterrupt = 1000;

    CountDownLatch countDownLatch = new CountDownLatch(rowsBeforeInterrupt);

    infiniteFactSegment = new TestInfiniteQueryableIndexSegment(
        JoinTestHelper.createFactIndexBuilder(temporaryFolder.newFolder()).buildMMappedIndex(),
        SegmentId.dummy("facts"),
        countDownLatch
    );

    countriesTable = JoinTestHelper.createCountriesIndexedTable();

    Thread joinCursorThread = new Thread(() -> makeCursorAndAdvance());
    ExceptionHandler exceptionHandler = new ExceptionHandler();
    joinCursorThread.setUncaughtExceptionHandler(exceptionHandler);
    joinCursorThread.start();

    countDownLatch.await(1, TimeUnit.SECONDS);
    joinCursorThread.interrupt();

    // Wait for a max of 1 sec for the exception to be set.
    for (int i = 0; i < 1000; i++) {
      if (exceptionHandler.getException() == null) {
        sleep(1);
      } else {
        assertTrue(exceptionHandler.getException() instanceof QueryInterruptedException);
        return;
      }
    }
    fail();
  }

  public void makeCursorAndAdvance()
  {

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToCountryOnIsoCode(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        null,
        joinableClauses,
        VirtualColumns.EMPTY
    );

    HashJoinSegmentStorageAdapter hashJoinSegmentStorageAdapter = new HashJoinSegmentStorageAdapter(
        infiniteFactSegment.asStorageAdapter(),
        joinableClauses,
        joinFilterPreAnalysis
    );

    Cursor cursor = Iterables.getOnlyElement(hashJoinSegmentStorageAdapter.makeCursors(
        null,
        Intervals.ETERNITY,
        VirtualColumns.EMPTY,
        Granularities.ALL,
        false,
        null
    ).toList());

    ((PostJoinCursor) cursor).setValueMatcher(new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        return false;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    });

    cursor.advance();
  }
}
