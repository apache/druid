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
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.ReferenceCountingSegment;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.filter.JoinFilterPreAnalysis;
import org.apache.druid.timeline.SegmentId;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PostJoinCursorTest extends BaseHashJoinSegmentCursorFactoryTest
{

  public QueryableIndexSegment infiniteFactSegment;

  /**
   * Simulates infinite segment by using a base cursor with advance() and advanceInterruptibly()
   * reduced to a no-op.
   */
  private static class TestInfiniteQueryableIndexSegment extends QueryableIndexSegment
  {
    private static class InfiniteCursorFactory implements CursorFactory
    {
      final CursorFactory delegate;
      CountDownLatch countDownLatch;

      public InfiniteCursorFactory(CursorFactory delegate, CountDownLatch countDownLatch)
      {
        this.delegate = delegate;
        this.countDownLatch = countDownLatch;
      }

      @Override
      public CursorHolder makeCursorHolder(CursorBuildSpec spec)
      {
        final CursorHolder holder = delegate.makeCursorHolder(spec);
        return new CursorHolder()
        {
          @Nullable
          @Override
          public Cursor asCursor()
          {
            return new CursorNoAdvance(holder.asCursor(), countDownLatch);
          }

          @Nullable
          @Override
          public List<OrderBy> getOrdering()
          {
            return holder.getOrdering();
          }

          @Override
          public void close()
          {
            holder.close();
          }
        };
      }

      @Override
      public RowSignature getRowSignature()
      {
        return delegate.getRowSignature();
      }

      @Override
      @Nullable
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return delegate.getColumnCapabilities(column);
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
          cursor.reset();
        }
      }
    }

    private final CursorFactory cursorFactory;

    public TestInfiniteQueryableIndexSegment(QueryableIndex index, SegmentId segmentId, CountDownLatch countDownLatch)
    {
      super(index, segmentId);
      cursorFactory = new InfiniteCursorFactory(new QueryableIndexCursorFactory(index), countDownLatch);
    }

    @Override
    public CursorFactory asCursorFactory()
    {
      return cursorFactory;
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
    testAdvance(true);
  }

  @Test
  public void testAdvanceWithoutInterruption() throws IOException, InterruptedException
  {
    testAdvance(false);
  }

  private void testAdvance(boolean withInterruption) throws IOException, InterruptedException
  {

    final int rowsBeforeInterrupt = 1000;

    CountDownLatch countDownLatch = new CountDownLatch(rowsBeforeInterrupt);

    infiniteFactSegment = new TestInfiniteQueryableIndexSegment(
        JoinTestHelper.createFactIndexBuilder(temporaryFolder.newFolder()).buildMMappedIndex(),
        SegmentId.dummy("facts"),
        countDownLatch
    );

    countriesTable = JoinTestHelper.createCountriesIndexedTable();

    Thread joinCursorThread = new Thread(() -> makeCursorAndAdvance(withInterruption));
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

  public void makeCursorAndAdvance(boolean withInterruption)
  {

    List<JoinableClause> joinableClauses = ImmutableList.of(
        factToCountryOnIsoCode(JoinType.LEFT)
    );

    JoinFilterPreAnalysis joinFilterPreAnalysis = makeDefaultConfigPreAnalysis(
        null,
        joinableClauses,
        VirtualColumns.EMPTY
    );

    HashJoinSegment hashJoinSegment = new HashJoinSegment(
        ReferenceCountingSegment.wrapRootGenerationSegment(infiniteFactSegment),
        null,
        joinableClauses,
        joinFilterPreAnalysis
    );

    try (final CursorHolder cursorHolder = hashJoinSegment.asCursorFactory().makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      Cursor cursor = cursorHolder.asCursor();

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

      if (withInterruption) {
        cursor.advance();
      } else {
        cursor.advanceUninterruptibly();
      }
    }
  }
}
