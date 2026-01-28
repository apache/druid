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

package org.apache.druid.indexing.common.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.InputRowFilterResult;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FilteringCloseableInputRowIteratorTest
{
  private static final List<String> DIMENSIONS = ImmutableList.of("dim1", "dim2");
  private static final List<InputRow> ROWS = ImmutableList.of(
      newRow(DateTimes.of("2020-01-01"), 10, 200),
      newRow(DateTimes.of("2020-01-01"), 10, 400),
      newRow(DateTimes.of("2020-01-01"), 20, 400),
      newRow(DateTimes.of("2020-01-01"), 10, 800),
      newRow(DateTimes.of("2020-01-01"), 30, 200),
      newRow(DateTimes.of("2020-01-01"), 10, 300)
  );

  private RowIngestionMeters rowIngestionMeters;
  private ParseExceptionHandler parseExceptionHandler;

  @Before
  public void setup()
  {
    rowIngestionMeters = new SimpleRowIngestionMeters();
    parseExceptionHandler = Mockito.spy(new ParseExceptionHandler(
        rowIngestionMeters,
        true,
        Integer.MAX_VALUE,
        1024 // do not use Integer.MAX_VALUE since it will create an object array of this length
    ));
  }

  @Test
  public void testFilterOutRows()
  {
    final Predicate<InputRow> filter = row -> (Integer) row.getRaw("dim1") == 10;
    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        CloseableIterators.withEmptyBaggage(ROWS.iterator()),
        InputRowFilter.fromPredicate(filter),
        rowIngestionMeters,
        parseExceptionHandler
    );
    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    Assert.assertEquals(
        ROWS.stream().filter(filter).collect(Collectors.toList()),
        filteredRows
    );
    Assert.assertEquals(2, rowIngestionMeters.getThrownAway());
  }

  @Test
  public void testParseExceptionInDelegateNext()
  {
    // This iterator throws ParseException every other call to next().
    final CloseableIterator<InputRow> parseExceptionThrowingIterator = new CloseableIterator<>()
    {
      final int numRowsToIterate = ROWS.size() * 2;
      int nextIdx = 0;

      @Override
      public boolean hasNext()
      {
        return nextIdx < numRowsToIterate;
      }

      @Override
      public InputRow next()
      {
        final int currentIdx = nextIdx++;
        if (currentIdx % 2 == 0) {
          return ROWS.get(currentIdx / 2);
        } else {
          throw new ParseException(null, "Parse exception at ", currentIdx);
        }
      }

      @Override
      public void close()
      {
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        parseExceptionThrowingIterator,
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    Assert.assertEquals(ROWS, filteredRows);
    Assert.assertEquals(ROWS.size(), rowIngestionMeters.getUnparseable());
  }

  @Test
  public void testParseExceptionInPredicateTest()
  {

    final CloseableIterator<InputRow> parseExceptionThrowingIterator = CloseableIterators.withEmptyBaggage(
        ROWS.iterator()
    );
    // This filter throws ParseException every other call to test().
    final Predicate<InputRow> filter = new Predicate<>()
    {
      boolean throwParseException = false;

      @Override
      public boolean test(InputRow inputRow)
      {
        if (throwParseException) {
          throwParseException = false;
          throw new ParseException(null, "test");
        } else {
          throwParseException = true;
          return true;
        }
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        parseExceptionThrowingIterator,
        InputRowFilter.fromPredicate(filter),
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    final List<InputRow> expectedRows = ImmutableList.of(
        ROWS.get(0),
        ROWS.get(2),
        ROWS.get(4)
    );
    Assert.assertEquals(expectedRows, filteredRows);
    Assert.assertEquals(ROWS.size() - expectedRows.size(), rowIngestionMeters.getUnparseable());
  }

  @Test
  public void testParseExceptionInDelegateHasNext()
  {
    // This iterator throws ParseException every other call to hasNext().
    final CloseableIterator<InputRow> parseExceptionThrowingIterator = new CloseableIterator<>()
    {
      final int numRowsToIterate = ROWS.size() * 2;
      int currentIndex = 0;
      int nextIndex = 0;

      @Override
      public boolean hasNext()
      {
        currentIndex = nextIndex++;
        if (currentIndex % 2 == 0) {
          return currentIndex < numRowsToIterate;
        } else {
          throw new ParseException(null, "Parse exception at ", currentIndex);
        }
      }

      @Override
      public InputRow next()
      {
        return ROWS.get(currentIndex / 2);
      }

      @Override
      public void close()
      {
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        parseExceptionThrowingIterator,
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    Assert.assertEquals(ROWS, filteredRows);
    Assert.assertEquals(ROWS.size(), rowIngestionMeters.getUnparseable());
  }

  @Test(expected = RuntimeException.class)
  public void testNonParseExceptionInDelegateHasNext()
  {
    // This iterator throws ParseException every other call to hasNext().
    final CloseableIterator<InputRow> parseExceptionThrowingIterator = new CloseableIterator<>()
    {
      final int numRowsToIterate = ROWS.size() * 2;
      int currentIndex = 0;
      int nextIndex = 0;

      @Override
      public boolean hasNext()
      {
        currentIndex = nextIndex++;
        if (currentIndex % 2 == 0) {
          return currentIndex < numRowsToIterate;
        } else {
          throw new RuntimeException("should explode");
        }
      }

      @Override
      public InputRow next()
      {
        return ROWS.get(currentIndex / 2);
      }

      @Override
      public void close()
      {
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        parseExceptionThrowingIterator,
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );

    while (rowIterator.hasNext()) {
      rowIterator.next();
    }
    Assert.fail("you never should have come here");
  }

  @Test
  public void testCloseDelegateIsClosed() throws IOException
  {
    final MutableBoolean closed = new MutableBoolean(false);
    final CloseableIterator<InputRow> delegate = CloseableIterators.wrap(
        Collections.emptyIterator(),
        closed::setTrue
    );
    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        delegate,
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );
    rowIterator.close();
    Assert.assertTrue(closed.isTrue());
  }

  @Test
  public void testParseExceptionSaveExceptionCause()
  {

    // This iterator throws ParseException every other call to hasNext().
    final CloseableIterator<InputRow> parseExceptionThrowingIterator = new CloseableIterator<>()
    {
      final int numRowsToIterate = ROWS.size() * 2;
      int currentIndex = 0;
      int nextIndex = 0;

      @Override
      public boolean hasNext()
      {
        currentIndex = nextIndex++;
        if (currentIndex % 2 == 0) {
          return currentIndex < numRowsToIterate;
        } else {
          try {
            throw new IllegalArgumentException("this is the root cause of the exception!");
          }
          catch (Exception e) {
            throw new ParseException(null, e, "Parse exception at [%d]", currentIndex);
          }
        }
      }

      @Override
      public InputRow next()
      {
        return ROWS.get(currentIndex / 2);
      }

      @Override
      public void close()
      {
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        parseExceptionThrowingIterator,
        InputRowFilter.allowAll(),
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
    Mockito.verify(parseExceptionHandler, Mockito.times(6)).logParseExceptionHelper(exceptionArgumentCaptor.capture());
    Exception parseException = exceptionArgumentCaptor.getValue();
    Assert.assertTrue(parseException.getMessage().contains("Parse exception at"));
    Assert.assertNotNull(parseException.getCause());
    Assert.assertTrue(parseException.getCause().getMessage().contains("this is the root cause of the exception!"));
    Assert.assertEquals(IllegalArgumentException.class, parseException.getCause().getClass());

    Assert.assertEquals(ROWS, filteredRows);
    Assert.assertEquals(ROWS.size(), rowIngestionMeters.getUnparseable());
  }


  @Test
  public void testRowFilterWithReasons()
  {
    // RowFilter that returns different reasons based on dim1 value
    final InputRowFilter rowFilter = row -> {
      int dim1 = (Integer) row.getRaw("dim1");
      if (dim1 == 10) {
        return InputRowFilterResult.ACCEPTED;
      } else if (dim1 == 20) {
        return InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME;
      } else {
        return InputRowFilterResult.CUSTOM_FILTER;
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        CloseableIterators.withEmptyBaggage(ROWS.iterator()),
        rowFilter,
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);

    // Only rows with dim1=10 should pass
    Assert.assertEquals(4, filteredRows.size());
    for (InputRow row : filteredRows) {
      Assert.assertEquals(10, row.getRaw("dim1"));
    }

    // Check total thrown away
    Assert.assertEquals(2, rowIngestionMeters.getThrownAway());

    // Check per-reason counts
    Map<String, Long> byReason = rowIngestionMeters.getThrownAwayByReason();
    Assert.assertEquals(2, byReason.size());
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowFilterResult.BEFORE_MIN_MESSAGE_TIME.getReason())); // dim1=20
    Assert.assertEquals(Long.valueOf(1), byReason.get(InputRowFilterResult.CUSTOM_FILTER.getReason())); // dim1=30
  }

  @Test
  public void testRowFilterFromPredicate()
  {
    // Use the static helper to convert a Predicate to RowFilter
    final Predicate<InputRow> predicate = row -> (Integer) row.getRaw("dim1") == 10;
    final InputRowFilter rowFilter = InputRowFilter.fromPredicate(predicate);

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        CloseableIterators.withEmptyBaggage(ROWS.iterator()),
        rowFilter,
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);

    Assert.assertEquals(4, filteredRows.size());
    Assert.assertEquals(2, rowIngestionMeters.getThrownAway());

    // All thrown away should have FILTERED reason when using fromPredicate
    Map<String, Long> byReason = rowIngestionMeters.getThrownAwayByReason();
    Assert.assertEquals(1, byReason.size());
    Assert.assertEquals(Long.valueOf(2), byReason.get(InputRowFilterResult.CUSTOM_FILTER.getReason()));
  }

  @Test
  public void testRowFilterAnd()
  {
    // First filter: reject nulls (simulated by checking dim1)
    final InputRowFilter nullFilter = row -> row == null ? InputRowFilterResult.NULL_OR_EMPTY_RECORD : InputRowFilterResult.ACCEPTED;

    // Second filter: reject if dim1 != 10
    final InputRowFilter valueFilter = row -> (Integer) row.getRaw("dim1") == 10 ? InputRowFilterResult.ACCEPTED : InputRowFilterResult.CUSTOM_FILTER;

    // Combine filters
    final InputRowFilter combinedFilter = nullFilter.and(valueFilter);

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        CloseableIterators.withEmptyBaggage(ROWS.iterator()),
        combinedFilter,
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);

    Assert.assertEquals(4, filteredRows.size());
    Assert.assertEquals(2, rowIngestionMeters.getThrownAway());

    // All rejected rows should have FILTERED reason (from second filter)
    Map<String, Long> byReason = rowIngestionMeters.getThrownAwayByReason();
    Assert.assertEquals(Long.valueOf(2), byReason.get(InputRowFilterResult.CUSTOM_FILTER.getReason()));
  }

  private static InputRow newRow(DateTime timestamp, Object dim1Val, Object dim2Val)
  {
    return new MapBasedInputRow(timestamp, DIMENSIONS, ImmutableMap.of("dim1", dim1Val, "dim2", dim2Val));
  }
}
