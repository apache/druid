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

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class FilteringCloseableInputRowIteratorTest
{
  private static final List<String> DIMENSIONS = ImmutableList.of("dim1", "dim2");

  private RowIngestionMeters rowIngestionMeters;
  private ParseExceptionHandler parseExceptionHandler;

  @Before
  public void setup()
  {
    rowIngestionMeters = new SimpleRowIngestionMeters();
    parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        1024 // do not use Integer.MAX_VALUE since it will create an object array of this length
    );
  }

  @Test
  public void testFilterOutRows()
  {
    final List<InputRow> rows = ImmutableList.of(
        newRow(DateTimes.of("2020-01-01"), 10, 200),
        newRow(DateTimes.of("2020-01-01"), 10, 400),
        newRow(DateTimes.of("2020-01-01"), 20, 400),
        newRow(DateTimes.of("2020-01-01"), 10, 800),
        newRow(DateTimes.of("2020-01-01"), 30, 200),
        newRow(DateTimes.of("2020-01-01"), 10, 300)
    );
    final Predicate<InputRow> filter = row -> (Integer) row.getRaw("dim1") == 10;
    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        CloseableIterators.withEmptyBaggage(rows.iterator()),
        filter,
        rowIngestionMeters,
        parseExceptionHandler
    );
    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    Assert.assertEquals(
        rows.stream().filter(filter).collect(Collectors.toList()),
        filteredRows
    );
    Assert.assertEquals(2, rowIngestionMeters.getThrownAway());
  }

  @Test
  public void testParseExceptionInDelegateNext()
  {
    final List<InputRow> rows = ImmutableList.of(
        newRow(DateTimes.of("2020-01-01"), 10, 200),
        newRow(DateTimes.of("2020-01-01"), 10, 400),
        newRow(DateTimes.of("2020-01-01"), 20, 400),
        newRow(DateTimes.of("2020-01-01"), 10, 800),
        newRow(DateTimes.of("2020-01-01"), 30, 200),
        newRow(DateTimes.of("2020-01-01"), 10, 300)
    );

    // This iterator throws ParseException every other call to next().
    final CloseableIterator<InputRow> parseExceptionThrowingIterator = new CloseableIterator<InputRow>()
    {
      final int numRowsToIterate = rows.size() * 2;
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
          return rows.get(currentIdx / 2);
        } else {
          throw new ParseException("Parse exception at ", currentIdx);
        }
      }

      @Override
      public void close()
      {
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        parseExceptionThrowingIterator,
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    Assert.assertEquals(rows, filteredRows);
    Assert.assertEquals(rows.size(), rowIngestionMeters.getUnparseable());
  }

  @Test
  public void testParseExceptionInPredicateTest()
  {
    final List<InputRow> rows = ImmutableList.of(
        newRow(DateTimes.of("2020-01-01"), 10, 200),
        newRow(DateTimes.of("2020-01-01"), 10, 400),
        newRow(DateTimes.of("2020-01-01"), 20, 400),
        newRow(DateTimes.of("2020-01-01"), 10, 800),
        newRow(DateTimes.of("2020-01-01"), 30, 200),
        newRow(DateTimes.of("2020-01-01"), 10, 300)
    );

    final CloseableIterator<InputRow> parseExceptionThrowingIterator = CloseableIterators.withEmptyBaggage(
        rows.iterator()
    );
    // This filter throws ParseException every other call to test().
    final Predicate<InputRow> filter = new Predicate<InputRow>()
    {
      boolean throwParseException = false;

      @Override
      public boolean test(InputRow inputRow)
      {
        if (throwParseException) {
          throwParseException = false;
          throw new ParseException("test");
        } else {
          throwParseException = true;
          return true;
        }
      }
    };

    final FilteringCloseableInputRowIterator rowIterator = new FilteringCloseableInputRowIterator(
        parseExceptionThrowingIterator,
        filter,
        rowIngestionMeters,
        parseExceptionHandler
    );

    final List<InputRow> filteredRows = new ArrayList<>();
    rowIterator.forEachRemaining(filteredRows::add);
    final List<InputRow> expectedRows = ImmutableList.of(
        rows.get(0),
        rows.get(2),
        rows.get(4)
    );
    Assert.assertEquals(expectedRows, filteredRows);
    Assert.assertEquals(rows.size() - expectedRows.size(), rowIngestionMeters.getUnparseable());
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
        row -> true,
        rowIngestionMeters,
        parseExceptionHandler
    );
    rowIterator.close();
    Assert.assertTrue(closed.isTrue());
  }

  private static InputRow newRow(DateTime timestamp, Object dim1Val, Object dim2Val)
  {
    return new MapBasedInputRow(timestamp, DIMENSIONS, ImmutableMap.of("dim1", dim1Val, "dim2", dim2Val));
  }
}
