/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid;

import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.result.Result;
import org.junit.Assert;

import java.util.Iterator;

/**
 */
public class TestHelper
{
  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Sequence<Result<T>> results)
  {
    assertResults(expectedResults, Sequences.toList(results, Lists.<Result<T>>newArrayList()), "");
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    assertResults(expectedResults, results, "");
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results, String failMsg)
  {
    assertResults(expectedResults, results, failMsg);
  }

  private static <T> void assertResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> actualResults, String failMsg)
  {
    Iterator<? extends Result> resultsIter = actualResults.iterator();
    Iterator<? extends Result> resultsIter2 = actualResults.iterator();
    Iterator<? extends Result> expectedResultsIter = expectedResults.iterator();

    while (resultsIter.hasNext() && resultsIter2.hasNext() && expectedResultsIter.hasNext()) {
      Result expectedNext = expectedResultsIter.next();
      final Result next = resultsIter.next();
      final Result next2 = resultsIter2.next();

      assertResult(failMsg, expectedNext, next);
      assertResult(
          String.format("%sSecond iterator bad, multiple calls to iterator() should be safe", failMsg),
          expectedNext,
          next2
      );
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%sExpected resultsIter to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (resultsIter2.hasNext()) {
      Assert.fail(
          String.format("%sExpected resultsIter2 to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%sExpected expectedResultsIter to be exhausted, next element was %s", failMsg, expectedResultsIter.next()
          )
      );
    }
  }

  private static void assertResult(String msg, Result<?> expected, Result actual)
  {
    Assert.assertEquals(msg, expected, actual);
  }
}
