/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Result;
import io.druid.segment.column.ColumnConfig;
import org.junit.Assert;

import java.util.Iterator;

/**
 */
public class TestHelper
{
  private static final IndexMerger INDEX_MERGER;
  private static final IndexMaker INDEX_MAKER;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  static {
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER = new IndexMerger(JSON_MAPPER, INDEX_IO);
    INDEX_MAKER = new IndexMaker(JSON_MAPPER, INDEX_IO);
  }


  public static IndexMerger getTestIndexMerger()
  {
    return INDEX_MERGER;
  }

  public static IndexMaker getTestIndexMaker()
  {
    return INDEX_MAKER;
  }

  public static IndexIO getTestIndexIO()
  {
    return INDEX_IO;
  }

  public static ObjectMapper getObjectMapper() {
    return JSON_MAPPER;
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Sequence<Result<T>> results)
  {
    assertResults(expectedResults, Sequences.toList(results, Lists.<Result<T>>newArrayList()), "");
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    assertResults(expectedResults, results, "");
  }

  public static <T> void assertExpectedResults(
      Iterable<Result<T>> expectedResults,
      Iterable<Result<T>> results,
      String failMsg
  )
  {
    assertResults(expectedResults, results, failMsg);
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Iterable<T> results, String failMsg)
  {
    assertObjects(expectedResults, results, failMsg);
  }

  public static <T> void assertExpectedObjects(Iterable<T> expectedResults, Sequence<T> results, String failMsg)
  {
    assertObjects(expectedResults, Sequences.toList(results, Lists.<T>newArrayList()), failMsg);
  }

  private static <T> void assertResults(
      Iterable<Result<T>> expectedResults,
      Iterable<Result<T>> actualResults,
      String failMsg
  )
  {
    Iterator<? extends Result> resultsIter = actualResults.iterator();
    Iterator<? extends Result> resultsIter2 = actualResults.iterator();
    Iterator<? extends Result> expectedResultsIter = expectedResults.iterator();

    while (resultsIter.hasNext() && resultsIter2.hasNext() && expectedResultsIter.hasNext()) {
      Object expectedNext = expectedResultsIter.next();
      final Object next = resultsIter.next();
      final Object next2 = resultsIter2.next();

      if (expectedNext instanceof Row) {
        // HACK! Special casing for groupBy
        Assert.assertEquals(failMsg, expectedNext, next);
        Assert.assertEquals(failMsg, expectedNext, next2);
      } else {
        assertResult(failMsg, (Result) expectedNext, (Result) next);
        assertResult(
            String.format("%s: Second iterator bad, multiple calls to iterator() should be safe", failMsg),
            (Result) expectedNext,
            (Result) next2
        );
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (resultsIter2.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter2 to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%s: Expected expectedResultsIter to be exhausted, next element was %s",
              failMsg,
              expectedResultsIter.next()
          )
      );
    }
  }

  private static <T> void assertObjects(Iterable<T> expectedResults, Iterable<T> actualResults, String failMsg)
  {
    Iterator resultsIter = actualResults.iterator();
    Iterator resultsIter2 = actualResults.iterator();
    Iterator expectedResultsIter = expectedResults.iterator();

    while (resultsIter.hasNext() && resultsIter2.hasNext() && expectedResultsIter.hasNext()) {
      Object expectedNext = expectedResultsIter.next();
      final Object next = resultsIter.next();
      final Object next2 = resultsIter2.next();

      Assert.assertEquals(failMsg, expectedNext, next);
      Assert.assertEquals(
          String.format("%s: Second iterator bad, multiple calls to iterator() should be safe", failMsg),
          expectedNext,
          next2
      );
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (resultsIter2.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter2 to be exhausted, next element was %s", failMsg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%s: Expected expectedResultsIter to be exhausted, next element was %s",
              failMsg,
              expectedResultsIter.next()
          )
      );
    }
  }

  private static void assertResult(String msg, Result<?> expected, Result actual)
  {
    Assert.assertEquals(msg, expected, actual);
  }
}
