/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Result;
import io.druid.segment.column.ColumnConfig;
import org.junit.Assert;

import java.util.Iterator;
import java.util.Map;

/**
 */
public class TestHelper
{
  private static final IndexMerger INDEX_MERGER;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
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
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  public static ObjectMapper getTestObjectMapper()
  {
    return JSON_MAPPER;
  }


  public static IndexMerger getTestIndexMerger()
  {
    return INDEX_MERGER;
  }

  public static IndexMergerV9 getTestIndexMergerV9()
  {
    return INDEX_MERGER_V9;
  }

  public static IndexIO getTestIndexIO()
  {
    return INDEX_IO;
  }

  public static ObjectMapper getObjectMapper() {
    return JSON_MAPPER;
  }

  public static <T> Iterable<T> revert(Iterable<T> input) {
    return Lists.reverse(Lists.newArrayList(input));
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
        assertRow(failMsg, (Row) expectedNext, (Row) next);
        assertRow(failMsg, (Row) expectedNext, (Row) next2);
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

  private static <T> void assertObjects(Iterable<T> expectedResults, Iterable<T> actualResults, String msg)
  {
    Iterator resultsIter = actualResults.iterator();
    Iterator resultsIter2 = actualResults.iterator();
    Iterator expectedResultsIter = expectedResults.iterator();

    int index = 0;
    while (resultsIter.hasNext() && resultsIter2.hasNext() && expectedResultsIter.hasNext()) {
      Object expectedNext = expectedResultsIter.next();
      final Object next = resultsIter.next();
      final Object next2 = resultsIter2.next();

      String failMsg = msg + "-" + index++;
      String failMsg2 = String.format("%s: Second iterator bad, multiple calls to iterator() should be safe", failMsg);

      if (expectedNext instanceof Row) {
        // HACK! Special casing for groupBy
        assertRow(failMsg, (Row) expectedNext, (Row) next);
        assertRow(failMsg2, (Row) expectedNext, (Row) next2);
      } else {
        Assert.assertEquals(failMsg, expectedNext, next);
        Assert.assertEquals(failMsg2, expectedNext, next2);
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter to be exhausted, next element was %s", msg, resultsIter.next())
      );
    }

    if (resultsIter2.hasNext()) {
      Assert.fail(
          String.format("%s: Expected resultsIter2 to be exhausted, next element was %s", msg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          String.format(
              "%s: Expected expectedResultsIter to be exhausted, next element was %s",
              msg,
              expectedResultsIter.next()
          )
      );
    }
  }

  private static void assertResult(String msg, Result<?> expected, Result actual)
  {
    Assert.assertEquals(msg, expected, actual);
  }

  private static void assertRow(String msg, Row expected, Row actual)
  {
    // Custom equals check to get fuzzy comparison of numerics, useful because different groupBy strategies don't
    // always generate exactly the same results (different merge ordering / float vs double)
    Assert.assertEquals(String.format("%s: timestamp", msg), expected.getTimestamp(), actual.getTimestamp());

    final Map<String, Object> expectedMap = ((MapBasedRow) expected).getEvent();
    final Map<String, Object> actualMap = ((MapBasedRow) actual).getEvent();

    Assert.assertEquals(String.format("%s: map keys", msg), expectedMap.keySet(), actualMap.keySet());
    for (final String key : expectedMap.keySet()) {
      final Object expectedValue = expectedMap.get(key);
      final Object actualValue = actualMap.get(key);

      if (expectedValue instanceof Float || expectedValue instanceof Double) {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            ((Number) expectedValue).doubleValue(),
            ((Number) actualValue).doubleValue(),
            ((Number) expectedValue).doubleValue() * 1e-6
        );
      } else {
        Assert.assertEquals(
            String.format("%s: key[%s]", msg, key),
            expectedValue,
            actualValue
        );
      }
    }
  }
}
