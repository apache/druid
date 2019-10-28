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

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Result;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.junit.Assert;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 */
public class TestHelper
{
  public static final ObjectMapper JSON_MAPPER = makeJsonMapper();

  public static IndexMergerV9 getTestIndexMergerV9(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    return new IndexMergerV9(JSON_MAPPER, getTestIndexIO(), segmentWriteOutMediumFactory);
  }

  public static IndexIO getTestIndexIO()
  {
    return new IndexIO(
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
  }

  public static ObjectMapper makeJsonMapper()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
    );
    return mapper;
  }

  public static ObjectMapper makeSmileMapper()
  {
    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), mapper)
    );
    return mapper;
  }

  public static <T> Iterable<T> revert(Iterable<T> input)
  {
    return Lists.reverse(Lists.newArrayList(input));
  }

  public static <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Sequence<Result<T>> results)
  {
    assertResults(expectedResults, results.toList(), "");
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
    assertObjects(expectedResults, results.toList(), failMsg);
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

      if (expectedNext instanceof ResultRow) {
        // HACK! Special casing for groupBy
        assertRow(failMsg, (ResultRow) expectedNext, (ResultRow) next);
        assertRow(failMsg, (ResultRow) expectedNext, (ResultRow) next2);
      } else if (expectedNext instanceof Result
                 && (((Result) expectedNext).getValue()) instanceof TimeseriesResultValue) {
        // Special case for GroupByTimeseriesQueryRunnerTest to allow a floating point delta to be used
        // in result comparison
        assertTimeseriesResultValue(failMsg, (Result) expectedNext, (Result) next);
        assertTimeseriesResultValue(
            StringUtils.format("%s: Second iterator bad, multiple calls to iterator() should be safe", failMsg),
            (Result) expectedNext,
            (Result) next2
        );

      } else if (expectedNext instanceof Result
                 && (((Result) expectedNext).getValue()) instanceof TopNResultValue) {
        // Special to allow a floating point delta to be used in result comparison due to legacy expected results
        assertTopNResultValue(failMsg, (Result) expectedNext, (Result) next);
        assertTopNResultValue(
            StringUtils.format("%s: Second iterator bad, multiple calls to iterator() should be safe", failMsg),
            (Result) expectedNext,
            (Result) next2
        );
      } else {
        assertResult(failMsg, (Result) expectedNext, (Result) next);
        assertResult(
            StringUtils.format("%s: Second iterator bad, multiple calls to iterator() should be safe", failMsg),
            (Result) expectedNext,
            (Result) next2
        );
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          StringUtils.format(
              "%s: Expected resultsIter to be exhausted, next element was %s",
              failMsg,
              resultsIter.next()
          )
      );
    }

    if (resultsIter2.hasNext()) {
      Assert.fail(
          StringUtils.format(
              "%s: Expected resultsIter2 to be exhausted, next element was %s",
              failMsg,
              resultsIter.next()
          )
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          StringUtils.format(
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
      String failMsg2 = StringUtils.format(
          "%s: Second iterator bad, multiple calls to iterator() should be safe",
          failMsg
      );

      if (expectedNext instanceof ResultRow) {
        // HACK! Special casing for groupBy
        assertRow(failMsg, (ResultRow) expectedNext, (ResultRow) next);
        assertRow(failMsg2, (ResultRow) expectedNext, (ResultRow) next2);
      } else {
        Assert.assertEquals(failMsg, expectedNext, next);
        Assert.assertEquals(failMsg2, expectedNext, next2);
      }
    }

    if (resultsIter.hasNext()) {
      Assert.fail(
          StringUtils.format("%s: Expected resultsIter to be exhausted, next element was %s", msg, resultsIter.next())
      );
    }

    if (resultsIter2.hasNext()) {
      Assert.fail(
          StringUtils.format("%s: Expected resultsIter2 to be exhausted, next element was %s", msg, resultsIter.next())
      );
    }

    if (expectedResultsIter.hasNext()) {
      Assert.fail(
          StringUtils.format(
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

  private static void assertTimeseriesResultValue(String msg, Result expected, Result actual)
  {
    // Custom equals check to get fuzzy comparison of numerics, useful because different groupBy strategies don't
    // always generate exactly the same results (different merge ordering / float vs double)
    Assert.assertEquals(StringUtils.format("%s: timestamp", msg), expected.getTimestamp(), actual.getTimestamp());

    TimeseriesResultValue expectedVal = (TimeseriesResultValue) expected.getValue();
    TimeseriesResultValue actualVal = (TimeseriesResultValue) actual.getValue();

    final Map<String, Object> expectedMap = expectedVal.getBaseObject();
    final Map<String, Object> actualMap = actualVal.getBaseObject();

    assertRow(
        msg,
        new MapBasedRow(expected.getTimestamp(), expectedMap),
        new MapBasedRow(actual.getTimestamp(), actualMap)
    );
  }

  private static void assertTopNResultValue(String msg, Result expected, Result actual)
  {
    TopNResultValue expectedVal = (TopNResultValue) expected.getValue();
    TopNResultValue actualVal = (TopNResultValue) actual.getValue();

    List<Row> listExpectedRows = expectedVal.getValue()
                                            .stream()
                                            .map(dimensionAndMetricValueExtractor -> new MapBasedRow(
                                                expected.getTimestamp(),
                                                dimensionAndMetricValueExtractor.getBaseObject()
                                            ))
                                            .collect(Collectors.toList());

    List<Row> listActualRows = actualVal.getValue()
                                        .stream()
                                        .map(dimensionAndMetricValueExtractor -> new MapBasedRow(
                                            actual.getTimestamp(),
                                            dimensionAndMetricValueExtractor.getBaseObject()
                                        ))
                                        .collect(Collectors.toList());
    Assert.assertEquals("Size of list must match", listExpectedRows.size(), listActualRows.size());

    IntStream.range(0, listExpectedRows.size()).forEach(value -> assertRow(
        StringUtils.format("%s, on value number [%s]", msg, value),
        listExpectedRows.get(value),
        listActualRows.get(value)
    ));
  }

  private static void assertRow(String msg, Row expected, Row actual)
  {
    // Custom equals check to get fuzzy comparison of numerics, useful because different groupBy strategies don't
    // always generate exactly the same results (different merge ordering / float vs double)
    Assert.assertEquals(
        StringUtils.format("%s: timestamp", msg),
        expected.getTimestamp(),
        actual.getTimestamp()
    );

    final Map<String, Object> expectedMap = ((MapBasedRow) expected).getEvent();
    final Map<String, Object> actualMap = ((MapBasedRow) actual).getEvent();

    Assert.assertEquals(StringUtils.format("%s: map keys", msg), expectedMap.keySet(), actualMap.keySet());
    for (final String key : expectedMap.keySet()) {
      final Object expectedValue = expectedMap.get(key);
      final Object actualValue = actualMap.get(key);

      if (expectedValue instanceof Float || expectedValue instanceof Double) {
        Assert.assertEquals(
            StringUtils.format("%s: key[%s]", msg, key),
            ((Number) expectedValue).doubleValue(),
            ((Number) actualValue).doubleValue(),
            Math.abs(((Number) expectedValue).doubleValue() * 1e-6)
        );
      } else {
        Assert.assertEquals(
            StringUtils.format("%s: key[%s]", msg, key),
            expectedValue,
            actualValue
        );
      }
    }
  }

  private static void assertRow(String msg, ResultRow expected, ResultRow actual)
  {
    Assert.assertEquals(
        StringUtils.format("%s: row length", msg),
        expected.length(),
        actual.length()
    );

    for (int i = 0; i < expected.length(); i++) {
      final String message = StringUtils.format("%s: idx[%d]", msg, i);
      final Object expectedValue = expected.get(i);
      final Object actualValue = actual.get(i);

      if (expectedValue instanceof Float || expectedValue instanceof Double) {
        Assert.assertEquals(
            message,
            ((Number) expectedValue).doubleValue(),
            ((Number) actualValue).doubleValue(),
            Math.abs(((Number) expectedValue).doubleValue() * 1e-6)
        );
      } else {
        Assert.assertEquals(
            message,
            expectedValue,
            actualValue
        );
      }
    }
  }

  public static Map<String, Object> createExpectedMap(Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = new HashMap<>();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }
    return theVals;
  }
}
