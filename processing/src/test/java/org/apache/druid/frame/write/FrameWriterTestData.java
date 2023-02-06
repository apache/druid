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

package org.apache.druid.frame.write;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Data used by {@link FrameWriterTest}.
 */
public class FrameWriterTestData
{
  public enum Sortedness
  {
    UNSORTED,
    ASCENDING,
    DESCENDING
  }

  public static final Dataset<String> TEST_STRINGS_SINGLE_VALUE = new Dataset<>(
      ColumnType.STRING,
      Stream.of(
          null,
          NullHandling.emptyToNullIfNeeded(""), // Empty string in SQL-compatible mode, null otherwise
          "\uD83D\uDE42",
          "\uD83E\uDEE5",
          "\uD83E\uDD20",
          "thee", // To ensure "the" is before "thee"
          "the",
          "quick",
          "brown",
          "fox",
          "jumps",
          "over",
          "the", // Repeated string
          "lazy",
          "dog"
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  /**
   * Multi-value strings in the form that {@link org.apache.druid.segment.DimensionSelector#getObject} would return
   * them: unwrapped string if single-element; list otherwise.
   */
  public static final Dataset<Object> TEST_STRINGS_MULTI_VALUE = new Dataset<>(
      ColumnType.STRING,
      Arrays.asList(
          Collections.emptyList(),
          null,
          NullHandling.emptyToNullIfNeeded(""),
          "foo",
          Arrays.asList("lazy", "dog"),
          "qux",
          Arrays.asList("the", "quick", "brown"),
          Arrays.asList("the", "quick", "brown", null),
          Arrays.asList("the", "quick", "brown", NullHandling.emptyToNullIfNeeded("")),
          Arrays.asList("the", "quick", "brown", "fox"),
          Arrays.asList("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"),
          Arrays.asList("the", "quick", "brown", "null"),
          Arrays.asList("thee", "quick", "brown"),
          "\uD83D\uDE42",
          Arrays.asList("\uD83D\uDE42", "\uD83E\uDEE5"),
          "\uD83E\uDD20"
      )
  );

  /**
   * String arrays in the form that a {@link org.apache.druid.segment.ColumnValueSelector} would return them.
   */
  public static final Dataset<Object> TEST_ARRAYS_STRING = new Dataset<>(
      ColumnType.STRING_ARRAY,
      Arrays.asList(
          Collections.emptyList(),
          Collections.singletonList(null),
          Collections.singletonList(NullHandling.emptyToNullIfNeeded("")),
          Collections.singletonList("dog"),
          Collections.singletonList("lazy"),
          Arrays.asList("the", "quick", "brown"),
          Arrays.asList("the", "quick", "brown", null),
          Arrays.asList("the", "quick", "brown", NullHandling.emptyToNullIfNeeded("")),
          Arrays.asList("the", "quick", "brown", "fox"),
          Arrays.asList("the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"),
          Arrays.asList("the", "quick", "brown", "null"),
          Collections.singletonList("\uD83D\uDE42"),
          Arrays.asList("\uD83D\uDE42", "\uD83E\uDEE5")
      )
  );

  public static final Dataset<Long> TEST_LONGS = new Dataset<>(
      ColumnType.LONG,
      Stream.of(
          NullHandling.defaultLongValue(),
          0L,
          -1L,
          1L,
          Long.MAX_VALUE,
          Long.MIN_VALUE,
          101L,
          -101L,
          3L,
          -2L,
          2L,
          1L,
          -1L,
          -3L
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  public static final Dataset<Float> TEST_FLOATS = new Dataset<>(
      ColumnType.FLOAT,
      Stream.of(
          0f,
          -0f,
          NullHandling.defaultFloatValue(),
          -1f,
          1f,
          //CHECKSTYLE.OFF: Regexp
          Float.MIN_VALUE,
          Float.MAX_VALUE,
          //CHECKSTYLE.ON: Regexp
          Float.NaN,
          -101f,
          101f,
          Float.POSITIVE_INFINITY,
          Float.NEGATIVE_INFINITY,
          0.004f,
          2.7e20f
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  public static final Dataset<Double> TEST_DOUBLES = new Dataset<>(
      ColumnType.DOUBLE,
      Stream.of(
          0d,
          -0d,
          NullHandling.defaultDoubleValue(),
          -1e-122d,
          1e122d,
          //CHECKSTYLE.OFF: Regexp
          Double.MIN_VALUE,
          Double.MAX_VALUE,
          //CHECKSTYLE.ON: Regexp
          Double.NaN,
          -101d,
          101d,
          Double.POSITIVE_INFINITY,
          Double.NEGATIVE_INFINITY,
          -0.000001d,
          2.7e100d
      ).sorted(Comparators.naturalNullsFirst()).collect(Collectors.toList())
  );

  public static final Dataset<HyperLogLogCollector> TEST_COMPLEX = new Dataset<>(
      HyperUniquesAggregatorFactory.TYPE,
      Arrays.asList(
          null,
          makeHllCollector(null),
          makeHllCollector("foo")
      )
  );

  /**
   * Wrapper around all the various TEST_* lists.
   */
  public static final List<Dataset<?>> DATASETS =
      ImmutableList.<Dataset<?>>builder()
                   .add(TEST_FLOATS)
                   .add(TEST_DOUBLES)
                   .add(TEST_LONGS)
                   .add(TEST_STRINGS_SINGLE_VALUE)
                   .add(TEST_STRINGS_MULTI_VALUE)
                   .add(TEST_ARRAYS_STRING)
                   .add(TEST_COMPLEX)
                   .build();

  private static HyperLogLogCollector makeHllCollector(@Nullable final String value)
  {
    final HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    if (value != null) {
      collector.add(Hashing.murmur3_128().hashBytes(StringUtils.toUtf8(value)).asBytes());
    }

    return collector;
  }

  public static class Dataset<T>
  {
    private final ColumnType type;
    private final List<T> sortedData;

    public Dataset(final ColumnType type, final List<T> sortedData)
    {
      this.type = type;
      this.sortedData = sortedData;
    }

    public ColumnType getType()
    {
      return type;
    }

    public List<T> getData(final Sortedness sortedness)
    {
      switch (sortedness) {
        case ASCENDING:
          return Collections.unmodifiableList(sortedData);
        case DESCENDING:
          return Collections.unmodifiableList(Lists.reverse(sortedData));
        case UNSORTED:
          // Shuffle ("unsort") the list, using the same seed every time for consistency.
          final List<T> newList = new ArrayList<>(sortedData);
          Collections.shuffle(newList, new Random(0));
          return newList;
        default:
          throw new ISE("No such sortedness [%s]", sortedness);
      }
    }
  }
}
