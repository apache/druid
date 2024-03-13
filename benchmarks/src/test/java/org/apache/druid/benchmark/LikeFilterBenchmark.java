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

package org.apache.druid.benchmark;

import com.google.common.collect.FluentIterable;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.LikeDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.serde.StringUtf8ColumnIndexSupplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class LikeFilterBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final int START_INT = 1_000_000;
  private static final int END_INT = 9_999_999;

  private static final Filter SELECTOR_EQUALS = new SelectorDimFilter(
      "foo",
      "1000000",
      null
  ).toFilter();

  private static final Filter LIKE_EQUALS = new LikeDimFilter(
      "foo",
      "1000000",
      null,
      null
  ).toFilter();

  private static final Filter BOUND_PREFIX = new BoundDimFilter(
      "foo",
      "50",
      "50\uffff",
      false,
      false,
      null,
      null,
      StringComparators.LEXICOGRAPHIC
  ).toFilter();

  private static final Filter REGEX_PREFIX = new RegexDimFilter(
      "foo",
      "^50.*",
      null
  ).toFilter();

  private static final Filter LIKE_PREFIX = new LikeDimFilter(
      "foo",
      "50%",
      null,
      null
  ).toFilter();

  private static final Filter REGEX_SUFFIX = new RegexDimFilter(
      "foo",
      ".*50$",
      null
  ).toFilter();

  private static final Filter LIKE_SUFFIX = new LikeDimFilter(
      "foo",
      "%50",
      null,
      null
  ).toFilter();

  private static final Filter LIKE_CONTAINS = new LikeDimFilter(
      "foo",
      "%50%",
      null,
      null
  ).toFilter();

  private static final Filter REGEX_CONTAINS = new RegexDimFilter(
      "foo",
      ".*50.*",
      null
  ).toFilter();

  private static final Filter LIKE_COMPLEX_CONTAINS = new LikeDimFilter(
      "foo",
      "%5_0%0_5%",
      null,
      null
  ).toFilter();

  private static final Filter REGEX_COMPLEX_CONTAINS = new RegexDimFilter(
      "foo",
      "%5_0%0_5",
      null
  ).toFilter();

  private static final Filter LIKE_KILLER = new LikeDimFilter(
      "foo",
      "%%%%x",
      null,
      null
  ).toFilter();

  private static final Filter REGEX_KILLER = new RegexDimFilter(
      "foo",
      ".*.*.*.*x",
      null
  ).toFilter();

  // cardinality, the dictionary will contain evenly spaced integers
  @Param({"1000", "100000", "1000000"})
  int cardinality;

  int step;

  // selector will contain a cardinality number of bitmaps; each one contains a single int: 0
  ColumnIndexSelector selector;

  @Setup
  public void setup()
  {
    step = (END_INT - START_INT) / cardinality;
    final BitmapFactory bitmapFactory = new RoaringBitmapFactory();
    final BitmapSerdeFactory serdeFactory = RoaringBitmapSerdeFactory.getInstance();
    final List<Integer> ints = generateInts();
    final GenericIndexed<ByteBuffer> dictionaryUtf8 = GenericIndexed.fromIterable(
        FluentIterable.from(ints)
                      .transform(i -> ByteBuffer.wrap(StringUtils.toUtf8(String.valueOf(i)))),
        GenericIndexed.UTF8_STRATEGY
    );
    final GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.fromIterable(
        FluentIterable.from(ints)
                      .transform(
                          i -> {
                            final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
                            mutableBitmap.add((i - START_INT) / step);
                            return bitmapFactory.makeImmutableBitmap(mutableBitmap);
                          }
                      ),
        serdeFactory.getObjectStrategy()
    );
    selector = new MockColumnIndexSelector(
        bitmapFactory,
        new StringUtf8ColumnIndexSupplier<>(bitmapFactory, dictionaryUtf8::singleThreaded, bitmaps, null)
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchLikeEquals()
  {
    return Filters.computeDefaultBitmapResults(LIKE_EQUALS, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchSelectorEquals()
  {
    return Filters.computeDefaultBitmapResults(SELECTOR_EQUALS, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchLikePrefix()
  {
    return Filters.computeDefaultBitmapResults(LIKE_PREFIX, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchBoundPrefix()
  {
    return Filters.computeDefaultBitmapResults(BOUND_PREFIX, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchRegexPrefix()
  {
    return Filters.computeDefaultBitmapResults(REGEX_PREFIX, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchLikeSuffix()
  {
    return Filters.computeDefaultBitmapResults(LIKE_SUFFIX, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchRegexSuffix()
  {
    return Filters.computeDefaultBitmapResults(REGEX_SUFFIX, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchLikeContains()
  {
    return Filters.computeDefaultBitmapResults(LIKE_CONTAINS, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchRegexContains()
  {
    return Filters.computeDefaultBitmapResults(REGEX_CONTAINS, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchLikeComplexContains()
  {
    return Filters.computeDefaultBitmapResults(LIKE_COMPLEX_CONTAINS, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchRegexComplexContains()
  {
    return Filters.computeDefaultBitmapResults(REGEX_COMPLEX_CONTAINS, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchLikeKiller()
  {
    return Filters.computeDefaultBitmapResults(LIKE_KILLER, selector);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public ImmutableBitmap matchRegexKiller()
  {
    return Filters.computeDefaultBitmapResults(REGEX_KILLER, selector);
  }

  private List<Integer> generateInts()
  {
    final List<Integer> ints = new ArrayList<>(cardinality);

    for (int i = 0; i < cardinality; i++) {
      ints.add(START_INT + step * i);
    }

    return ints;
  }
}
