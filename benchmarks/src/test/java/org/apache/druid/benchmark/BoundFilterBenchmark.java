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

import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.extendedset.intset.ConciseSetUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.filter.BoundFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.serde.DictionaryEncodedStringIndexSupplier;
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
public class BoundFilterBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final int START_INT = 1_000_000_000;
  private static final int END_INT = ConciseSetUtils.MAX_ALLOWED_INTEGER;

  private static final BoundFilter NOTHING_LEXICOGRAPHIC = new BoundFilter(
      new BoundDimFilter(
          "foo",
          String.valueOf(START_INT),
          String.valueOf(START_INT),
          true,
          false,
          false,
          null,
          StringComparators.LEXICOGRAPHIC
      )
  );

  private static final BoundFilter HALF_LEXICOGRAPHIC = new BoundFilter(
      new BoundDimFilter(
          "foo",
          String.valueOf(START_INT + (END_INT - START_INT) / 2),
          String.valueOf(END_INT),
          false,
          false,
          false,
          null,
          StringComparators.LEXICOGRAPHIC
      )
  );

  private static final BoundFilter EVERYTHING_LEXICOGRAPHIC = new BoundFilter(
      new BoundDimFilter(
          "foo",
          String.valueOf(START_INT),
          String.valueOf(END_INT),
          false,
          false,
          false,
          null,
          StringComparators.LEXICOGRAPHIC
      )
  );

  private static final BoundFilter NOTHING_ALPHANUMERIC = new BoundFilter(
      new BoundDimFilter(
          "foo",
          String.valueOf(START_INT),
          String.valueOf(START_INT),
          true,
          false,
          true,
          null,
          StringComparators.ALPHANUMERIC
      )
  );

  private static final BoundFilter HALF_ALPHANUMERIC = new BoundFilter(
      new BoundDimFilter(
          "foo",
          String.valueOf(START_INT + (END_INT - START_INT) / 2),
          String.valueOf(END_INT),
          false,
          false,
          true,
          null,
          StringComparators.ALPHANUMERIC
      )
  );

  private static final BoundFilter EVERYTHING_ALPHANUMERIC = new BoundFilter(
      new BoundDimFilter(
          "foo",
          String.valueOf(START_INT),
          String.valueOf(END_INT),
          false,
          false,
          true,
          null,
          StringComparators.ALPHANUMERIC
      )
  );

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
    final BitmapSerdeFactory serdeFactory = new RoaringBitmapSerdeFactory(null);
    final List<Integer> ints = generateInts();
    final GenericIndexed<String> dictionary = GenericIndexed.fromIterable(
        FluentIterable.from(ints).transform(Object::toString),
        GenericIndexed.STRING_STRATEGY
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
    final GenericIndexed<ByteBuffer> dictionaryUtf8 = GenericIndexed.fromIterable(
        FluentIterable.from(ints)
                      .transform(i -> ByteBuffer.wrap(StringUtils.toUtf8(String.valueOf(i)))),
        GenericIndexed.BYTE_BUFFER_STRATEGY
    );
    selector = new MockColumnIndexSelector(
        bitmapFactory,
        new DictionaryEncodedStringIndexSupplier(bitmapFactory, dictionary, dictionaryUtf8, bitmaps, null)
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchNothingLexicographic()
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(NOTHING_LEXICOGRAPHIC, selector);
    Preconditions.checkState(bitmapIndex.size() == 0);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchHalfLexicographic()
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(HALF_LEXICOGRAPHIC, selector);
    Preconditions.checkState(bitmapIndex.size() > 0 && bitmapIndex.size() < cardinality);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchEverythingLexicographic()
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(EVERYTHING_LEXICOGRAPHIC, selector);
    Preconditions.checkState(bitmapIndex.size() == cardinality);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchNothingAlphaNumeric()
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(NOTHING_ALPHANUMERIC, selector);
    Preconditions.checkState(bitmapIndex.size() == 0);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchHalfAlphaNumeric()
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(HALF_ALPHANUMERIC, selector);
    Preconditions.checkState(bitmapIndex.size() > 0 && bitmapIndex.size() < cardinality);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchEverythingAlphaNumeric()
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(EVERYTHING_ALPHANUMERIC, selector);
    Preconditions.checkState(bitmapIndex.size() == cardinality);
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
