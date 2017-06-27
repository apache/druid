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

package io.druid.benchmark;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.RoaringBitmapFactory;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.LikeDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.serde.BitmapIndexColumnPartSupplier;
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
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class LikeFilterBenchmark
{
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

  // cardinality, the dictionary will contain evenly spaced integers
  @Param({"1000", "100000", "1000000"})
  int cardinality;

  int step;

  // selector will contain a cardinality number of bitmaps; each one contains a single int: 0
  BitmapIndexSelector selector;

  @Setup
  public void setup() throws IOException
  {
    step = (END_INT - START_INT) / cardinality;
    final BitmapFactory bitmapFactory = new RoaringBitmapFactory();
    final BitmapSerdeFactory serdeFactory = new RoaringBitmapSerdeFactory(null);
    final List<Integer> ints = generateInts();
    final GenericIndexed<String> dictionary = GenericIndexed.fromIterable(
        FluentIterable.from(ints)
                      .transform(
                          new Function<Integer, String>()
                          {
                            @Override
                            public String apply(Integer i)
                            {
                              return i.toString();
                            }
                          }
                      ),
        GenericIndexed.STRING_STRATEGY
    );
    final BitmapIndex bitmapIndex = new BitmapIndexColumnPartSupplier(
        bitmapFactory,
        GenericIndexed.fromIterable(
            FluentIterable.from(ints)
                          .transform(
                              new Function<Integer, ImmutableBitmap>()
                              {
                                @Override
                                public ImmutableBitmap apply(Integer i)
                                {
                                  final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
                                  mutableBitmap.add((i - START_INT) / step);
                                  return bitmapFactory.makeImmutableBitmap(mutableBitmap);
                                }
                              }
                          ),
            serdeFactory.getObjectStrategy()
        ),
        dictionary
    ).get();
    selector = new BitmapIndexSelector()
    {
      @Override
      public Indexed<String> getDimensionValues(String dimension)
      {
        return dictionary;
      }

      @Override
      public boolean hasMultipleValues(final String dimension)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public int getNumRows()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return bitmapFactory;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(String dimension, String value)
      {
        return bitmapIndex.getBitmap(bitmapIndex.getIndex(value));
      }

      @Override
      public BitmapIndex getBitmapIndex(String dimension)
      {
        return bitmapIndex;
      }

      @Override
      public ImmutableRTree getSpatialIndex(String dimension)
      {
        throw new UnsupportedOperationException();
      }
    };
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchLikeEquals(Blackhole blackhole)
  {
    final ImmutableBitmap bitmapIndex = LIKE_EQUALS.getBitmapIndex(selector);
    blackhole.consume(bitmapIndex);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchSelectorEquals(Blackhole blackhole)
  {
    final ImmutableBitmap bitmapIndex = SELECTOR_EQUALS.getBitmapIndex(selector);
    blackhole.consume(bitmapIndex);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchLikePrefix(Blackhole blackhole)
  {
    final ImmutableBitmap bitmapIndex = LIKE_PREFIX.getBitmapIndex(selector);
    blackhole.consume(bitmapIndex);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchBoundPrefix(Blackhole blackhole)
  {
    final ImmutableBitmap bitmapIndex = BOUND_PREFIX.getBitmapIndex(selector);
    blackhole.consume(bitmapIndex);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void matchRegexPrefix(Blackhole blackhole)
  {
    final ImmutableBitmap bitmapIndex = REGEX_PREFIX.getBitmapIndex(selector);
    blackhole.consume(bitmapIndex);
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
