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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.query.filter.BitmapIndexSelector;
import org.apache.druid.query.filter.DruidDoublePredicate;
import org.apache.druid.query.filter.DruidFloatPredicate;
import org.apache.druid.query.filter.DruidLongPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.filter.DimensionPredicateFilter;
import org.apache.druid.segment.serde.BitmapIndexColumnPartSupplier;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class DimensionPredicateFilterBenchmark
{
  private static final int START_INT = 1_000_000_000;

  private static final DimensionPredicateFilter IS_EVEN = new DimensionPredicateFilter(
      "foo",
      new DruidPredicateFactory()
      {
        @Override
        public Predicate<String> makeStringPredicate()
        {
          return new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              if (input == null) {
                return false;
              }
              return Integer.parseInt(input) % 2 == 0;
            }
          };
        }

        @Override
        public DruidLongPredicate makeLongPredicate()
        {
          return DruidLongPredicate.ALWAYS_FALSE;
        }

        @Override
        public DruidFloatPredicate makeFloatPredicate()
        {
          return DruidFloatPredicate.ALWAYS_FALSE;
        }

        @Override
        public DruidDoublePredicate makeDoublePredicate()
        {
          return DruidDoublePredicate.ALWAYS_FALSE;
        }
      },
      null
  );

  // cardinality, the dictionary will contain integers starting from START_INT
  @Param({"1000", "100000", "1000000"})
  int cardinality;

  // selector will contain a cardinality number of bitmaps; each one contains a single int: 0
  BitmapIndexSelector selector;

  @Setup
  public void setup()
  {
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
                                  mutableBitmap.add(i - START_INT);
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
      public CloseableIndexed<String> getDimensionValues(String dimension)
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
  public void matchIsEven()
  {
    final ImmutableBitmap bitmapIndex = IS_EVEN.getBitmapIndex(selector);
    Preconditions.checkState(bitmapIndex.size() == cardinality / 2);
  }

  private List<Integer> generateInts()
  {
    final List<Integer> ints = new ArrayList<>(cardinality);

    for (int i = 0; i < cardinality; i++) {
      ints.add(START_INT + i);
    }

    return ints;
  }
}
