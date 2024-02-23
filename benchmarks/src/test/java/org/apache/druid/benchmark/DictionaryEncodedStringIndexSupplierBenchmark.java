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
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.index.BitmapColumnIndex;
import org.apache.druid.segment.index.IndexedUtf8ValueIndexes;
import org.apache.druid.segment.index.semantic.StringValueSetIndexes;
import org.apache.druid.segment.serde.StringUtf8ColumnIndexSupplier;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class DictionaryEncodedStringIndexSupplierBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState
  {
    @Nullable
    private IndexedUtf8ValueIndexes<?> stringValueSetIndex;
    private final TreeSet<ByteBuffer> values = new TreeSet<>();
    private static final int START_INT = 10_000_000;

    // cardinality of the dictionary. it will contain this many ints (as strings, of course), starting at START_INT,
    // even numbers only.
    @Param({"1000000"})
    int dictionarySize;

    @Param({"1", "2", "5", "10", "15", "20", "30", "50", "100"})
    int filterToDictionaryPercentage;

    @Param({"10", "100"})
    int selectivityPercentage;

    @Setup(Level.Trial)
    public void setup()
    {
      final BitmapFactory bitmapFactory = new RoaringBitmapFactory();
      final BitmapSerdeFactory serdeFactory = RoaringBitmapSerdeFactory.getInstance();
      final Iterable<Integer> ints = intGenerator();
      final GenericIndexed<ByteBuffer> dictionaryUtf8 = GenericIndexed.fromIterable(
          FluentIterable.from(ints)
                        .transform(i -> ByteBuffer.wrap(StringUtils.toUtf8(String.valueOf(i)))),
          GenericIndexed.UTF8_STRATEGY
      );
      final GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.fromIterable(
          () -> IntStream.range(0, dictionarySize)
                         .mapToObj(
                             i -> {
                               final MutableBitmap mutableBitmap = bitmapFactory.makeEmptyMutableBitmap();
                               mutableBitmap.add(i);
                               return bitmapFactory.makeImmutableBitmap(mutableBitmap);
                             }
                         )
                         .iterator(),
          serdeFactory.getObjectStrategy()
      );
      StringUtf8ColumnIndexSupplier<?> indexSupplier =
          new StringUtf8ColumnIndexSupplier<>(bitmapFactory, dictionaryUtf8::singleThreaded, bitmaps, null);
      stringValueSetIndex = (IndexedUtf8ValueIndexes<?>) indexSupplier.as(StringValueSetIndexes.class);
      List<Integer> filterValues = new ArrayList<>();
      List<Integer> nonFilterValues = new ArrayList<>();
      for (int i = 0; i < dictionarySize; i++) {
        filterValues.add(START_INT + i * 2);
        nonFilterValues.add(START_INT + i * 2 + 1);
      }
      Random r = new Random(9001);
      Collections.shuffle(filterValues);
      Collections.shuffle(nonFilterValues);
      values.clear();
      for (int i = 0; i < filterToDictionaryPercentage * dictionarySize / 100; i++) {
        if (r.nextInt(100) < selectivityPercentage) {
          values.add(ByteBuffer.wrap((filterValues.get(i).toString()).getBytes(StandardCharsets.UTF_8)));
        } else {
          values.add(ByteBuffer.wrap((nonFilterValues.get(i).toString()).getBytes(StandardCharsets.UTF_8)));
        }
      }
    }

    private Iterable<Integer> intGenerator()
    {
      // i * 2 => half of these values will be present in the inFilter, half won't.
      return () -> IntStream.range(0, dictionarySize).map(i -> START_INT + i * 2).boxed().iterator();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void doValueSetCheck(Blackhole blackhole, BenchmarkState state)
  {
    BitmapColumnIndex bitmapIndex = state.stringValueSetIndex.forSortedValuesUtf8(state.values);
  }
}
