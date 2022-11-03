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
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
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
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class InFilterBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final int START_INT = 10_000_000;

  private InDimFilter inFilter;
  private InDimFilter endInDimFilter;

  // cardinality of the dictionary. it will contain this many ints (as strings, of course), starting at START_INT,
  // even numbers only.
  @Param({"1000000"})
  int dictionarySize;

  // cardinality of the "in" filter. half of its values will be in the dictionary, half will not.
  @Param({"1", "10", "100", "1000", "10000"})
  int filterSize;

  // selector will contain a "dictionarySize" number of bitmaps; each one contains a single int.
  // this benchmark is not about bitmap union speed, so no need for that part to be realistic.
  ColumnIndexSelector selector;

  @Setup
  public void setup()
  {
    final BitmapFactory bitmapFactory = new RoaringBitmapFactory();
    final BitmapSerdeFactory serdeFactory = new RoaringBitmapSerdeFactory(null);
    final Iterable<Integer> ints = intGenerator();
    final GenericIndexed<String> dictionary = GenericIndexed.fromIterable(
        FluentIterable.from(ints)
                      .transform(Object::toString),
        GenericIndexed.STRING_STRATEGY
    );
    final GenericIndexed<ByteBuffer> dictionaryUtf8 = GenericIndexed.fromIterable(
        FluentIterable.from(ints)
                      .transform(i -> ByteBuffer.wrap(StringUtils.toUtf8(String.valueOf(i)))),
        GenericIndexed.BYTE_BUFFER_STRATEGY
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
    selector = new MockColumnIndexSelector(
        bitmapFactory,
        new DictionaryEncodedStringIndexSupplier(bitmapFactory, dictionary, dictionaryUtf8, bitmaps, null)
    );
    inFilter = new InDimFilter(
        "dummy",
        IntStream.range(START_INT, START_INT + filterSize).mapToObj(String::valueOf).collect(Collectors.toSet())
    );
    endInDimFilter = new InDimFilter(
        "dummy",
        IntStream.range(START_INT + dictionarySize * 2, START_INT + dictionarySize * 2 + 1)
                 .mapToObj(String::valueOf)
                 .collect(Collectors.toSet())
    );
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void doFilter(Blackhole blackhole)
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(inFilter, selector);
    blackhole.consume(bitmapIndex);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void doFilterAtEnd(Blackhole blackhole)
  {
    final ImmutableBitmap bitmapIndex = Filters.computeDefaultBitmapResults(endInDimFilter, selector);
    blackhole.consume(bitmapIndex);
  }

  private Iterable<Integer> intGenerator()
  {
    // i * 2 => half of these values will be present in the inFilter, half won't.
    return () -> IntStream.range(0, dictionarySize).map(i -> START_INT + i * 2).boxed().iterator();
  }
}
