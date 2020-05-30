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

package org.apache.druid.collections.bitmap;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.Clock;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

/**
 * TODO rewrite this benchmark to JMH
 * If you want to run locally, remove @Ignore on the class.
 */
@BenchmarkOptions(clock = Clock.NANO_TIME, benchmarkRounds = 50)
@Ignore
public class BitmapBenchmark
{
  public static final int LENGTH = 500_000;
  public static final int SIZE = 10_000;
  static final ImmutableConciseSet[] CONCISE = new ImmutableConciseSet[SIZE];
  static final ImmutableConciseSet[] OFF_HEAP_CONCISE = new ImmutableConciseSet[SIZE];
  static final ImmutableRoaringBitmap[] ROARING = new ImmutableRoaringBitmap[SIZE];
  static final ImmutableRoaringBitmap[] IMMUTABLE_ROARING = new ImmutableRoaringBitmap[SIZE];
  static final ImmutableRoaringBitmap[] OFF_HEAP_ROARING = new ImmutableRoaringBitmap[SIZE];
  static final ImmutableBitmap[] GENERIC_CONCISE = new ImmutableBitmap[SIZE];
  static final ImmutableBitmap[] GENERIC_ROARING = new ImmutableBitmap[SIZE];
  static final ConciseBitmapFactory CONCISE_FACTORY = new ConciseBitmapFactory();
  static final RoaringBitmapFactory ROARING_FACTORY = new RoaringBitmapFactory();
  static Random rand = new Random(0);
  static long totalConciseBytes = 0;
  static long totalRoaringBytes = 0;
  static long conciseCount = 0;
  static long roaringCount = 0;
  static long unionCount = 0;
  static long minIntersection = 0;
  @Rule
  public TestRule benchmarkRun = new BenchmarkRule();

  static {
    NullHandling.initializeForTests();
  }

  protected static ImmutableConciseSet makeOffheapConcise(ImmutableConciseSet concise)
  {
    final byte[] bytes = concise.toBytes();
    totalConciseBytes += bytes.length;
    conciseCount++;
    final ByteBuffer buf = ByteBuffer.allocateDirect(bytes.length).put(bytes);
    buf.rewind();
    return new ImmutableConciseSet(buf.asIntBuffer());
  }

  protected static ImmutableRoaringBitmap writeImmutable(MutableRoaringBitmap r, ByteBuffer buf) throws IOException
  {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    r.serialize(new DataOutputStream(out));
    final byte[] bytes = out.toByteArray();
    Assert.assertEquals(buf.remaining(), bytes.length);
    buf.put(bytes);
    buf.rewind();
    return new ImmutableRoaringBitmap(buf.asReadOnlyBuffer());
  }

  protected static void reset()
  {
    conciseCount = 0;
    roaringCount = 0;
    totalConciseBytes = 0;
    totalRoaringBytes = 0;
    unionCount = 0;
    minIntersection = 0;
    rand = new Random(0);
  }

  protected static void printSizeStats(double density, String name)
  {
    System.out.println();
    System.out.println("## " + name);
    System.out.println();
    System.out.printf(Locale.ENGLISH, " d = %06.5f | Concise | Roaring%n", density);
    System.out.println("-------------|---------|---------");
    System.out.printf(Locale.ENGLISH, "Count        |   %5d |   %5d %n", conciseCount, roaringCount);
    System.out.printf(
        Locale.ENGLISH,
        "Average size |   %5d |   %5d %n",
        totalConciseBytes / conciseCount,
        totalRoaringBytes / roaringCount
    );
    System.out.println("-------------|---------|---------");
    System.out.println();
    System.out.flush();
  }

  protected static ImmutableRoaringBitmap makeOffheapRoaring(MutableRoaringBitmap r) throws IOException
  {
    final int size = r.serializedSizeInBytes();
    final ByteBuffer buf = ByteBuffer.allocateDirect(size);
    totalRoaringBytes += size;
    roaringCount++;
    return writeImmutable(r, buf);
  }

  protected static ImmutableRoaringBitmap makeImmutableRoaring(MutableRoaringBitmap r) throws IOException
  {
    final ByteBuffer buf = ByteBuffer.allocate(r.serializedSizeInBytes());
    return writeImmutable(r, buf);
  }

  @Test
  @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 2)
  public void timeConciseUnion()
  {
    ImmutableConciseSet union = ImmutableConciseSet.union(CONCISE);
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 2)
  public void timeOffheapConciseUnion()
  {
    ImmutableConciseSet union = ImmutableConciseSet.union(OFF_HEAP_CONCISE);
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 2)
  public void timeGenericConciseUnion()
  {
    ImmutableBitmap union = CONCISE_FACTORY.union(Arrays.asList(GENERIC_CONCISE));
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 5)
  public void timeGenericConciseIntersection()
  {
    ImmutableBitmap intersection = CONCISE_FACTORY.intersection(Arrays.asList(GENERIC_CONCISE));
    Assert.assertTrue(intersection.size() >= minIntersection);
  }

  @Test
  public void timeRoaringUnion()
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Arrays.asList(ROARING).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void timeImmutableRoaringUnion()
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Arrays.asList(IMMUTABLE_ROARING).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void timeOffheapRoaringUnion()
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Arrays.asList(OFF_HEAP_ROARING).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void timeGenericRoaringUnion()
  {
    ImmutableBitmap union = ROARING_FACTORY.union(Arrays.asList(GENERIC_ROARING));
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  public void timeGenericRoaringIntersection()
  {
    ImmutableBitmap intersection = ROARING_FACTORY.intersection(Arrays.asList(GENERIC_ROARING));
    Assert.assertTrue(intersection.size() >= minIntersection);
  }
}
