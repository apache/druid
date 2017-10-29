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

package io.druid.collections.bitmap;

import com.carrotsearch.junitbenchmarks.BenchmarkOptions;
import com.carrotsearch.junitbenchmarks.BenchmarkRule;
import com.carrotsearch.junitbenchmarks.Clock;
import com.google.common.collect.Lists;
import io.druid.extendedset.intset.ImmutableConciseSet;
import org.junit.Assert;
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
import java.util.Locale;
import java.util.Random;


@BenchmarkOptions(clock = Clock.NANO_TIME, benchmarkRounds = 50)
public class BitmapBenchmark
{
  public static final int LENGTH = 500_000;
  public static final int SIZE = 10_000;
  final static ImmutableConciseSet concise[] = new ImmutableConciseSet[SIZE];
  final static ImmutableConciseSet offheapConcise[] = new ImmutableConciseSet[SIZE];
  final static ImmutableRoaringBitmap roaring[] = new ImmutableRoaringBitmap[SIZE];
  final static ImmutableRoaringBitmap immutableRoaring[] = new ImmutableRoaringBitmap[SIZE];
  final static ImmutableRoaringBitmap offheapRoaring[] = new ImmutableRoaringBitmap[SIZE];
  final static ImmutableBitmap genericConcise[] = new ImmutableBitmap[SIZE];
  final static ImmutableBitmap genericRoaring[] = new ImmutableBitmap[SIZE];
  final static ConciseBitmapFactory conciseFactory = new ConciseBitmapFactory();
  final static RoaringBitmapFactory roaringFactory = new RoaringBitmapFactory();
  static Random rand = new Random(0);
  static long totalConciseBytes = 0;
  static long totalRoaringBytes = 0;
  static long conciseCount = 0;
  static long roaringCount = 0;
  static long unionCount = 0;
  static long minIntersection = 0;
  @Rule
  public TestRule benchmarkRun = new BenchmarkRule();

  protected static ImmutableConciseSet makeOffheapConcise(ImmutableConciseSet concise)
  {
    final byte[] bytes = concise.toBytes();
    totalConciseBytes += bytes.length;
    conciseCount++;
    final ByteBuffer buf = ByteBuffer.allocateDirect(bytes.length).put(bytes);
    buf.rewind();
    return new ImmutableConciseSet(buf);
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
    System.out.println("");
    System.out.println("## " + name);
    System.out.println("");
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
    System.out.println("");
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
  public void timeConciseUnion() throws Exception
  {
    ImmutableConciseSet union = ImmutableConciseSet.union(concise);
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 2)
  public void timeOffheapConciseUnion() throws Exception
  {
    ImmutableConciseSet union = ImmutableConciseSet.union(offheapConcise);
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 2)
  public void timeGenericConciseUnion() throws Exception
  {
    ImmutableBitmap union = conciseFactory.union(Lists.newArrayList(genericConcise));
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  @BenchmarkOptions(warmupRounds = 1, benchmarkRounds = 5)
  public void timeGenericConciseIntersection() throws Exception
  {
    ImmutableBitmap intersection = conciseFactory.intersection(Lists.newArrayList(genericConcise));
    Assert.assertTrue(intersection.size() >= minIntersection);
  }

  @Test
  public void timeRoaringUnion() throws Exception
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Lists.newArrayList(roaring).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void timeImmutableRoaringUnion() throws Exception
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Lists.newArrayList(immutableRoaring).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void timeOffheapRoaringUnion() throws Exception
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Lists.newArrayList(offheapRoaring).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void timeGenericRoaringUnion() throws Exception
  {
    ImmutableBitmap union = roaringFactory.union(Lists.newArrayList(genericRoaring));
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  public void timeGenericRoaringIntersection() throws Exception
  {
    ImmutableBitmap intersection = roaringFactory.intersection(Lists.newArrayList(genericRoaring));
    Assert.assertTrue(intersection.size() >= minIntersection);
  }
}
