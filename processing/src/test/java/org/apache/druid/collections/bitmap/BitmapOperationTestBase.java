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

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.extendedset.intset.ImmutableConciseSet;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.utils.CloseableUtils;
import org.junit.Assert;
import org.junit.Test;
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

public abstract class BitmapOperationTestBase
{
  public static final int BITMAP_LENGTH = 500_000;
  public static final int NUM_BITMAPS = 1000;
  static final ImmutableConciseSet[] CONCISE = new ImmutableConciseSet[NUM_BITMAPS];
  static final ResourceHolder<ImmutableConciseSet>[] OFF_HEAP_CONCISE = new ResourceHolder[NUM_BITMAPS];
  static final ImmutableRoaringBitmap[] ROARING = new ImmutableRoaringBitmap[NUM_BITMAPS];
  static final ImmutableRoaringBitmap[] IMMUTABLE_ROARING = new ImmutableRoaringBitmap[NUM_BITMAPS];
  static final ResourceHolder<ImmutableRoaringBitmap>[] OFF_HEAP_ROARING = new ResourceHolder[NUM_BITMAPS];
  static final ImmutableBitmap[] GENERIC_CONCISE = new ImmutableBitmap[NUM_BITMAPS];
  static final ImmutableBitmap[] GENERIC_ROARING = new ImmutableBitmap[NUM_BITMAPS];
  static final ConciseBitmapFactory CONCISE_FACTORY = new ConciseBitmapFactory();
  static final RoaringBitmapFactory ROARING_FACTORY = new RoaringBitmapFactory();
  static Random rand = new Random(0);
  static long totalConciseBytes = 0;
  static long totalRoaringBytes = 0;
  static long conciseCount = 0;
  static long roaringCount = 0;
  static long unionCount = 0;
  static long minIntersection = 0;

  static {
    NullHandling.initializeForTests();
  }

  protected static ResourceHolder<ImmutableConciseSet> makeOffheapConcise(ImmutableConciseSet concise)
  {
    final byte[] bytes = concise.toBytes();
    totalConciseBytes += bytes.length;
    conciseCount++;

    final ResourceHolder<ByteBuffer> bufHolder = ByteBufferUtils.allocateDirect(bytes.length);
    final ByteBuffer buf = bufHolder.get().put(bytes);
    buf.rewind();
    final ImmutableConciseSet bitmap = new ImmutableConciseSet(buf.asIntBuffer());

    return new ResourceHolder<ImmutableConciseSet>()
    {
      @Override
      public ImmutableConciseSet get()
      {
        return bitmap;
      }

      @Override
      public void close()
      {
        bufHolder.close();
      }
    };
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

  protected static void reset() throws IOException
  {
    CloseableUtils.closeAll(Arrays.asList(OFF_HEAP_CONCISE));
    CloseableUtils.closeAll(Arrays.asList(OFF_HEAP_ROARING));

    Arrays.fill(CONCISE, null);
    Arrays.fill(ROARING, null);
    Arrays.fill(IMMUTABLE_ROARING, null);
    Arrays.fill(GENERIC_CONCISE, null);
    Arrays.fill(GENERIC_ROARING, null);
    Arrays.fill(OFF_HEAP_CONCISE, null);
    Arrays.fill(OFF_HEAP_ROARING, null);

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

  protected static ResourceHolder<ImmutableRoaringBitmap> makeOffheapRoaring(MutableRoaringBitmap r) throws IOException
  {
    final int size = r.serializedSizeInBytes();
    final ResourceHolder<ByteBuffer> bufHolder = ByteBufferUtils.allocateDirect(size);
    final ByteBuffer buf = bufHolder.get();
    totalRoaringBytes += size;
    roaringCount++;
    final ImmutableRoaringBitmap bitmap = writeImmutable(r, buf);

    return new ResourceHolder<ImmutableRoaringBitmap>()
    {
      @Override
      public ImmutableRoaringBitmap get()
      {
        return bitmap;
      }

      @Override
      public void close()
      {
        bufHolder.close();
      }
    };
  }

  protected static ImmutableRoaringBitmap makeImmutableRoaring(MutableRoaringBitmap r) throws IOException
  {
    final ByteBuffer buf = ByteBuffer.allocate(r.serializedSizeInBytes());
    return writeImmutable(r, buf);
  }

  @Test
  public void testConciseUnion()
  {
    ImmutableConciseSet union = ImmutableConciseSet.union(CONCISE);
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  public void testOffheapConciseUnion()
  {
    ImmutableConciseSet union = ImmutableConciseSet.union(
        Arrays.stream(OFF_HEAP_CONCISE).map(ResourceHolder::get).iterator()
    );
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  public void testGenericConciseUnion()
  {
    ImmutableBitmap union = CONCISE_FACTORY.union(Arrays.asList(GENERIC_CONCISE));
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  public void testGenericConciseIntersection()
  {
    ImmutableBitmap intersection = CONCISE_FACTORY.intersection(Arrays.asList(GENERIC_CONCISE));
    Assert.assertTrue(intersection.size() >= minIntersection);
  }

  @Test
  public void testRoaringUnion()
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Arrays.asList(ROARING).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void testImmutableRoaringUnion()
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.horizontal_or(Arrays.asList(IMMUTABLE_ROARING).iterator());
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void testOffheapRoaringUnion()
  {
    ImmutableRoaringBitmap union = BufferFastAggregation.naive_or(
        Arrays.stream(OFF_HEAP_ROARING).map(ResourceHolder::get).iterator()
    );
    Assert.assertEquals(unionCount, union.getCardinality());
  }

  @Test
  public void testGenericRoaringUnion()
  {
    ImmutableBitmap union = ROARING_FACTORY.union(Arrays.asList(GENERIC_ROARING));
    Assert.assertEquals(unionCount, union.size());
  }

  @Test
  public void testGenericRoaringIntersection()
  {
    ImmutableBitmap intersection = ROARING_FACTORY.intersection(Arrays.asList(GENERIC_ROARING));
    Assert.assertTrue(intersection.size() >= minIntersection);
  }
}
