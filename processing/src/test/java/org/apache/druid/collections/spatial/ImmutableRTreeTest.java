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

package org.apache.druid.collections.spatial;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import junit.framework.Assert;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.RoaringBitmapFactory;
import org.apache.druid.collections.spatial.search.PolygonBound;
import org.apache.druid.collections.spatial.search.RadiusBound;
import org.apache.druid.collections.spatial.search.RectangularBound;
import org.apache.druid.collections.spatial.split.LinearGutmanSplitStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ImmutableRTreeObjectStrategy;
import org.junit.Test;
import org.roaringbitmap.IntIterator;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 */
public class ImmutableRTreeTest
{
  @Test
  public void testToAndFromByteBuffer()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);

    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{1, 1}, 2);
    tree.insert(new float[]{2, 2}, 3);
    tree.insert(new float[]{3, 3}, 4);
    tree.insert(new float[]{4, 4}, 5);

    ImmutableRTree firstTree = ImmutableRTree.newImmutableFromMutable(tree);
    ByteBuffer buffer = ByteBuffer.wrap(firstTree.toBytes());
    ImmutableRTree secondTree = new ImmutableRTree(buffer, bf);
    Iterable<ImmutableBitmap> points = secondTree.search(new RadiusBound(new float[]{0, 0}, 10));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);
    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testToAndFromByteBufferRoaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);

    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{1, 1}, 2);
    tree.insert(new float[]{2, 2}, 3);
    tree.insert(new float[]{3, 3}, 4);
    tree.insert(new float[]{4, 4}, 5);

    ImmutableRTree firstTree = ImmutableRTree.newImmutableFromMutable(tree);
    ByteBuffer buffer = ByteBuffer.wrap(firstTree.toBytes());
    ImmutableRTree secondTree = new ImmutableRTree(buffer, bf);
    Iterable<ImmutableBitmap> points = secondTree.search(new RadiusBound(new float[]{0, 0}, 10));
    ImmutableBitmap finalSet = bf.union(points);

    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchNoSplit()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{10, 10}, 10);
    tree.insert(new float[]{1, 3}, 2);
    tree.insert(new float[]{27, 34}, 20);
    tree.insert(new float[]{106, 19}, 30);
    tree.insert(new float[]{4, 2}, 3);
    tree.insert(new float[]{5, 0}, 4);
    tree.insert(new float[]{4, 72}, 40);
    tree.insert(new float[]{-4, -3}, 5);
    tree.insert(new float[]{119, -78}, 50);

    Assert.assertEquals(tree.getRoot().getChildren().size(), 10);

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(new RadiusBound(new float[]{0, 0}, 5));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchNoSplitRoaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{10, 10}, 10);
    tree.insert(new float[]{1, 3}, 2);
    tree.insert(new float[]{27, 34}, 20);
    tree.insert(new float[]{106, 19}, 30);
    tree.insert(new float[]{4, 2}, 3);
    tree.insert(new float[]{5, 0}, 4);
    tree.insert(new float[]{4, 72}, 40);
    tree.insert(new float[]{-4, -3}, 5);
    tree.insert(new float[]{119, -78}, 50);

    Assert.assertEquals(tree.getRoot().getChildren().size(), 10);

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(new RadiusBound(new float[]{0, 0}, 5));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchWithSplit()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{1, 3}, 2);
    tree.insert(new float[]{4, 2}, 3);
    tree.insert(new float[]{5, 0}, 4);
    tree.insert(new float[]{-4, -3}, 5);

    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 95; i++) {
      tree.insert(
          new float[]{(float) (rand.nextDouble() * 10 + 10.0), (float) (rand.nextDouble() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(new RadiusBound(new float[]{0, 0}, 5));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchWithSplitRoaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{1, 3}, 2);
    tree.insert(new float[]{4, 2}, 3);
    tree.insert(new float[]{5, 0}, 4);
    tree.insert(new float[]{-4, -3}, 5);

    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 95; i++) {
      tree.insert(
          new float[]{(float) (rand.nextDouble() * 10 + 10.0), (float) (rand.nextDouble() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(new RadiusBound(new float[]{0, 0}, 5));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }


  @Test
  public void testSearchWithSplit2()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0.0f, 0.0f}, 0);
    tree.insert(new float[]{1.0f, 3.0f}, 1);
    tree.insert(new float[]{4.0f, 2.0f}, 2);
    tree.insert(new float[]{7.0f, 3.0f}, 3);
    tree.insert(new float[]{8.0f, 6.0f}, 4);

    Random rand = ThreadLocalRandom.current();
    for (int i = 5; i < 5000; i++) {
      tree.insert(
          new float[]{(float) (rand.nextDouble() * 10 + 10.0), (float) (rand.nextDouble() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(
        new RectangularBound(
            new float[]{0, 0},
            new float[]{9, 9}
        )
    );
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(0, 1, 2, 3, 4);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchWithSplit2Roaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0.0f, 0.0f}, 0);
    tree.insert(new float[]{1.0f, 3.0f}, 1);
    tree.insert(new float[]{4.0f, 2.0f}, 2);
    tree.insert(new float[]{7.0f, 3.0f}, 3);
    tree.insert(new float[]{8.0f, 6.0f}, 4);

    Random rand = ThreadLocalRandom.current();
    for (int i = 5; i < 5000; i++) {
      tree.insert(
          new float[]{(float) (rand.nextDouble() * 10 + 10.0), (float) (rand.nextDouble() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(
        new RectangularBound(
            new float[]{0, 0},
            new float[]{9, 9}
        )
    );
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(0, 1, 2, 3, 4);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchWithSplit3()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0.0f, 0.0f}, 0);
    tree.insert(new float[]{1.0f, 3.0f}, 1);
    tree.insert(new float[]{4.0f, 2.0f}, 2);
    tree.insert(new float[]{7.0f, 3.0f}, 3);
    tree.insert(new float[]{8.0f, 6.0f}, 4);

    Random rand = ThreadLocalRandom.current();
    for (int i = 5; i < 5000; i++) {
      tree.insert(
          new float[]{(float) (rand.nextFloat() * 10 + 10.0), (float) (rand.nextFloat() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(
        new RadiusBound(new float[]{0.0f, 0.0f}, 5)
    );
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 3);

    Set<Integer> expected = Sets.newHashSet(0, 1, 2);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchWithSplit3Roaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0.0f, 0.0f}, 0);
    tree.insert(new float[]{1.0f, 3.0f}, 1);
    tree.insert(new float[]{4.0f, 2.0f}, 2);
    tree.insert(new float[]{7.0f, 3.0f}, 3);
    tree.insert(new float[]{8.0f, 6.0f}, 4);

    Random rand = ThreadLocalRandom.current();
    for (int i = 5; i < 5000; i++) {
      tree.insert(
          new float[]{(float) (rand.nextFloat() * 10 + 10.0), (float) (rand.nextFloat() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(
        new RadiusBound(new float[]{0.0f, 0.0f}, 5)
    );
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 3);

    Set<Integer> expected = Sets.newHashSet(0, 1, 2);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchWithSplit4()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    Random rand = ThreadLocalRandom.current();

    int outPolygon = 0, inPolygon = 0;
    for (; inPolygon < 500; ) {
      double abscissa = rand.nextDouble() * 5;
      double ordinate = rand.nextDouble() * 4;

      if (abscissa < 1 || abscissa > 4 || ordinate < 1 || ordinate > 3 || abscissa < 2 && ordinate > 2) {
        tree.insert(
            new float[]{(float) abscissa, (float) ordinate},
            outPolygon + 500
        );
        outPolygon++;
      } else if (abscissa > 1 && abscissa < 4 && ordinate > 1 && ordinate < 2
                 || abscissa > 2 && abscissa < 4 && ordinate >= 2 && ordinate < 3) {
        tree.insert(
            new float[]{(float) abscissa, (float) ordinate},
            inPolygon
        );
        inPolygon++;
      }
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(PolygonBound.from(
        new float[]{1.0f, 1.0f, 2.0f, 2.0f, 4.0f, 4.0f},
        new float[]{1.0f, 2.0f, 2.0f, 3.0f, 3.0f, 1.0f}
    ));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() == 500);

    Set<Integer> expected = new HashSet<>();
    for (int i = 0; i < 500; i++) {
      expected.add(i);
    }
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testSearchWithSplit4Roaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    Random rand = ThreadLocalRandom.current();

    int outPolygon = 0, inPolygon = 0;
    for (; inPolygon < 500; ) {
      double abscissa = rand.nextDouble() * 5;
      double ordinate = rand.nextDouble() * 4;

      if (abscissa < 1 || abscissa > 4 || ordinate < 1 || ordinate > 3 || abscissa < 2 && ordinate > 2) {
        tree.insert(
            new float[]{(float) abscissa, (float) ordinate},
            outPolygon + 500
        );
        outPolygon++;
      } else if (abscissa > 1 && abscissa < 4 && ordinate > 1 && ordinate < 2
                 || abscissa > 2 && abscissa < 4 && ordinate >= 2 && ordinate < 3) {
        tree.insert(
            new float[]{(float) abscissa, (float) ordinate},
            inPolygon
        );
        inPolygon++;
      }
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(PolygonBound.from(
        new float[]{1.0f, 1.0f, 2.0f, 2.0f, 4.0f, 4.0f},
        new float[]{1.0f, 2.0f, 2.0f, 3.0f, 3.0f, 1.0f}
    ));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() == 500);

    Set<Integer> expected = new HashSet<>();
    for (int i = 0; i < 500; i++) {
      expected.add(i);
    }
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @Test
  public void testEmptyConciseSet()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0.0f, 0.0f}, bf.makeEmptyMutableBitmap());

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(
        new RadiusBound(new float[]{0.0f, 0.0f}, 5)
    );
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertEquals(finalSet.size(), 0);
  }

  @Test
  public void testEmptyRoaringBitmap()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0.0f, 0.0f}, bf.makeEmptyMutableBitmap());

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(
        new RadiusBound(new float[]{0.0f, 0.0f}, 5)
    );
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertEquals(finalSet.size(), 0);
    Assert.assertTrue(finalSet.isEmpty());
  }

  @Test
  public void testSearchWithSplitLimitedBound()
  {
    BitmapFactory bf = new ConciseBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{1, 3}, 2);
    tree.insert(new float[]{4, 2}, 3);
    tree.insert(new float[]{5, 0}, 4);
    tree.insert(new float[]{-4, -3}, 5);

    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 4995; i++) {
      tree.insert(
          new float[]{(float) (rand.nextDouble() * 10 + 10.0), (float) (rand.nextDouble() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(new RadiusBound(new float[]{0, 0}, 5, 2));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }


  @Test
  public void testSearchWithSplitLimitedBoundRoaring()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    tree.insert(new float[]{0, 0}, 1);
    tree.insert(new float[]{1, 3}, 2);
    tree.insert(new float[]{4, 2}, 3);
    tree.insert(new float[]{5, 0}, 4);
    tree.insert(new float[]{-4, -3}, 5);

    Random rand = ThreadLocalRandom.current();
    for (int i = 0; i < 4995; i++) {
      tree.insert(
          new float[]{(float) (rand.nextDouble() * 10 + 10.0), (float) (rand.nextDouble() * 10 + 10.0)},
          i
      );
    }

    ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
    Iterable<ImmutableBitmap> points = searchTree.search(new RadiusBound(new float[]{0, 0}, 5, 2));
    ImmutableBitmap finalSet = bf.union(points);
    Assert.assertTrue(finalSet.size() >= 5);

    Set<Integer> expected = Sets.newHashSet(1, 2, 3, 4, 5);
    IntIterator iter = finalSet.iterator();
    while (iter.hasNext()) {
      Assert.assertTrue(expected.contains(iter.next()));
    }
  }

  @SuppressWarnings("unused") // TODO rewrite to JMH and move to the benchmarks project
  public void showBenchmarks()
  {
    final int start = 1;
    final int factor = 10;
    final int end = 10000000;
    final int radius = 10;

    for (int numPoints = start; numPoints <= end; numPoints *= factor) {
      try {
        BitmapFactory bf = new ConciseBitmapFactory();
        RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);

        Stopwatch stopwatch = Stopwatch.createStarted();
        Random rand = ThreadLocalRandom.current();
        for (int i = 0; i < numPoints; i++) {
          tree.insert(new float[]{(float) (rand.nextDouble() * 100), (float) (rand.nextDouble() * 100)}, i);
        }
        long stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf(Locale.ENGLISH, "[%,d]: insert = %,d ms%n", numPoints, stop);

        stopwatch.reset().start();
        ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
        stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf(Locale.ENGLISH, "[%,d]: size = %,d bytes%n", numPoints, searchTree.toBytes().length);
        System.out.printf(Locale.ENGLISH, "[%,d]: buildImmutable = %,d ms%n", numPoints, stop);

        stopwatch.reset().start();

        Iterable<ImmutableBitmap> points = searchTree.search(new RadiusBound(new float[]{50, 50}, radius));

        Iterables.size(points);
        stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        System.out.printf(Locale.ENGLISH, "[%,d]: search = %,dms%n", numPoints, stop);

        stopwatch.reset().start();

        ImmutableBitmap finalSet = bf.union(points);

        stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf(Locale.ENGLISH, "[%,d]: union of %,d points in %,d ms%n", numPoints, finalSet.size(), stop);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unused") // TODO rewrite to JMH and move to the benchmarks project
  public void showBenchmarksBoundWithLimits()
  {
    //final int start = 1;
    final int start = 10000000;
    final int factor = 10;
    final int end = 10000000;
    //final int end = 10;

    for (int numPoints = start; numPoints <= end; numPoints *= factor) {
      try {
        BitmapFactory bf = new ConciseBitmapFactory();
        RTree tree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);

        Stopwatch stopwatch = Stopwatch.createStarted();
        Random rand = ThreadLocalRandom.current();
        for (int i = 0; i < numPoints; i++) {
          tree.insert(new float[]{(float) (rand.nextDouble() * 100), (float) (rand.nextDouble() * 100)}, i);
        }
        long stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf(Locale.ENGLISH, "[%,d]: insert = %,d ms%n", numPoints, stop);

        stopwatch.reset().start();
        ImmutableRTree searchTree = ImmutableRTree.newImmutableFromMutable(tree);
        stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf(Locale.ENGLISH, "[%,d]: size = %,d bytes%n", numPoints, searchTree.toBytes().length);
        System.out.printf(Locale.ENGLISH, "[%,d]: buildImmutable = %,d ms%n", numPoints, stop);

        stopwatch.reset().start();

        Iterable<ImmutableBitmap> points = searchTree.search(
            new RectangularBound(
                new float[]{40, 40},
                new float[]{60, 60},
                100
            )
        );

        Iterables.size(points);
        stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);

        System.out.printf(Locale.ENGLISH, "[%,d]: search = %,dms%n", numPoints, stop);

        stopwatch.reset().start();

        ImmutableBitmap finalSet = bf.union(points);

        stop = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        System.out.printf(Locale.ENGLISH, "[%,d]: union of %,d points in %,d ms%n", numPoints, finalSet.size(), stop);
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testToBytes()
  {
    BitmapFactory bf = new RoaringBitmapFactory();
    ImmutableRTreeObjectStrategy rTreeObjectStrategy = new ImmutableRTreeObjectStrategy(bf);
    RTree rTree = new RTree(2, new LinearGutmanSplitStrategy(0, 50, bf), bf);
    rTree.insert(new float[]{0, 0}, 1);
    ImmutableRTree immutableRTree = ImmutableRTree.newImmutableFromMutable(rTree);
    byte[] bytes1 = immutableRTree.toBytes();

    GenericIndexed<ImmutableRTree> genericIndexed = GenericIndexed.fromIterable(
        Arrays.asList(immutableRTree, immutableRTree),
        rTreeObjectStrategy
    );

    ImmutableRTree deserializedTree = genericIndexed.get(0);
    byte[] bytes2 = deserializedTree.toBytes();
    org.junit.Assert.assertEquals(Bytes.asList(bytes1), Bytes.asList(bytes2));
  }
}
