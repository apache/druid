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

package io.druid.segment;

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import io.druid.collections.bitmap.WrappedRoaringBitmap;
import io.druid.extendedset.intset.EmptyIntIterator;
import io.druid.java.util.common.RE;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.Offset;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import org.roaringbitmap.IntIterator;

import java.util.Arrays;
import java.util.HashSet;

/**
 */
public class BitmapOffset extends Offset
{
  private static final int INVALID_VALUE = -1;
  private static final BitmapFactory ROARING_BITMAP_FACTORY = new RoaringBitmapSerdeFactory(false).getBitmapFactory();

  /**
   * Currently the default stops are not consciously optimized for the goals described in {@link #factorizeFullness}.
   * They are chosen intuitively. There was no experimentation with different bitmapFullnessFactorizationStops.
   * Experimentation and performance feedback with a different set of stops is welcome.
   */
  private static final String DEFAULT_FULLNESS_FACTORIZATION_STOPS = "0.01,0.1,0.3,0.5,0.7,0.9,0.99";
  private static final double[] BITMAP_FULLNESS_FACTORIZATION_STOPS;
  private static final String[] FACTORIZED_FULLNESS;
  static {
    String stopString = System.getProperty("bitmapFullnessFactorizationStops", DEFAULT_FULLNESS_FACTORIZATION_STOPS);
    String[] stopsArray = stopString.split(",");
    if (stopsArray.length == 0) {
      throw new RE("Empty bitmapFullnessFactorizationStops: " + stopString);
    }
    if (new HashSet<>(Arrays.asList(stopsArray)).size() != stopsArray.length) {
      throw new RE("Non unique bitmapFullnessFactorizationStops: " + stopString);
    }

    BITMAP_FULLNESS_FACTORIZATION_STOPS = new double[stopsArray.length];
    for (int i = 0; i < stopsArray.length; i++) {
      String stop = stopsArray[i];
      BITMAP_FULLNESS_FACTORIZATION_STOPS[i] = Double.parseDouble(stop);

    }
    Arrays.sort(BITMAP_FULLNESS_FACTORIZATION_STOPS);


    double firstStop = BITMAP_FULLNESS_FACTORIZATION_STOPS[0];
    if (Double.isNaN(firstStop) || firstStop <= 0.0) {
      throw new RE("First bitmapFullnessFactorizationStop[%d] should be > 0", firstStop);
    }
    double lastStop = BITMAP_FULLNESS_FACTORIZATION_STOPS[stopsArray.length - 1];
    if (Double.isNaN(lastStop) || lastStop >= 1) {
      throw new RE("Last bitmapFullnessFactorizationStop[%d] should be < 1", lastStop);
    }

    String prevStop = "0";
    FACTORIZED_FULLNESS = new String[stopsArray.length + 1];
    for (int i = 0; i < stopsArray.length; i++) {
      String stop = String.valueOf(BITMAP_FULLNESS_FACTORIZATION_STOPS[i]);
      FACTORIZED_FULLNESS[i] = "(" + prevStop + ", " + stop + "]";
      prevStop = stop;
    }
    FACTORIZED_FULLNESS[stopsArray.length] = "(" + prevStop + ", 1)";
  }

  /**
   * Processing of queries with BitmapOffsets, whose Bitmaps has different factorized fullness (bucket), reported from
   * this method, uses different copies of the same code, so JIT compiler analyzes and compiles the code for different
   * factorized fullness separately. The goal is to capture frequency of abstraction usage in compressed bitmap
   * algorithms, i. e.
   *    - "Zero sequence" vs. "Literal" vs. "One sequence" in {@link io.druid.extendedset.intset.ImmutableConciseSet}
   *    - {@link org.roaringbitmap.ArrayContainer} vs {@link org.roaringbitmap.BitmapContainer} in Roaring
   * and then https://shipilev.net/blog/2015/black-magic-method-dispatch/ comes into play. The secondary goal is to
   * capture HotSpot's thresholds, which it uses to compile conditional blocks differently inside bitmap impls. See
   * https://bugs.openjdk.java.net/browse/JDK-6743900. The default BlockLayoutMinDiamondPercentage=20, i. e. if
   * probability of taking some branch is less than 20%, it is moved out of the hot path (to save some icache?).
   *
   * On the other hand, we don't want to factor fullness into too small pieces, because
   *  - too little queries may fall into those small buckets, and they are not compiled with Hotspot's C2 compiler
   *  - if there are a lot of queries for each small factorized fullness and their copies of the code is compiled by
   *  C2, this pollutes code cache and takes time to perform too many compilations, while some of them likely produce
   *  identical code.
   *
   *  Ideally there should be as much buckets as possible as long as Hotspot's C2 output for each bucket is different.
   */
  private static String factorizeFullness(long bitmapCardinality, long numRows)
  {
    if (bitmapCardinality == 0) {
      return "0";
    } else if (bitmapCardinality == numRows) {
      return "1";
    } else {
      double fullness = bitmapCardinality / (double) numRows;
      int index = Arrays.binarySearch(BITMAP_FULLNESS_FACTORIZATION_STOPS, fullness);
      if (index < 0) {
        index = ~index;
      }
      return FACTORIZED_FULLNESS[index];
    }
  }

  final IntIterator itr;
  final String fullness;

  int val;

  public static IntIterator getReverseBitmapOffsetIterator(ImmutableBitmap bitmapIndex)
  {
    ImmutableBitmap roaringBitmap = bitmapIndex;
    if (!(bitmapIndex instanceof WrappedImmutableRoaringBitmap)) {
      final MutableBitmap bitmap = ROARING_BITMAP_FACTORY.makeEmptyMutableBitmap();
      final IntIterator iterator = bitmapIndex.iterator();
      while (iterator.hasNext()) {
        bitmap.add(iterator.next());
      }
      roaringBitmap = ROARING_BITMAP_FACTORY.makeImmutableBitmap(bitmap);
    }
    return ((WrappedImmutableRoaringBitmap) roaringBitmap).getBitmap().getReverseIntIterator();
  }

  public static BitmapOffset of(ImmutableBitmap bitmapIndex, boolean descending, long numRows)
  {
    if (bitmapIndex instanceof WrappedImmutableRoaringBitmap ||
        bitmapIndex instanceof WrappedRoaringBitmap ||
        descending) {
      return new RoaringBitmapOffset(bitmapIndex, descending, numRows);
    } else {
      return new BitmapOffset(bitmapIndex, descending, numRows);
    }
  }

  private BitmapOffset(ImmutableBitmap bitmapIndex, boolean descending, long numRows)
  {
    this.itr = newIterator(bitmapIndex, descending);
    this.fullness = factorizeFullness(bitmapIndex.size(), numRows);
    increment();
  }

  private IntIterator newIterator(ImmutableBitmap bitmapIndex, boolean descending)
  {
    if (!descending) {
      return bitmapIndex.iterator();
    } else {
      return getReverseBitmapOffsetIterator(bitmapIndex);
    }
  }

  private BitmapOffset(String fullness, IntIterator itr, int val)
  {
    this.fullness = fullness;
    this.itr = itr;
    this.val = val;
  }

  @Override
  public void increment()
  {
    if (itr.hasNext()) {
      val = itr.next();
    } else {
      val = INVALID_VALUE;
    }
  }

  @Override
  public boolean withinBounds()
  {
    return val > INVALID_VALUE;
  }

  @Override
  public Offset clone()
  {
    return new BitmapOffset(fullness, itr.clone(), val);
  }

  @Override
  public int getOffset()
  {
    return val;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("itr", itr);
    inspector.visit("fullness", fullness);
  }

  public static class RoaringBitmapOffset extends BitmapOffset
  {

    public RoaringBitmapOffset(ImmutableBitmap bitmapIndex, boolean descending, long numRows)
    {
      super(bitmapIndex, descending, numRows);
    }

    RoaringBitmapOffset(String fullness, IntIterator itr, int val)
    {
      super(fullness, itr, val);
    }

    @Override
    public Offset clone()
    {
      return new RoaringBitmapOffset(fullness, itr.hasNext() ? itr.clone() : EmptyIntIterator.instance(), val);
    }
  }
}
