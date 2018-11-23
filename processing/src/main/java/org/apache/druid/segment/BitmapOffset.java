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

package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.extendedset.intset.EmptyIntIterator;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
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
   *    - "Zero sequence" vs. "Literal" vs. "One sequence" in {@link org.apache.druid.extendedset.intset.ImmutableConciseSet}
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

  private final String fullness;
  private IntIterator iterator;
  private final IntIterator iteratorForReset;
  private final int valueForReset;
  private int value;

  static IntIterator getReverseBitmapOffsetIterator(ImmutableBitmap bitmapIndex)
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
    return new BitmapOffset(bitmapIndex, descending, numRows);
  }

  private BitmapOffset(ImmutableBitmap bitmapIndex, boolean descending, long numRows)
  {
    this.fullness = factorizeFullness(bitmapIndex.size(), numRows);
    this.iterator = newIterator(bitmapIndex, descending);
    increment();
    // It's important to set iteratorForReset and valueForReset after calling increment(), because only after that the
    // iterator and the value are in proper initial state.
    this.iteratorForReset = safeClone(iterator);
    this.valueForReset = value;
  }

  private IntIterator newIterator(ImmutableBitmap bitmapIndex, boolean descending)
  {
    if (!descending) {
      return bitmapIndex.iterator();
    } else {
      return getReverseBitmapOffsetIterator(bitmapIndex);
    }
  }

  /**
   * Constructor for {@link #clone()}.
   */
  private BitmapOffset(String fullness, IntIterator iterator, int value)
  {
    this.fullness = fullness;
    this.iterator = iterator;
    this.iteratorForReset = safeClone(iterator);
    this.valueForReset = value;
    this.value = value;
  }

  @Override
  public void increment()
  {
    if (iterator.hasNext()) {
      value = iterator.next();
    } else {
      value = INVALID_VALUE;
    }
  }

  @Override
  public boolean withinBounds()
  {
    return value > INVALID_VALUE;
  }

  @Override
  public void reset()
  {
    iterator = safeClone(iteratorForReset);
    value = valueForReset;
  }

  @Override
  public ReadableOffset getBaseReadableOffset()
  {
    return this;
  }

  @SuppressWarnings("MethodDoesntCallSuperMethod")
  @Override
  public Offset clone()
  {
    return new BitmapOffset(fullness, safeClone(iterator), value);
  }

  @Override
  public int getOffset()
  {
    return value;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("iterator", iterator);
    inspector.visit("fullness", fullness);
  }

  private static IntIterator safeClone(IntIterator iterator)
  {
    // Calling clone() on empty iterators from RoaringBitmap library sometimes fails with NPE,
    // see https://github.com/apache/incubator-druid/issues/4709, https://github.com/RoaringBitmap/RoaringBitmap/issues/177
    return iterator.hasNext() ? iterator.clone() : EmptyIntIterator.instance();
  }
}
