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

package org.apache.druid.query.aggregation.histogram;

import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class ApproximateHistogramTest
{
  static final float[] VALUES = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};
  static final float[] VALUES2 = {23, 19, 10, 16, 36, 2, 1, 9, 32, 30, 45, 46};

  static final float[] VALUES3 = {
      20, 16, 19, 27, 17, 20, 18, 20, 28, 14, 17, 21, 20, 21, 10, 25, 23, 17, 21, 18,
      14, 20, 18, 12, 19, 20, 23, 25, 15, 22, 14, 17, 15, 23, 23, 15, 27, 20, 17, 15
  };
  static final float[] VALUES4 = {
      27.489f, 3.085f, 3.722f, 66.875f, 30.998f, -8.193f, 5.395f, 5.109f, 10.944f, 54.75f,
      14.092f, 15.604f, 52.856f, 66.034f, 22.004f, -14.682f, -50.985f, 2.872f, 61.013f,
      -21.766f, 19.172f, 62.882f, 33.537f, 21.081f, 67.115f, 44.789f, 64.1f, 20.911f,
      -6.553f, 2.178f
  };
  static final float[] VALUES5 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static final float[] VALUES6 = {
      1f, 1.5f, 2f, 2.5f, 3f, 3.5f, 4f, 4.5f, 5f, 5.5f, 6f, 6.5f, 7f, 7.5f, 8f, 8.5f, 9f, 9.5f, 10f
  };

  // Based on the example from https://metamarkets.com/2013/histograms/
  // This dataset can make getQuantiles() return values exceeding max
  // for example: q=0.95 returns 25.16 when max=25
  static final float[] VALUES7 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 12, 12, 15, 20, 25, 25, 25};

  protected ApproximateHistogram buildHistogram(int size, float[] values)
  {
    ApproximateHistogram h = new ApproximateHistogram(size);
    for (float v : values) {
      h.offer(v);
    }
    return h;
  }

  protected ApproximateHistogram buildHistogram(int size, float[] values, float lowerLimit, float upperLimit)
  {
    ApproximateHistogram h = new ApproximateHistogram(size, lowerLimit, upperLimit);
    for (float v : values) {
      h.offer(v);
    }
    return h;
  }

  @Test
  public void testOffer()
  {
    ApproximateHistogram h = buildHistogram(5, VALUES);

    // (2, 1), (9.5, 2), (19.33, 3), (32.67, 3), (45, 1)
    Assertions.assertArrayEquals(
        new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, h.positions(), 0.1f, "final bin positions match expected positions"
    );

    Assertions.assertArrayEquals(
        new long[]{1, 2, 3, 3, 1}, h.bins(), "final bin positions match expected positions"
    );

    Assertions.assertEquals(2, h.min(), 0, "min value matches expexted min");
    Assertions.assertEquals(45, h.max(), 0, "max value matches expexted max");

    Assertions.assertEquals(5, h.binCount(), "bin count matches expected bin count");
  }

  @Test
  public void testFold()
  {
    ApproximateHistogram merged = new ApproximateHistogram(0);
    ApproximateHistogram mergedFast = new ApproximateHistogram(0);
    ApproximateHistogram h1 = new ApproximateHistogram(5);
    ApproximateHistogram h2 = new ApproximateHistogram(10);

    for (int i = 0; i < 5; ++i) {
      h1.offer(VALUES[i]);
    }
    for (int i = 5; i < VALUES.length; ++i) {
      h2.offer(VALUES[i]);
    }

    merged.fold(h1, null, null, null);
    merged.fold(h2, null, null, null);
    mergedFast.foldFast(h1);
    mergedFast.foldFast(h2);

    Assertions.assertArrayEquals(
        new float[]{2, 9.5f, 19.33f, 32.67f, 45f}, merged.positions(), 0.1f, "final bin positions match expected positions"
    );
    Assertions.assertArrayEquals(
        new float[]{11.2f, 30.25f, 45f}, mergedFast.positions(), 0.1f, "final bin positions match expected positions"
    );

    Assertions.assertArrayEquals(
        new long[]{1, 2, 3, 3, 1}, merged.bins(), "final bin counts match expected counts"
    );
    Assertions.assertArrayEquals(
        new long[]{5, 4, 1}, mergedFast.bins(), "final bin counts match expected counts"
    );

    Assertions.assertEquals(45f, merged.max(), 0.1f, "merged max matches expected value");
    Assertions.assertEquals(45f, mergedFast.max(), 0.1f, "mergedfast max matches expected value");
    Assertions.assertEquals(2f, merged.min(), 0.1f, "merged min matches expected value");
    Assertions.assertEquals(2f, mergedFast.min(), 0.1f, "mergedfast min matches expected value");

    // fold where merged bincount is less than total bincount
    ApproximateHistogram a = buildHistogram(10, new float[]{1, 2, 3, 4, 5, 6});
    ApproximateHistogram aFast = buildHistogram(10, new float[]{1, 2, 3, 4, 5, 6});
    ApproximateHistogram b = buildHistogram(5, new float[]{3, 4, 5, 6});

    a.fold(b, null, null, null);
    aFast.foldFast(b);

    Assertions.assertEquals(
        new ApproximateHistogram(
            6,
            new float[]{1, 2, 3, 4, 5, 6, 0, 0, 0, 0},
            new long[]{1, 1, 2, 2, 2, 2, 0, 0, 0, 0},
            1, 6
        ), a
    );
    Assertions.assertEquals(
        new ApproximateHistogram(
            6,
            new float[]{1, 2, 3, 4, 5, 6, 0, 0, 0, 0},
            new long[]{1, 1, 2, 2, 2, 2, 0, 0, 0, 0},
            1, 6
        ), aFast
    );

    ApproximateHistogram h3 = new ApproximateHistogram(10);
    ApproximateHistogram h4 = new ApproximateHistogram(10);
    for (float v : VALUES3) {
      h3.offer(v);
    }
    for (float v : VALUES4) {
      h4.offer(v);
    }
    h3.fold(h4, null, null, null);
    Assertions.assertArrayEquals(
        new float[]{-50.98f, -21.77f, -9.81f, 3.73f, 13.72f, 20.1f, 29f, 44.79f, 53.8f, 64.67f},
        h3.positions(), 0.1f, "final bin positions match expected positions"
    );
    Assertions.assertArrayEquals(
        new long[]{1, 1, 3, 6, 12, 32, 6, 1, 2, 6}, h3.bins(), "final bin counts match expected counts"
    );

  }

  @Test
  public void testFoldNothing()
  {
    ApproximateHistogram h1 = new ApproximateHistogram(10);
    ApproximateHistogram h2 = new ApproximateHistogram(10);

    h1.fold(h2, null, null, null);
    h1.foldFast(h2);
  }

  @Test
  public void testFoldNothing2()
  {
    ApproximateHistogram h1 = new ApproximateHistogram(10);
    ApproximateHistogram h1Fast = new ApproximateHistogram(10);
    ApproximateHistogram h2 = new ApproximateHistogram(10);
    ApproximateHistogram h3 = new ApproximateHistogram(10);
    ApproximateHistogram h4 = new ApproximateHistogram(10);
    ApproximateHistogram h4Fast = new ApproximateHistogram(10);
    for (float v : VALUES3) {
      h3.offer(v);
      h4.offer(v);
      h4Fast.offer(v);
    }

    h1.fold(h3, null, null, null);
    h4.fold(h2, null, null, null);
    h1Fast.foldFast(h3);
    h4Fast.foldFast(h2);

    Assertions.assertEquals(h3, h1);
    Assertions.assertEquals(h4, h3);
    Assertions.assertEquals(h3, h1Fast);
    Assertions.assertEquals(h3, h4Fast);
  }

    //@Test
  @Disabled
  @SuppressWarnings("unused") //TODO rewrite using JMH and move to the benchmarks module
  public void testFoldSpeed()
  {
    final int combinedHistSize = 200;
    final int histSize = 50;
    final int numRand = 10000;
    ApproximateHistogram h = new ApproximateHistogram(combinedHistSize);
    Random rand = new Random(0);
    //for (int i = 0; i < 200; ++i) h.offer((float)(rand.nextGaussian() * 50.0));
    long tFold = 0;
    int count = 5000000;
    // May be a bug that randNums are not used, should be resolved if testFoldSpeed() becomes a jUnit test again
    @SuppressWarnings("MismatchedReadAndWriteOfArray")
    Float[] randNums = new Float[numRand];
    for (int i = 0; i < numRand; i++) {
      randNums[i] = (float) rand.nextGaussian();
    }

    List<ApproximateHistogram> randHist = new ArrayList<>();
    Iterator<ApproximateHistogram> it = Iterators.cycle(randHist);

    for (int k = 0; k < numRand; ++k) {
      ApproximateHistogram tmp = new ApproximateHistogram(histSize);
      for (int i = 0; i < 20; ++i) {
        tmp.offer((float) (rand.nextGaussian() + (double) k));
      }
      randHist.add(tmp);
    }

    float[] mergeBufferP = new float[combinedHistSize * 2];
    long[] mergeBufferB = new long[combinedHistSize * 2];
    @SuppressWarnings("unused") // make two different benchmarks - for fold, and foldFast (see commented line below)
    float[] mergeBufferD = new float[combinedHistSize * 2];

    for (int i = 0; i < count; ++i) {
      ApproximateHistogram tmp = it.next();

      long t0 = System.nanoTime();
      //h.fold(tmp, mergeBufferP, mergeBufferB, mergeBufferD);
      h.foldFast(tmp, mergeBufferP, mergeBufferB);
      tFold += System.nanoTime() - t0;
    }

    System.out.println(StringUtils.format("Average folds per second : %f", (double) count / (double) tFold * 1e9));
  }

  @Test
  public void testSum()
  {
    ApproximateHistogram h = buildHistogram(5, VALUES);

    Assertions.assertEquals(0.0f, h.sum(0), 0.01);
    Assertions.assertEquals(1.0f, h.sum(2), 0.01);
    Assertions.assertEquals(1.16f, h.sum(5), 0.01);
    Assertions.assertEquals(3.28f, h.sum(15), 0.01);
    Assertions.assertEquals(VALUES.length, h.sum(45), 0.01);
    Assertions.assertEquals(VALUES.length, h.sum(46), 0.01);

    ApproximateHistogram h2 = buildHistogram(5, VALUES2);

    Assertions.assertEquals(0.0f, h2.sum(0), 0.01);
    Assertions.assertEquals(0.0f, h2.sum(1f), 0.01);
    Assertions.assertEquals(1.0f, h2.sum(1.5f), 0.01);
    Assertions.assertEquals(1.125f, h2.sum(2f), 0.001);
    Assertions.assertEquals(2.0625f, h2.sum(5.75f), 0.001);
    Assertions.assertEquals(3.0f, h2.sum(9.5f), 0.01);
    Assertions.assertEquals(11.0f, h2.sum(45.5f), 0.01);
    Assertions.assertEquals(12.0f, h2.sum(46f), 0.01);
    Assertions.assertEquals(12.0f, h2.sum(47f), 0.01);
  }

  @Test
  public void testSerializeCompact()
  {
    ApproximateHistogram h = buildHistogram(5, VALUES);
    Assertions.assertEquals(h, ApproximateHistogram.fromBytes(h.toBytes()));

    ApproximateHistogram h2 = new ApproximateHistogram(50).fold(h, null, null, null);
    Assertions.assertEquals(h2, ApproximateHistogram.fromBytes(h2.toBytes()));
  }

  @Test
  public void testSerializeDense()
  {
    ApproximateHistogram h = buildHistogram(5, VALUES);
    ByteBuffer buf = ByteBuffer.allocate(h.getDenseStorageSize());
    h.toBytesDense(buf);
    Assertions.assertEquals(h, ApproximateHistogram.fromBytes(buf.array()));
  }

  @Test
  public void testSerializeSparse()
  {
    ApproximateHistogram h = buildHistogram(5, VALUES);
    ByteBuffer buf = ByteBuffer.allocate(h.getSparseStorageSize());
    h.toBytesSparse(buf);
    Assertions.assertEquals(h, ApproximateHistogram.fromBytes(buf.array()));
  }

  @Test
  public void testSerializeCompactExact()
  {
    ApproximateHistogram h = buildHistogram(50, new float[]{1f, 2f, 3f, 4f, 5f});
    Assertions.assertEquals(h, ApproximateHistogram.fromBytes(h.toBytes()));

    h = buildHistogram(5, new float[]{1f, 2f, 3f});
    Assertions.assertEquals(h, ApproximateHistogram.fromBytes(h.toBytes()));

    h = new ApproximateHistogram(40).fold(h, null, null, null);
    Assertions.assertEquals(h, ApproximateHistogram.fromBytes(h.toBytes()));
  }

  @Test
  public void testSerializeEmpty()
  {
    ApproximateHistogram h = new ApproximateHistogram(50);
    Assertions.assertEquals(h, ApproximateHistogram.fromBytes(h.toBytes()));
  }

  @Test
  public void testQuantileSmaller()
  {
    ApproximateHistogram h = buildHistogram(20, VALUES5);
    Assertions.assertArrayEquals(
        new float[]{5f},
        h.getQuantiles(new float[]{.5f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{3.33f, 6.67f},
        h.getQuantiles(new float[]{.333f, .666f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{2.5f, 5f, 7.5f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{2f, 4f, 6f, 8f},
        h.getQuantiles(new float[]{.2f, .4f, .6f, .8f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}), 0.1f, "expected quantiles match actual quantiles"
    );
  }

  @Test
  public void testQuantileEqualSize()
  {
    ApproximateHistogram h = buildHistogram(10, VALUES5);
    Assertions.assertArrayEquals(
        new float[]{5f},
        h.getQuantiles(new float[]{.5f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{3.33f, 6.67f},
        h.getQuantiles(new float[]{.333f, .666f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{2.5f, 5f, 7.5f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{2f, 4f, 6f, 8f},
        h.getQuantiles(new float[]{.2f, .4f, .6f, .8f}), 0.1f, "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f, 9f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}), 0.1f, "expected quantiles match actual quantiles"
    );
  }

  @Test
  public void testQuantileBetweenMinMax()
  {
    ApproximateHistogram h = buildHistogram(20, VALUES7);

    Assertions.assertTrue(
        VALUES7[0] == h.min(),
        "min value incorrect"
    );
    Assertions.assertTrue(
        VALUES7[VALUES7.length - 1] == h.max(),
        "max value incorrect"
    );

    Assertions.assertArrayEquals(
        new float[]{1.8f, 3.6f, 5.4f, 7.2f, 9f, 11.05f, 12.37f, 17f, 23.5f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );

    // Test for outliers (0.05f and 0.95f, which should be min <= value <= max)
    Assertions.assertArrayEquals(
        new float[]{h.min(), h.max()},
        h.getQuantiles(new float[]{.05f, .95f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
  }

  @Test
  public void testQuantileBigger()
  {
    ApproximateHistogram h = buildHistogram(5, VALUES5);
    Assertions.assertArrayEquals(
        new float[]{4.5f},
        h.getQuantiles(new float[]{.5f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{2.83f, 6.17f},
        h.getQuantiles(new float[]{.333f, .666f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{2f, 4.5f, 7f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{1.5f, 3.5f, 5.5f, 7.5f},
        h.getQuantiles(new float[]{.2f, .4f, .6f, .8f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{1f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f, 8.5f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
  }

  @Test
  public void testQuantileBigger2()
  {
    float[] thousand = new float[1000];
    for (int i = 1; i <= 1000; ++i) {
      thousand[i - 1] = i;
    }
    ApproximateHistogram h = buildHistogram(100, thousand);

    Assertions.assertArrayEquals(
        new float[]{493.5f},
        h.getQuantiles(new float[]{.5f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{327.5f, 662f},
        h.getQuantiles(new float[]{.333f, .666f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{244.5f, 493.5f, 746f},
        h.getQuantiles(new float[]{.25f, .5f, .75f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
    Assertions.assertArrayEquals(
        new float[]{96.5f, 196.53f, 294.5f, 395.5f, 493.5f, 597f, 696f, 795f, 895.25f},
        h.getQuantiles(new float[]{.1f, .2f, .3f, .4f, .5f, .6f, .7f, .8f, .9f}),
        0.1f,
        "expected quantiles match actual quantiles"
    );
  }

  @Test
  public void testLimitSum()
  {
    final float lowerLimit = 0f;
    final float upperLimit = 10f;

    ApproximateHistogram h = buildHistogram(15, VALUES6, lowerLimit, upperLimit);

    for (int i = 1; i <= 20; ++i) {
      ApproximateHistogram hLow = new ApproximateHistogram(5);
      ApproximateHistogram hHigh = new ApproximateHistogram(5);
      hLow.offer(lowerLimit - i);
      hHigh.offer(upperLimit + i);
      h.foldFast(hLow);
      h.foldFast(hHigh);
    }

    Assertions.assertEquals(20f, h.sum(lowerLimit), .7f);
    Assertions.assertEquals(VALUES6.length + 20f, h.sum(upperLimit), 0.01);
  }

  @Test
  public void testBuckets()
  {
    final float[] values = new float[]{-5f, .01f, .02f, .06f, .12f, 1f, 2f};
    ApproximateHistogram h = buildHistogram(50, values, 0f, 1f);
    Histogram h2 = h.toHistogram(.05f, 0f);

    Assertions.assertArrayEquals(
        new double[]{1f, 2f, 1f, 1f, 0f, 1f, 1f},
        h2.getCounts(), 0.1f, "expected counts match actual counts"
    );

    Assertions.assertArrayEquals(
        new double[]{-5.05f, 0f, .05f, .1f, .15f, .95f, 1f, 2f},
        h2.getBreaks(), 0.1f, "expected breaks match actual breaks"
    );
  }

  @Test
  public void testBuckets2()
  {
    final float[] values = new float[]{-5f, .01f, .02f, .06f, .12f, .94f, 1f, 2f};
    ApproximateHistogram h = buildHistogram(50, values, 0f, 1f);
    Histogram h2 = h.toHistogram(.05f, 0f);

    Assertions.assertArrayEquals(
        new double[]{1f, 2f, 1f, 1f, 0f, 1f, 1f, 1f},
        h2.getCounts(),
        0.1f,
        "expected counts match actual counts"
    );

    Assertions.assertArrayEquals(
        new double[]{-5.05f, 0f, .05f, .1f, .15f, .9f, .95f, 1f, 2.05f},
        h2.getBreaks(),
        0.1f,
        "expected breaks match actual breaks"
    );
  }

  @Test
  public void testBuckets3()
  {
    final float[] values = new float[]{0f, 0f, .02f, .06f, .12f, .94f};
    ApproximateHistogram h = buildHistogram(50, values, 0f, 1f);
    Histogram h2 = h.toHistogram(1f, 0f);

    Assertions.assertArrayEquals(
        new double[]{2f, 4f},
        h2.getCounts(),
        0.1f,
        "expected counts match actual counts"
    );

    Assertions.assertArrayEquals(
        new double[]{-1f, 0f, 1f},
        h2.getBreaks(),
        0.1f,
        "expected breaks match actual breaks"
    );
  }

  @Test
  public void testBuckets4()
  {
    final float[] values = new float[]{0f, 0f, 0.01f, 0.51f, 0.6f, 0.8f};
    ApproximateHistogram h = buildHistogram(50, values, 0.5f, 1f);
    Histogram h3 = h.toHistogram(0.2f, 0);

    Assertions.assertArrayEquals(
        new double[]{3f, 2f, 1f},
        h3.getCounts(),
        0.1f,
        "Expected counts match actual counts"
    );

    Assertions.assertArrayEquals(
        new double[]{-0.2f, 0.5f, 0.7f, 0.9f},
        h3.getBreaks(),
        0.1f,
        "expected breaks match actual breaks"
    );
  }

  @Test public void testBuckets5()
  {
    final float[] values = new float[]{0.1f, 0.5f, 0.6f};
    ApproximateHistogram h = buildHistogram(50, values, 0f, 1f);
    Histogram h4 = h.toHistogram(0.5f, 0);

    Assertions.assertArrayEquals(
        new double[]{2, 1},
        h4.getCounts(),
        0.1f,
        "Expected counts match actual counts"
    );

    Assertions.assertArrayEquals(
        new double[]{0f, 0.5f, 1f},
        h4.getBreaks(),
        0.1f,
        "Expected breaks match actual breaks"
    );
  }

  @Test
  public void testEmptyHistogram()
  {
    ApproximateHistogram h = new ApproximateHistogram(50);
    Assertions.assertArrayEquals(
        new float[]{Float.NaN, Float.NaN},
        h.getQuantiles(new float[]{0.8f, 0.9f}),
        1e-9f
    );
  }


}
