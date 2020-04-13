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

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class FixedBucketsHistogramTest
{
  private static final Logger log = new Logger(FixedBucketsHistogramTest.class);

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

  static final float[] VALUES7 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 12, 12, 15, 20, 25, 25, 25};

  protected FixedBucketsHistogram buildHistogram(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode,
      float[] values
  )
  {
    FixedBucketsHistogram h = new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );

    for (float v : values) {
      h.add(v);
    }
    return h;
  }

  @Test
  public void testOffer()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES2
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{2.5f, 20.0f, 46.76f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testOfferRandoms()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        1000,
        50,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{}
    );

    Random rng = new Random(1000);
    double[] values = new double[100000];
    for (int i = 0; i < 100000; i++) {
      values[i] = (double) rng.nextInt(1000);
      h.add(values[i]);
    }

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 25.0f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{125.04082f, 248.84348f, 501.67166f, 979.7799f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testNormalDistribution()
  {
    NormalDistribution normalDistribution = new NormalDistribution(
        new JDKRandomGenerator(1000),
        50000,
        10000
    );

    FixedBucketsHistogram h = new FixedBucketsHistogram(
        0,
        100000,
        1000,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    for (int i = 0; i < 100000; i++) {
      double val = normalDistribution.sample();
      h.add(val);
    }
    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 25.0f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{38565.324f, 43297.95f, 50091.902f, 70509.125f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testOfferWithNegatives()
  {
    FixedBucketsHistogram h = buildHistogram(
        -100,
        100,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES2
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{3.0f, 20.0f, 47.52f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testOfferValues3()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES3
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{14.857142f, 20.0f, 28.4f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testOfferValues4()
  {
    FixedBucketsHistogram h = buildHistogram(
        -100,
        100,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES4
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{-8.5f, 20.0f, 67.6f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testOfferValues5()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        10,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES5
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{2.125f, 5.5f, 9.82f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testOfferValues6()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        10,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES6
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{2.125f, 5.5f, 9.82f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testOfferValues7()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        50,
        50,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES7
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});

    Assert.assertArrayEquals(
        new float[]{3.25f, 10f, 25.88f},
        quantiles,
        0.01f
    );
  }

  @Test
  public void testMergeSameBuckets()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19, 25, -5}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{1, 2, 7, 12, 19, 25, -5}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{1, 2, 7, 12, 19, 25, -5}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{3, 8, 9, 13, -99, 200}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{3, 1, 2, 2, 1}, h.getHistogram());
    Assert.assertEquals(9, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(2, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{5, 1, 2, 2, 3}, hClip.getHistogram());
    Assert.assertEquals(13, hClip.getCount());
    Assert.assertEquals(0, hClip.getMin(), 0.01);
    Assert.assertEquals(20, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{3, 1, 2, 2, 1}, hIgnore.getHistogram());
    Assert.assertEquals(9, hIgnore.getCount());
    Assert.assertEquals(1, hIgnore.getMin(), 0.01);
    Assert.assertEquals(19, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeNoOverlapRight()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        50,
        100,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{60, 70, 80, 90}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(4, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 1, 5}, hClip.getHistogram());
    Assert.assertEquals(9, hClip.getCount());
    Assert.assertEquals(1, hClip.getMin(), 0.01);
    Assert.assertEquals(20, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 1, 1}, hIgnore.getHistogram());
    Assert.assertEquals(5, hIgnore.getCount());
    Assert.assertEquals(1, hIgnore.getMin(), 0.01);
    Assert.assertEquals(19, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeNoOverlapLeft()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        -100,
        -50,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{-60, -70, -80, -90}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(4, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{6, 1, 0, 1, 1}, hClip.getHistogram());
    Assert.assertEquals(9, hClip.getCount());
    Assert.assertEquals(0, hClip.getMin(), 0.01);
    Assert.assertEquals(19, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 1, 1}, hIgnore.getHistogram());
    Assert.assertEquals(5, hIgnore.getCount());
    Assert.assertEquals(1, hIgnore.getMin(), 0.01);
    Assert.assertEquals(19, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }


  @Test
  public void testMergeSameBucketsRightOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 2, 2}, h.getHistogram());
    Assert.assertEquals(7, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 2, 4}, hClip.getHistogram());
    Assert.assertEquals(9, hClip.getCount());
    Assert.assertEquals(1, hClip.getMin(), 0.01);
    Assert.assertEquals(20, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 2, 2}, hIgnore.getHistogram());
    Assert.assertEquals(7, hIgnore.getCount());
    Assert.assertEquals(1, hIgnore.getMin(), 0.01);
    Assert.assertEquals(19, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeSameBucketsLeftOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 13, 19}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(12, h.getLowerLimit(), 0.01);
    Assert.assertEquals(32, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 2, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(6, h.getCount());
    Assert.assertEquals(12, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(3, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(12, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(32, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{5, 2, 0, 1, 1}, hClip.getHistogram());
    Assert.assertEquals(9, hClip.getCount());
    Assert.assertEquals(12, hClip.getMin(), 0.01);
    Assert.assertEquals(29, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(12, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(32, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 2, 0, 1, 1}, hIgnore.getHistogram());
    Assert.assertEquals(6, hIgnore.getCount());
    Assert.assertEquals(12, hIgnore.getMin(), 0.01);
    Assert.assertEquals(29, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeSameBucketsContainsOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{2, 18, 34, 48}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{2, 18, 34, 48}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 29, 99, -100}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(10, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(50, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0, 1, 2, 1, 1, 1, 0, 0, 1}, h.getHistogram());
    Assert.assertEquals(8, h.getCount());
    Assert.assertEquals(2, h.getMin(), 0.01);
    Assert.assertEquals(48, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(1, h.getLowerOutlierCount());
    Assert.assertEquals(1, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(10, hClip.getNumBuckets());
    Assert.assertEquals(5.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(50, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 0, 1, 2, 1, 1, 1, 0, 0, 2}, hClip.getHistogram());
    Assert.assertEquals(10, hClip.getCount());
    Assert.assertEquals(0, hClip.getMin(), 0.01);
    Assert.assertEquals(50, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(10, hIgnore.getNumBuckets());
    Assert.assertEquals(5.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(50, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0, 1, 2, 1, 1, 1, 0, 0, 1}, hIgnore.getHistogram());
    Assert.assertEquals(8, hIgnore.getCount());
    Assert.assertEquals(2, hIgnore.getMin(), 0.01);
    Assert.assertEquals(48, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeSameBucketsContainedByOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48, 99, -100}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(4, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(10, h.getLowerLimit(), 0.01);
    Assert.assertEquals(30, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 2, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(11, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(2, h.getLowerOutlierCount());
    Assert.assertEquals(3, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(4, hClip.getNumBuckets());
    Assert.assertEquals(5.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(10, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(30, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{3, 2, 1, 4}, hClip.getHistogram());
    Assert.assertEquals(10, hClip.getCount());
    Assert.assertEquals(10, hClip.getMin(), 0.01);
    Assert.assertEquals(30, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(4, hIgnore.getNumBuckets());
    Assert.assertEquals(5.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(10, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(30, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 2, 1, 1}, hIgnore.getHistogram());
    Assert.assertEquals(5, hIgnore.getCount());
    Assert.assertEquals(11, hIgnore.getMin(), 0.01);
    Assert.assertEquals(29, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBuckets2()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        6,
        6,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        12,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 1, 1, 1, 5, 5, 5, 5}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(6, h.getNumBuckets());
    Assert.assertEquals(1.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(6, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 1, 1, 1, 1, 1}, h.getHistogram());
    Assert.assertEquals(6, h.getCount());
    Assert.assertEquals(0, h.getMin(), 0.01);
    Assert.assertEquals(5.25, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(0, h.getUpperOutlierCount());
  }


  @Test
  public void testMergeDifferentBuckets()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 18}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{1, 2, 7, 12, 18}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{1, 2, 7, 12, 18}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        7,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{3, 8, 9, 19, 99, -50}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 3, 1, 1, 2}, h.getHistogram());
    Assert.assertEquals(9, h.getCount());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(18, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(1, h.getLowerOutlierCount());
    Assert.assertEquals(1, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{3, 3, 1, 1, 3}, hClip.getHistogram());
    Assert.assertEquals(11, hClip.getCount());
    Assert.assertEquals(0, hClip.getMin(), 0.01);
    Assert.assertEquals(20, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 3, 1, 1, 2}, hIgnore.getHistogram());
    Assert.assertEquals(9, hIgnore.getCount());
    Assert.assertEquals(1, hIgnore.getMin(), 0.01);
    Assert.assertEquals(18, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsRightOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        12,
        32,
        7,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29, -10}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(20, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertEquals(7, h.getCount());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 2, 2}, h.getHistogram());
    Assert.assertEquals(1, h.getMin(), 0.01);
    Assert.assertEquals(19, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(1, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertEquals(10, hClip.getCount());
    Assert.assertArrayEquals(new long[]{3, 1, 0, 2, 4}, hClip.getHistogram());
    Assert.assertEquals(0, hClip.getMin(), 0.01);
    Assert.assertEquals(20, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(20, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertEquals(7, hIgnore.getCount());
    Assert.assertArrayEquals(new long[]{2, 1, 0, 2, 2}, hIgnore.getHistogram());
    Assert.assertEquals(1, hIgnore.getMin(), 0.01);
    Assert.assertEquals(19, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsLeftOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{13, 18, 25, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        9,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19, -99, 100}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getNumBuckets());
    Assert.assertEquals(4.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(12, h.getLowerLimit(), 0.01);
    Assert.assertEquals(32, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 2, 0, 1, 1}, h.getHistogram());
    Assert.assertEquals(6, h.getCount());
    Assert.assertEquals(13, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(4, h.getLowerOutlierCount());
    Assert.assertEquals(1, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(5, hClip.getNumBuckets());
    Assert.assertEquals(4.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(12, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(32, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{6, 2, 0, 1, 2}, hClip.getHistogram());
    Assert.assertEquals(11, hClip.getCount());
    Assert.assertEquals(12, hClip.getMin(), 0.01);
    Assert.assertEquals(32, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(5, hIgnore.getNumBuckets());
    Assert.assertEquals(4.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(12, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(32, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 2, 0, 1, 1}, hIgnore.getHistogram());
    Assert.assertEquals(6, hIgnore.getCount());
    Assert.assertEquals(13, hIgnore.getMin(), 0.01);
    Assert.assertEquals(29, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsContainsOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{2, 18, 34, 48}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        0,
        50,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{2, 18, 34, 48}
    );


    FixedBucketsHistogram h2 = buildHistogram(
        10,
        30,
        7,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 21, 29, -99, 100}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(10, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(0, h.getLowerLimit(), 0.01);
    Assert.assertEquals(50, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0, 2, 2, 1, 1, 1, 0, 0, 1}, h.getHistogram());
    Assert.assertEquals(9, h.getCount());
    Assert.assertEquals(2, h.getMin(), 0.01);
    Assert.assertEquals(48, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(1, h.getLowerOutlierCount());
    Assert.assertEquals(1, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(10, hClip.getNumBuckets());
    Assert.assertEquals(5.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(0, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(50, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 0, 2, 2, 1, 1, 1, 0, 0, 2}, hClip.getHistogram());
    Assert.assertEquals(11, hClip.getCount());
    Assert.assertEquals(0, hClip.getMin(), 0.01);
    Assert.assertEquals(50, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(10, hIgnore.getNumBuckets());
    Assert.assertEquals(5.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(0, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(50, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 0, 2, 2, 1, 1, 1, 0, 0, 1}, hIgnore.getHistogram());
    Assert.assertEquals(9, hIgnore.getCount());
    Assert.assertEquals(2, hIgnore.getMin(), 0.01);
    Assert.assertEquals(48, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMergeDifferentBucketsContainedByOther()
  {
    FixedBucketsHistogram h = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram hClip = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram hIgnore = buildHistogram(
        10,
        30,
        4,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        new float[]{11, 15, 21, 29}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        50,
        13,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{2, 18, 34, 48}
    );

    h.combineHistogram(h2);
    Assert.assertEquals(4, h.getNumBuckets());
    Assert.assertEquals(5.0, h.getBucketSize(), 0.01);
    Assert.assertEquals(10, h.getLowerLimit(), 0.01);
    Assert.assertEquals(30, h.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW, h.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 2, 1, 1}, h.getHistogram());
    Assert.assertEquals(5, h.getCount());
    Assert.assertEquals(11, h.getMin(), 0.01);
    Assert.assertEquals(29, h.getMax(), 0.01);
    Assert.assertEquals(0, h.getMissingValueCount());
    Assert.assertEquals(1, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getUpperOutlierCount());

    hClip.combineHistogram(h2);
    Assert.assertEquals(4, hClip.getNumBuckets());
    Assert.assertEquals(5.0, hClip.getBucketSize(), 0.01);
    Assert.assertEquals(10, hClip.getLowerLimit(), 0.01);
    Assert.assertEquals(30, hClip.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.CLIP, hClip.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{2, 2, 1, 3}, hClip.getHistogram());
    Assert.assertEquals(8, hClip.getCount());
    Assert.assertEquals(10, hClip.getMin(), 0.01);
    Assert.assertEquals(30, hClip.getMax(), 0.01);
    Assert.assertEquals(0, hClip.getMissingValueCount());
    Assert.assertEquals(0, hClip.getLowerOutlierCount());
    Assert.assertEquals(0, hClip.getUpperOutlierCount());

    hIgnore.combineHistogram(h2);
    Assert.assertEquals(4, hIgnore.getNumBuckets());
    Assert.assertEquals(5.0, hIgnore.getBucketSize(), 0.01);
    Assert.assertEquals(10, hIgnore.getLowerLimit(), 0.01);
    Assert.assertEquals(30, hIgnore.getUpperLimit(), 0.01);
    Assert.assertEquals(FixedBucketsHistogram.OutlierHandlingMode.IGNORE, hIgnore.getOutlierHandlingMode());
    Assert.assertArrayEquals(new long[]{1, 2, 1, 1}, hIgnore.getHistogram());
    Assert.assertEquals(5, hIgnore.getCount());
    Assert.assertEquals(11, hIgnore.getMin(), 0.01);
    Assert.assertEquals(29, hIgnore.getMax(), 0.01);
    Assert.assertEquals(0, hIgnore.getMissingValueCount());
    Assert.assertEquals(0, hIgnore.getLowerOutlierCount());
    Assert.assertEquals(0, hIgnore.getUpperOutlierCount());
  }

  @Test
  public void testMissing()
  {
    FixedBucketsHistogram h = new FixedBucketsHistogram(
        0,
        200,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE
    );

    h.incrementMissing();
    h.incrementMissing();
    Assert.assertEquals(2, h.getMissingValueCount());

    FixedBucketsHistogram h2 = new FixedBucketsHistogram(
        0,
        200,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE
    );

    h2.incrementMissing();
    h2.incrementMissing();
    h2.incrementMissing();
    h.combineHistogram(h2);
    Assert.assertEquals(5, h.getMissingValueCount());
  }


  @Test
  public void testOutlierIgnore()
  {
    FixedBucketsHistogram h = new FixedBucketsHistogram(
        0,
        200,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE
    );

    h.add(900);
    h.add(300);
    h.add(-275);
    h.add(500);
    h.add(-1000);
    h.add(10);
    h.add(199);

    Assert.assertEquals(0, h.getUpperOutlierCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getCount());
    Assert.assertEquals(10, h.getMin(), 0.01);
    Assert.assertEquals(199, h.getMax(), 0.01);
  }

  @Test
  public void testOutlierOverflow()
  {
    FixedBucketsHistogram h = new FixedBucketsHistogram(
        0,
        200,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    h.add(900);
    h.add(300);
    h.add(-275);
    h.add(500);
    h.add(-1000);
    h.add(10);
    h.add(199);

    Assert.assertEquals(3, h.getUpperOutlierCount());
    Assert.assertEquals(2, h.getLowerOutlierCount());
    Assert.assertEquals(2, h.getCount());
    Assert.assertEquals(10, h.getMin(), 0.01);
    Assert.assertEquals(199, h.getMax(), 0.01);
  }

  @Test
  public void testOutlierClip()
  {
    FixedBucketsHistogram h = new FixedBucketsHistogram(
        0,
        200,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.CLIP
    );

    h.add(900);
    h.add(300);
    h.add(-275);
    h.add(500);
    h.add(-1000);
    h.add(10);
    h.add(199);

    Assert.assertEquals(0, h.getUpperOutlierCount());
    Assert.assertEquals(0, h.getLowerOutlierCount());
    Assert.assertEquals(7, h.getCount());
    Assert.assertEquals(0, h.getMin(), 0.01);
    Assert.assertEquals(200, h.getMax(), 0.01);
  }

  @Test
  public void testSerdeFullHistogram()
  {
    FixedBucketsHistogram hFull = new FixedBucketsHistogram(
        -100,
        100,
        200,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    for (int i = -100; i < 100; i++) {
      hFull.add(i);
    }

    hFull.incrementMissing();

    byte[] fullWithHeader = hFull.toBytesFull(true);
    byte[] sparse = hFull.toBytesSparse(hFull.getNonEmptyBucketCount());
    String asBase64Full = hFull.toBase64();

    byte[] asBytesAuto = hFull.toBytes();

    Assert.assertArrayEquals(fullWithHeader, asBytesAuto);

    FixedBucketsHistogram fromFullWithHeader = FixedBucketsHistogram.fromBytes(fullWithHeader);
    Assert.assertEquals(hFull, fromFullWithHeader);

    FixedBucketsHistogram fromSparse = FixedBucketsHistogram.fromBytes(sparse);
    Assert.assertEquals(hFull, fromSparse);

    FixedBucketsHistogram fromBase64 = FixedBucketsHistogram.fromBase64(asBase64Full);
    Assert.assertEquals(hFull, fromBase64);
  }

  @Test
  public void testSerdeSparseHistogram()
  {
    FixedBucketsHistogram hSparse = buildHistogram(
        -10,
        200,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES3
    );

    hSparse.add(300);
    hSparse.add(400);
    hSparse.add(500);
    hSparse.add(-300);
    hSparse.add(-700);
    hSparse.incrementMissing();

    byte[] fullWithHeader = hSparse.toBytesFull(true);
    byte[] sparse = hSparse.toBytesSparse(hSparse.getNonEmptyBucketCount());
    String asBase64Full = hSparse.toBase64();

    byte[] asBytesAuto = hSparse.toBytes();

    Assert.assertArrayEquals(sparse, asBytesAuto);

    FixedBucketsHistogram fromFullWithHeader = FixedBucketsHistogram.fromBytes(fullWithHeader);
    Assert.assertEquals(hSparse, fromFullWithHeader);

    FixedBucketsHistogram fromSparse = FixedBucketsHistogram.fromBytes(sparse);
    Assert.assertEquals(hSparse, fromSparse);

    FixedBucketsHistogram fromBase64 = FixedBucketsHistogram.fromBase64(asBase64Full);
    Assert.assertEquals(hSparse, fromBase64);
  }
}
