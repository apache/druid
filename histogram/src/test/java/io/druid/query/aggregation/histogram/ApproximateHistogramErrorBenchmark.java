/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.histogram;

import com.google.common.primitives.Floats;
import io.druid.query.aggregation.Histogram;

import java.util.Arrays;
import java.util.Random;

public class ApproximateHistogramErrorBenchmark
{
  private boolean debug = true;
  private int numBuckets = 20;
  private int numBreaks = numBuckets + 1;
  private int numPerHist = 50;
  private int numHists = 10;
  private int resolution = 50;
  private int combinedResolution = 100;
  private Random rand = new Random(2);

  public ApproximateHistogramErrorBenchmark setDebug(boolean debug)
  {
    this.debug = debug;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumBuckets(int numBuckets)
  {
    this.numBuckets = numBuckets;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumBreaks(int numBreaks)
  {
    this.numBreaks = numBreaks;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumPerHist(int numPerHist)
  {
    this.numPerHist = numPerHist;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setNumHists(int numHists)
  {
    this.numHists = numHists;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setResolution(int resolution)
  {
    this.resolution = resolution;
    return this;
  }

  public ApproximateHistogramErrorBenchmark setCombinedResolution(int combinedResolution)
  {
    this.combinedResolution = combinedResolution;
    return this;
  }


  public static void main(String[] args)
  {
    ApproximateHistogramErrorBenchmark approxHist = new ApproximateHistogramErrorBenchmark();
    System.out.println(
        Arrays.toString(
            approxHist.setDebug(true)
                      .setNumPerHist(50)
                      .setNumHists(10000)
                      .setResolution(50)
                      .setCombinedResolution(100)
                      .getErrors()
        )
    );


    ApproximateHistogramErrorBenchmark approxHist2 = new ApproximateHistogramErrorBenchmark();
    int[] numHistsArray = new int[]{10, 100, 1000, 10000, 100000};
    float[] errs1 = new float[numHistsArray.length];
    float[] errs2 = new float[numHistsArray.length];
    for (int i = 0; i < numHistsArray.length; ++i) {
      float[] tmp = approxHist2.setDebug(false).setNumHists(numHistsArray[i]).setCombinedResolution(100).getErrors();
      errs1[i] = tmp[0];
      errs2[i] = tmp[1];
    }

    System.out
          .format("Number of histograms for folding                           : %s \n", Arrays.toString(numHistsArray));
    System.out.format("Errors for approximate histogram                           : %s \n", Arrays.toString(errs1));
    System.out.format("Errors for approximate histogram, ruleFold                 : %s \n", Arrays.toString(errs2));
  }

  private float[] getErrors()
  {
    final int numValues = numHists * numPerHist;
    final float[] values = new float[numValues];

    for (int i = 0; i < numValues; ++i) {
      values[i] = (float) rand.nextGaussian();
    }

    float min = Floats.min(values);
    min = (float) (min < 0 ? 1.02 : .98) * min;
    float max = Floats.max(values);
    max = (float) (max < 0 ? .98 : 1.02) * max;
    final float stride = (max - min) / numBuckets;
    final float[] breaks = new float[numBreaks];
    for (int i = 0; i < numBreaks; i++) {
      breaks[i] = min + stride * i;
    }

    Histogram h = new Histogram(breaks);
    for (float v : values) {
      h.offer(v);
    }
    double[] hcounts = h.asVisual().counts;

    ApproximateHistogram ah1 = new ApproximateHistogram(resolution);
    ApproximateHistogram ah2 = new ApproximateHistogram(combinedResolution);
    ApproximateHistogram tmp = new ApproximateHistogram(resolution);
    for (int i = 0; i < numValues; ++i) {
      tmp.offer(values[i]);
      if ((i + 1) % numPerHist == 0) {
        ah1.fold(tmp);
        ah2.foldRule(tmp, null, null);
        tmp = new ApproximateHistogram(resolution);
      }
    }
    double[] ahcounts1 = ah1.toHistogram(breaks).getCounts();
    double[] ahcounts2 = ah2.toHistogram(breaks).getCounts();

    float err1 = 0;
    float err2 = 0;
    for (int j = 0; j < hcounts.length; j++) {
      err1 += Math.abs((hcounts[j] - ahcounts1[j]) / numValues);
      err2 += Math.abs((hcounts[j] - ahcounts2[j]) / numValues);
    }

    if (debug) {
      float sum = 0;
      for (double v : hcounts) {
        sum += v;
      }
      System.out.println("Exact Histogram Sum:");
      System.out.println(sum);
      sum = 0;
      for (double v : ahcounts1) {
        sum += v;
      }
      System.out.println("Approximate Histogram Sum:");
      System.out.println(sum);
      sum = 0;
      for (double v : ahcounts2) {
        sum += v;
      }
      System.out.println("Approximate Histogram Rule Fold Sum:");
      System.out.println(sum);
      System.out.println("Exact Histogram:");
      System.out.println(h.asVisual());
      System.out.println("Approximate Histogram:");
      System.out.println(ah1.toHistogram(breaks));
      System.out.println("Approximate Histogram Rule Fold:");
      System.out.println(ah2.toHistogram(breaks));
      System.out.format("Error for approximate histogram: %s \n", err1);
      System.out.format("Error for approximate histogram, ruleFold: %s \n", err2);
      System.out.format("Error ratio for AHRF: %s \n", err2 / err1);
    }
    return new float[]{err1, err2, err2 / err1};
  }

}
