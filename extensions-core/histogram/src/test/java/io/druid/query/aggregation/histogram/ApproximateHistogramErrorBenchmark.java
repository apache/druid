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

package io.druid.query.aggregation.histogram;

import com.google.common.primitives.Floats;
import io.druid.query.aggregation.Histogram;

import java.util.Arrays;
import java.util.Locale;
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

    System.out.printf(
        Locale.ENGLISH,
        "Number of histograms for folding                           : %s %n",
        Arrays.toString(numHistsArray)
    );
    System.out.printf(
        Locale.ENGLISH,
        "Errors for approximate histogram                           : %s %n",
        Arrays.toString(errs1)
    );
    System.out.printf(
        Locale.ENGLISH,
        "Errors for approximate histogram, ruleFold                 : %s %n",
        Arrays.toString(errs2)
    );
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
      err1 += (float)Math.abs((hcounts[j] - ahcounts1[j]) / numValues);
      err2 += (float)Math.abs((hcounts[j] - ahcounts2[j]) / numValues);
    }

    if (debug) {
      float sum = 0;
      for (double v : hcounts) {
        sum += (float)v;
      }
      System.out.println("Exact Histogram Sum:");
      System.out.println(sum);
      sum = 0;
      for (double v : ahcounts1) {
        sum += (float)v;
      }
      System.out.println("Approximate Histogram Sum:");
      System.out.println(sum);
      sum = 0;
      for (double v : ahcounts2) {
        sum += (float)v;
      }
      System.out.println("Approximate Histogram Rule Fold Sum:");
      System.out.println(sum);
      System.out.println("Exact Histogram:");
      System.out.println(h.asVisual());
      System.out.println("Approximate Histogram:");
      System.out.println(ah1.toHistogram(breaks));
      System.out.println("Approximate Histogram Rule Fold:");
      System.out.println(ah2.toHistogram(breaks));
      System.out.printf(Locale.ENGLISH, "Error for approximate histogram: %f %n", err1);
      System.out.printf(Locale.ENGLISH, "Error for approximate histogram, ruleFold: %f %n", err2);
      System.out.printf(Locale.ENGLISH, "Error ratio for AHRF: %f %n", err2 / err1);
    }
    return new float[]{err1, err2, err2 / err1};
  }

}
