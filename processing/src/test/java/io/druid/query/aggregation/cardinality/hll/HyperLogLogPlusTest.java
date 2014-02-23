package io.druid.query.aggregation.cardinality.hll;

/*
 * Copyright (C) 2011 Clearspring Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HyperLogLogPlusTest
{

//  @Test
  public void testTiming() {
    long startTime = System.currentTimeMillis();

    int[] numElements = {10, 100, 1000, 5000, 10000, 25000, 50000, 100000, 250000, 1000000, 10000000, 100000000};


//    HyperLogLogPlus hll = new HyperLogLogPlus(ByteBuffer.allocateDirect(1376).order(ByteOrder.nativeOrder()).putInt(0, 11));
    HyperLogLogPlus hll = new HyperLogLogPlus(11);

    int counter = 0;
    for (int i = 0; i < Integer.MAX_VALUE; ++i) {
      if (i % numElements[counter] == 0) {
        long card = hll.cardinality();
        System.out.printf(
            "%,15d => %,15d -- %,15.2f%% error in %,d millis%n",
            i,
            card,
            100 * (Math.abs(card - i) / (double) i),
            System.currentTimeMillis() - startTime
        );
        ++counter;
        if (counter >= numElements.length) {
          counter = numElements.length - 1;
        }
      }
      hll.offer(i);
    }
  }

  @Test
  public void testComputeCount() {
    HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14);
    int count = 70000;
    for (int i = 0; i < count; i++) {
      hyperLogLogPlus.offer("i" + i);
    }
    long estimate = hyperLogLogPlus.cardinality();
    double se = count * (1.04 / Math.sqrt(Math.pow(2, 14)));
    long expectedCardinality = count;

    assertTrue(estimate >= expectedCardinality - (3 * se));
    assertTrue(estimate <= expectedCardinality + (3 * se));
  }

  @Test
  public void testSmallCardinalityRepeatedInsert() {
    HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(14);
    int count = 15000;
    int maxAttempts = 200;
    Random r = new Random();
    for (int i = 0; i < count; i++) {
      int n = r.nextInt(maxAttempts) + 1;
      for (int j = 0; j < n; j++) {
        hyperLogLogPlus.offer("i" + i);
      }
    }
    long estimate = hyperLogLogPlus.cardinality();
    double se = count * (1.04 / Math.sqrt(Math.pow(2, 14)));
    long expectedCardinality = count;

    assertTrue(estimate >= expectedCardinality - (3 * se));
    assertTrue(estimate <= expectedCardinality + (3 * se));
  }

  @Test
  public void testSerialization_Normal() throws IOException {
    HyperLogLogPlus hll = new HyperLogLogPlus(5);
    for (int i = 0; i < 100000; i++) {
      hll.offer("" + i);
    }

    ByteBuffer buff = ByteBuffer.allocate(hll.sizeof());
    buff.put(hll.getBuffer());
    HyperLogLogPlus hll2 = new HyperLogLogPlus(hll.getBuffer());
    assertEquals(hll.cardinality(), hll2.cardinality());
  }

  @Test
  public void testHighCardinality() {
    HyperLogLogPlus hyperLogLogPlus = new HyperLogLogPlus(18);
    int size = 10000000;
    for (int i = 0; i < size; i++) {
      hyperLogLogPlus.offer(i);
    }
    long estimate = hyperLogLogPlus.cardinality();
    double err = Math.abs(estimate - size) / (double) size;
    assertTrue(err < .1);
  }

  @Test
  public void testMergeSelf() throws IOException {
    final int[] cardinalities = {0, 1, 10, 1000, 100000};
    for (int cardinality : cardinalities) {
      for (int j = 4; j < 24; j++) {
        HyperLogLogPlus hllPlus = new HyperLogLogPlus(j);
        for (int l = 0; l < cardinality; l++) {
          hllPlus.offer(Math.random());
        }

        HyperLogLogPlus deserialized = hllPlus.mutableCopy();
        assertEquals(hllPlus.cardinality(), deserialized.cardinality());
        HyperLogLogPlus merged = new HyperLogLogPlus(j);
        merged.addAll(hllPlus);
        merged.addAll(deserialized);
        assertEquals(hllPlus.cardinality(), merged.cardinality());
      }
    }
  }

  @Test
  public void testOne() throws IOException {
    HyperLogLogPlus one = new HyperLogLogPlus(8);
    one.offer("a");
    assertEquals(1, one.cardinality());
  }

  @Test
  public void testMerge_Normal() {
    int numToMerge = 4;
    int bits = 18;
    int cardinality = 5000;

    HyperLogLogPlus[] hyperLogLogs = new HyperLogLogPlus[numToMerge];
    HyperLogLogPlus baseline = new HyperLogLogPlus(bits);
    for (int i = 0; i < numToMerge; i++) {
      hyperLogLogs[i] = new HyperLogLogPlus(bits);
      for (int j = 0; j < cardinality; j++) {
        double val = Math.random();
        hyperLogLogs[i].offer(val);
        baseline.offer(val);
      }
    }

    long expectedCardinality = numToMerge * cardinality;
    HyperLogLogPlus hll = new HyperLogLogPlus(bits);
    for (int i = 0; i < hyperLogLogs.length; ++i) {
      hll.addAll(hyperLogLogs[i]);
    }
    long mergedEstimate = hll.cardinality();
    double se = expectedCardinality * (1.04 / Math.sqrt(Math.pow(2, bits)));

    assertTrue(mergedEstimate >= expectedCardinality - (3 * se));
    assertTrue(mergedEstimate <= expectedCardinality + (3 * se));
  }

  @Test
  public void testHll() {
    HyperLogLogPlus hll = new HyperLogLogPlus(11);
    hll.offer(1);
    hll.offer(2);
    HyperLogLogPlus anotherHll = hll.mutableCopy();
    Assert.assertEquals(hll.cardinality(), anotherHll.cardinality());

    anotherHll.offer(3);
    Assert.assertEquals(anotherHll.cardinality(), new HyperLogLogPlus(anotherHll.getBuffer()).cardinality());
  }
}